package controller

import (
	"fmt"
	"time"

	"github.com/longhorn/backupstore"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	BackupStatusQueryInterval = 2 * time.Second
)

type BackupController struct {
	*baseController

	// use as the OwnerID of the controller
	namespace    string
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	bvStoreSynced cache.InformerSynced
	bStoreSynced  cache.InformerSynced
}

func NewBackupController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	backupVolumeInformer lhinformers.BackupVolumeInformer,
	backupInformer lhinformers.BackupInformer,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) *BackupController {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	bc := &BackupController{
		baseController: newBaseController("longhorn-backup", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-backup-controller"}),

		bvStoreSynced: backupVolumeInformer.Informer().HasSynced,
		bStoreSynced:  backupInformer.Informer().HasSynced,
	}

	backupInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    bc.enqueueBackup,
		UpdateFunc: func(old, cur interface{}) { bc.enqueueBackup(cur) },
		DeleteFunc: bc.enqueueBackup,
	})

	return bc
}

func (bc *BackupController) enqueueBackup(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	bc.queue.AddRateLimited(key)
}

func (bc *BackupController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer bc.queue.ShutDown()

	bc.logger.Infof("Start Longhorn Backup Snapshot controller")
	defer bc.logger.Infof("Shutting down Longhorn Backup Snapshot controller")

	if !cache.WaitForNamedCacheSync(bc.name, stopCh, bc.bvStoreSynced, bc.bStoreSynced) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(bc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (bc *BackupController) worker() {
	for bc.processNextWorkItem() {
	}
}

func (bc *BackupController) processNextWorkItem() bool {
	key, quit := bc.queue.Get()
	if quit {
		return false
	}
	defer bc.queue.Done(key)
	err := bc.syncHandler(key.(string))
	bc.handleErr(err, key)
	return true
}

func (bc *BackupController) handleErr(err error, key interface{}) {
	if err == nil {
		bc.queue.Forget(key)
		return
	}

	if bc.queue.NumRequeues(key) < maxRetries {
		bc.logger.WithError(err).Warnf("Error syncing Longhorn backup %v", key)
		bc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	bc.logger.WithError(err).Warnf("Dropping Longhorn backup %v out of the queue", key)
	bc.queue.Forget(key)
}

func (bc *BackupController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: fail to sync backup %v", bc.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != bc.namespace {
		return nil
	}
	return bc.reconcile(bc.logger, name)
}

func (bc *BackupController) backupCreation(log logrus.FieldLogger, backup *longhorn.Backup, pollInterval metav1.Duration) error {
	volumeName, err := bc.getBackupVolume(log, backup)
	if err != nil {
		return err
	}

	backupTarget, err := bc.ds.GetSettingValueExisted(types.SettingNameBackupTarget)
	if err != nil {
		return err
	}

	credential, err := manager.GetBackupCredentialConfig(bc.ds)
	if err != nil {
		return err
	}

	volumeManager := manager.NewVolumeManager(bc.controllerID, bc.ds)
	engine, err := volumeManager.GetEngineClient(volumeName)
	if err != nil {
		return err
	}

	// blocks till the backup snapshot creation has been started
	_, err = engine.SnapshotBackup(
		backup.Name, backup.Spec.SnapshotName, backupTarget,
		backup.Spec.BackingImage, backup.Spec.BackingImageURL,
		backup.Spec.Labels, credential)
	if err != nil {
		log.WithError(err).Errorf("Failed to initiate backup snapshot %v of volume %v with label %v",
			backup.Spec.SnapshotName, volumeName, backup.Spec.Labels)
		return err
	}

	bc.eventRecorder.Eventf(backup, v1.EventTypeNormal, EventReasonStartedBackup,
		"Initiated backup snapshot %v of volume %v with label %v",
		backup.Spec.SnapshotName, volumeName, backup.Spec.Labels)
	log.Debugf("Initiated backup snapshot %v of volume %v with label %v", backup.Spec.SnapshotName, volumeName, backup.Spec.Labels)

	go func() {
		bks := &types.BackupStatus{}
		for {
			engines, err := bc.ds.ListVolumeEngines(volumeName)
			if err != nil {
				logrus.Errorf("fail to get engines for volume %v", volumeName)
				return
			}

			for _, e := range engines {
				backupStatusList := e.Status.BackupStatus
				for _, b := range backupStatusList {
					if b.SnapshotName == backup.Spec.SnapshotName {
						bks = b
						break
					}
				}
			}
			if bks.Error != "" {
				// Record backup snapshot failed event
				bc.eventRecorder.Eventf(backup, v1.EventTypeWarning, EventReasonFailedBackup, "%v", bks.Error)
				logrus.Errorf("Failed to updated volume LastBackup for %v due to backup error %v", volumeName, bks.Error)
				break
			}
			if bks.Progress == 100 {
				// Record backup snapshot succeeded event
				bc.eventRecorder.Eventf(backup, v1.EventTypeNormal, EventReasonSucceededBackup,
					"Backup snapshot %v of volume %v with label %v",
					backup.Spec.SnapshotName, volumeName, backup.Spec.Labels)

				// Request backup_volume_controller to reconcile BackupVolume immediately.
				backupVolume, err := bc.ds.GetBackupVolume(volumeName)
				if err == nil {
					backupVolume.Spec.SyncRequestAt = &metav1.Time{Time: time.Now().UTC()}
					if _, err = bc.ds.UpdateBackupVolume(backupVolume); err != nil && !datastore.ErrorIsConflict(err) {
						log.WithError(err).Errorf("Error updating backup volume %s status", volumeName)
					}
				} else if err != nil && datastore.ErrorIsNotFound(err) {
					backupVolume := &longhorn.BackupVolume{
						ObjectMeta: metav1.ObjectMeta{
							Name:       volumeName,
							Finalizers: []string{longhornFinalizerKey},
						},
					}
					if _, err = bc.ds.CreateBackupVolume(backupVolume); err != nil {
						log.WithError(err).Errorf("Error creating backup volume %s into cluster", volumeName)
					}
				}

				break
			}
			time.Sleep(BackupStatusQueryInterval)
		}
	}()
	return nil
}

func (bc *BackupController) getBackupVolume(log logrus.FieldLogger, backup *longhorn.Backup) (string, error) {
	backupVolumeName, ok := backup.Labels[types.LonghornLabelVolume]
	if !ok {
		log.Warn("Cannot find the volume label")
		return "", fmt.Errorf("Cannot find the volume label")
	}
	return backupVolumeName, nil
}

func (bc *BackupController) getBackupMetadataURL(log logrus.FieldLogger, backup *longhorn.Backup, backupTargetURL string) (string, error) {
	backupVolumeName, err := bc.getBackupVolume(log, backup)
	if err != nil {
		return "", err
	}
	return backupstore.EncodeMetadataURL(backup.Name, backupVolumeName, backupTargetURL), nil
}

func (bc *BackupController) reconcile(log logrus.FieldLogger, backupName string) (err error) {
	backupTarget, err := bc.ds.GetBackupTarget(defaultBackupTargetName)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return err
		}
		log.Warnf("Backup target %s be deleted", defaultBackupTargetName)
		return nil
	}

	// Check the responsible node
	if backupTarget.Spec.ResponsibleNodeID != bc.controllerID {
		return nil
	}

	log = log.WithFields(logrus.Fields{
		"backup": backupName,
	})

	// Get Backup CR
	backup, err := bc.ds.GetBackup(backupName)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return err
		}
		return nil
	}

	// Examine DeletionTimestamp to determine if object is under deletion
	if backup.DeletionTimestamp != nil {
		// Delete the backup from the remote backup target
		if backup.Spec.DeleteRemoteConfig {
			backupURL, err := bc.getBackupMetadataURL(log, backup, backupTarget.Spec.BackupTargetURL)
			if err != nil {
				log.WithError(err).Error("Get backup URL")
				// Ignore error to prevent enqueue
				return nil
			}

			backupTargetClient, err := manager.GenerateBackupTarget(bc.ds)
			if err != nil {
				log.WithError(err).Error("Error generate backup target client")
				// Ignore error to prevent enqueue
				return nil
			}

			if err := backupTargetClient.DeleteBackup(backupURL); err != nil {
				log.WithError(err).Error("Error deleting remote backup")
				return err
			}
		}

		// Request backup_volume_controller to reconcile BackupVolume immediately.
		volumeName, err := bc.getBackupVolume(log, backup)
		if err != nil {
			return err
		}
		backupVolume, err := bc.ds.GetBackupVolume(volumeName)
		if err == nil {
			backupVolume.Spec.SyncRequestAt = &metav1.Time{Time: time.Now().UTC()}
			if _, err = bc.ds.UpdateBackupVolume(backupVolume); err != nil && !datastore.ErrorIsConflict(err) {
				log.WithError(err).Errorf("Error updating backup volume %s status", volumeName)
			}
		}
		return bc.ds.RemoveFinalizerForBackup(backup)
	}

	// Perform snapshot backup
	if backup.Spec.SnapshotName != "" && !backup.Status.BackupCreationIsStart {
		err = bc.backupCreation(log, backup, backupTarget.Spec.PollInterval)
		if err != nil {
			// Record backup snapshot failed event
			bc.eventRecorder.Eventf(backup, v1.EventTypeWarning, EventReasonFailedBackup, "%v", err)
			log.WithError(err).Error("Backup creation")
			return err
		}

		// Update `status.backupCreationIsStart` to prevent reconciles to snapshot backup again
		backup.Status.BackupCreationIsStart = true
		if _, err = bc.ds.UpdateBackupStatus(backup); err != nil && !datastore.ErrorIsConflict(err) {
			log.WithError(err).Error("Error updating backup status")
			return err
		}
		return nil
	}

	// The backup config had synced
	if !backup.Status.LastSyncedAt.IsZero() {
		return nil
	}

	backupURL, err := bc.getBackupMetadataURL(log, backup, backupTarget.Spec.BackupTargetURL)
	if err != nil {
		log.WithError(err).Error("Get backup URL")
		// Ignore error to prevent enqueue
		return nil
	}

	backupTargetClient, err := manager.GenerateBackupTarget(bc.ds)
	if err != nil {
		log.WithError(err).Error("Error generate backup target client")
		// Ignore error to prevent enqueue
		return nil
	}

	backupInfo, err := backupTargetClient.InspectBackupConfig(backupURL)
	if err != nil || backupInfo == nil {
		log.WithError(err).Error("Cannot inspect the backup config")
		// Ignore error to prevent enqueue
		return nil
	}

	// Updates Backup CR status first
	backup.Status.URL = backupInfo.URL
	backup.Status.SnapshotName = backupInfo.SnapshotName
	backup.Status.SnapshotCreateAt = backupInfo.SnapshotCreated
	backup.Status.BackupCreateAt = backupInfo.Created
	backup.Status.Size = backupInfo.Size
	backup.Status.Labels = backupInfo.Labels
	backup.Status.Messages = backupInfo.Messages
	backup.Status.VolumeName = backupInfo.VolumeName
	backup.Status.VolumeSize = backupInfo.VolumeSize
	backup.Status.VolumeCreated = backupInfo.VolumeCreated
	backup.Status.VolumeBackingImageName = backupInfo.VolumeBackingImageName
	backup.Status.VolumeBackingImageURL = backupInfo.VolumeBackingImageURL
	backup.Status.LastSyncedAt = &metav1.Time{Time: time.Now().UTC()}
	if _, err = bc.ds.UpdateBackupStatus(backup); err != nil && !datastore.ErrorIsConflict(err) {
		log.WithError(err).Error("Error updating backup status")
		return err
	}
	return nil
}

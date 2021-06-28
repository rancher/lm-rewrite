package controller

import (
	"fmt"
	"strings"
	"time"

	"github.com/longhorn/backupstore"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
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

type BackupVolumeController struct {
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

func NewBackupVolumeController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	backupVolumeInformer lhinformers.BackupVolumeInformer,
	backupInformer lhinformers.BackupInformer,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) *BackupVolumeController {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	bvc := &BackupVolumeController{
		baseController: newBaseController("longhorn-backup-volume", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-backup-volume-controller"}),

		bvStoreSynced: backupVolumeInformer.Informer().HasSynced,
		bStoreSynced:  backupInformer.Informer().HasSynced,
	}

	backupVolumeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    bvc.enqueueBackupVolume,
		UpdateFunc: func(old, cur interface{}) { bvc.enqueueBackupVolume(cur) },
		DeleteFunc: bvc.enqueueBackupVolume,
	})

	return bvc
}

func (bvc *BackupVolumeController) enqueueBackupVolume(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	bvc.queue.AddRateLimited(key)
}

func (bvc *BackupVolumeController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer bvc.queue.ShutDown()

	bvc.logger.Infof("Start Longhorn Backup Volume controller")
	defer bvc.logger.Infof("Shutting down Longhorn Backup Volume controller")

	if !cache.WaitForNamedCacheSync(bvc.name, stopCh, bvc.bvStoreSynced, bvc.bStoreSynced) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(bvc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (bvc *BackupVolumeController) worker() {
	for bvc.processNextWorkItem() {
	}
}

func (bvc *BackupVolumeController) processNextWorkItem() bool {
	key, quit := bvc.queue.Get()
	if quit {
		return false
	}
	defer bvc.queue.Done(key)
	err := bvc.syncHandler(key.(string))
	bvc.handleErr(err, key)
	return true
}

func (bvc *BackupVolumeController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: fail to sync backup volume %v", bvc.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != bvc.namespace {
		// Not ours, skip it
		return nil
	}
	return bvc.reconcile(bvc.logger, name)
}

func (bvc *BackupVolumeController) handleErr(err error, key interface{}) {
	if err == nil {
		bvc.queue.Forget(key)
		return
	}

	if bvc.queue.NumRequeues(key) < maxRetries {
		bvc.logger.WithError(err).Warnf("Error syncing Longhorn backup volume %v", key)
		bvc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	bvc.logger.WithError(err).Warnf("Dropping Longhorn backup volume %v out of the queue", key)
	bvc.queue.Forget(key)
}

func (bvc *BackupVolumeController) DeleteBackups(volumeName string, addFinalizer bool) error {
	backups, err := bvc.ds.ListBackup(volumeName)
	if err != nil {
		return err
	}

	var errs []string
	for backupName := range backups {
		if addFinalizer {
			err := bvc.ds.AddFinalizerForBackup(backupName)
			if err != nil {
				errs = append(errs, err.Error())
				continue
			}
		}

		err = bvc.ds.DeleteBackup(backupName)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ","))
	}
	return nil
}

func (bvc *BackupVolumeController) reconcile(log logrus.FieldLogger, backupVolumeName string) (err error) {
	backupTarget, err := bvc.ds.GetBackupTarget(defaultBackupTargetName)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return err
		}
		log.Warnf("Backup target %s be deleted", defaultBackupTargetName)
		return nil
	}

	if isResponsible, err := shouldProcess(bvc.ds, bvc.controllerID, backupTarget.Spec.PollInterval); err != nil || !isResponsible {
		if err != nil {
			log.WithError(err).Warn("Failed to select node, will try again next poll interval")
		}
		return nil
	}

	log = log.WithFields(logrus.Fields{
		"backupVolume": backupVolumeName,
	})

	// Reconcile delete BackupVolume CR
	backupVolume, err := bvc.ds.GetBackupVolume(backupVolumeName)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return err
		}
		// Delete related Backup CRs with the given volume name
		return bvc.DeleteBackups(backupVolumeName, false)
	}

	// Examine DeletionTimestamp to determine if object is under deletion
	if backupVolume.DeletionTimestamp != nil {
		// Delete related Backup CRs with the given volume name
		if err := bvc.DeleteBackups(backupVolumeName, true); err != nil {
			log.WithError(err).Error("Error deleting backups")
			return err
		}

		backupTargetClient, err := manager.GenerateBackupTarget(bvc.ds)
		if err != nil {
			log.WithError(err).Error("Error generate backup target client")
			// Ignore error to prevent enqueue
			return nil
		}

		// Delete the backup volume from the remote backup target
		if err := backupTargetClient.DeleteBackupVolume(backupVolumeName); err != nil {
			log.WithError(err).Error("Error deleting remote backup volume")
			return err
		}
		return bvc.ds.RemoveFinalizerForBackupVolume(backupVolume)
	}

	// Check to force sync backup volume or not
	if !backupVolume.Spec.ForceSync {
		// The user disables poll interval
		if backupTarget.Spec.PollInterval.Duration == 0 {
			return nil
		}
		if !shouldSync(backupVolume.Status.LastSyncedAt, backupTarget.Spec.PollInterval) {
			return nil
		}
	}

	// Get a list of all the backups that are stored in the backup target
	log.Debug("Pulling backups from backup target")

	backupTargetClient, err := manager.GenerateBackupTarget(bvc.ds)
	if err != nil {
		log.WithError(err).Error("Error generate backup target client")
		// Ignore error to prevent enqueue
		return nil
	}

	res, err := backupTargetClient.ListBackupNames(backupVolumeName)
	if err != nil {
		log.WithError(err).Error("Error listing backups from backup target")
		// Ignore error to prevent enqueue
		return nil
	}

	backupStoreBackups := sets.NewString(res...)
	log.WithField("BackupCount", len(backupStoreBackups)).Debug("Got backups from backup target")

	// Get a list of all the backups that exist as custom resources in the cluster
	clusterBackups, err := bvc.ds.ListBackup(backupVolumeName)
	if err != nil {
		log.WithError(err).Error("Error listing backups in the cluster, proceeding with pull into cluster")
	} else {
		log.WithField("clusterBackupCount", len(clusterBackups)).Debug("Got backups in the cluster")
	}

	clustersSet := sets.NewString()
	for _, b := range clusterBackups {
		clustersSet.Insert(b.Name)
	}

	// Get a list of backups that *are* in the backup target and *aren't* in the cluster
	// and create the Backup CR in the cluster
	backupsToPull := backupStoreBackups.Difference(clustersSet)
	if count := backupsToPull.Len(); count > 0 {
		log.Infof("Found %v backups in the backup target that do not exist in the cluster and need to be pulled", count)
	}
	for backupName := range backupsToPull {
		backup := &longhorn.Backup{
			ObjectMeta: metav1.ObjectMeta{
				Name:   backupName,
				Labels: types.GetVolumeLabels(backupVolumeName),
			},
			Spec: types.BackupSnapshotSpec{
				// Request backup_controller to reconcile Backup immediately.
				ForceSync: true,
			},
		}
		if _, err = bvc.ds.CreateBackup(backup); err != nil {
			log.WithError(err).Errorf("Error creating backup %s into cluster", backupName)
		}
	}

	// Get a list of backups that *are* in the cluster and *aren't* in the backup target
	// and delete the Backup CR in the cluster
	backupsToDelete := clustersSet.Difference(backupStoreBackups)
	if count := backupsToDelete.Len(); count > 0 {
		log.Infof("Found %v backups in the backup target that do not exist in the backup target and need to be deleted", count)
	}
	for backupName := range backupsToDelete {
		if err = bvc.ds.DeleteBackup(backupName); err != nil {
			log.WithError(err).Errorf("Error deleting backup %s into cluster", backupName)
		}
	}

	backupVolumeMetadataURL := backupstore.EncodeMetadataURL("", backupVolumeName, backupTarget.Spec.BackupTargetURL)
	configMetadata, err := backupTargetClient.GetConfigMetadata(backupVolumeMetadataURL)
	if err != nil {
		log.WithError(err).Error("Error getting backup volume config metadata from backup target")
		// Ignore error to prevent enqueue
		return nil
	}

	// Check the config metadata got changed
	if backupVolume.Spec.ConfigModificationTime == configMetadata.ModificationTime {
		return nil
	}

	backupVolumeInfo, err := backupTargetClient.InspectBackupVolumeConfig(backupVolumeMetadataURL)
	if err != nil || backupVolumeInfo == nil {
		log.WithError(err).Error("Error getting backup volume config from backup target")

		// Cannot inspect the config, clean up the status
		backupVolume.Status = types.BackupVolumeStatus{}
		if _, err = bvc.ds.UpdateBackupVolumeStatus(backupVolume); err != nil && !datastore.ErrorIsConflict(err) {
			log.WithError(err).Error("Error updating backup volume status")
			return err
		}

		// Ignore error to prevent enqueue
		return nil
	}

	// Updates BackupVolume CR status first
	backupVolume.Status.Size = backupVolumeInfo.Size
	backupVolume.Status.Labels = backupVolumeInfo.Labels
	backupVolume.Status.CreateAt = backupVolumeInfo.Created
	backupVolume.Status.LastBackupName = backupVolumeInfo.LastBackupName
	backupVolume.Status.LastBackupAt = backupVolumeInfo.LastBackupAt
	backupVolume.Status.DataStored = backupVolumeInfo.DataStored
	backupVolume.Status.Messages = backupVolumeInfo.Messages
	backupVolume.Status.BackingImageName = backupVolumeInfo.BackingImageName
	backupVolume.Status.BackingImageURL = backupVolumeInfo.BackingImageURL
	backupVolume.Status.LastSyncedAt = &metav1.Time{Time: time.Now().UTC()}
	backupVolume, err = bvc.ds.UpdateBackupVolumeStatus(backupVolume)
	if err != nil && !datastore.ErrorIsConflict(err) {
		log.WithError(err).Error("Error updating backup volume status")
		return err
	}

	// Then updates BackupVolume CR spec
	backupVolume.Spec.ConfigModificationTime = configMetadata.ModificationTime
	backupVolume.Spec.ForceSync = false
	backupVolume, err = bvc.ds.UpdateBackupVolume(backupVolume)
	if err != nil && !datastore.ErrorIsConflict(err) {
		log.WithError(err).Error("Error updating backup volume spec")
		return err
	}

	// Updates Volume CR last backup name and last backup time
	if err := manager.SyncVolumeLastBackupWithBackupVolume(backupVolumeName, backupVolumeInfo, bvc.ds.GetVolume, bvc.ds.UpdateVolumeStatus); err != nil {
		log.WithError(err).Errorf("Failed to update volume LastBackup for %v: %v", backupVolumeName, err)
		return err
	}
	return nil
}

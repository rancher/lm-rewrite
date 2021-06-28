package controller

import (
	"fmt"
	"sort"
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

const (
	defaultBackupTargetName = "default"
)

type BackupTargetController struct {
	*baseController

	// use as the OwnerID of the controller
	namespace    string
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	btStoreSynced cache.InformerSynced
	bvStoreSynced cache.InformerSynced
}

func NewBackupTargetController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	backupTargetInformer lhinformers.BackupTargetInformer,
	backupVolumeInformer lhinformers.BackupVolumeInformer,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) *BackupTargetController {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	btc := &BackupTargetController{
		baseController: newBaseController("longhorn-backup-target", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-backup-target-controller"}),

		btStoreSynced: backupTargetInformer.Informer().HasSynced,
		bvStoreSynced: backupVolumeInformer.Informer().HasSynced,
	}

	backupTargetInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    btc.enqueueBackupTarget,
		UpdateFunc: func(old, cur interface{}) { btc.enqueueBackupTarget(cur) },
	})

	return btc
}

func (btc *BackupTargetController) enqueueBackupTarget(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	btc.queue.AddRateLimited(key)
}

func (btc *BackupTargetController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer btc.queue.ShutDown()

	btc.logger.Infof("Start Longhorn Backup Target controller")
	defer btc.logger.Infof("Shutting down Longhorn Backup Target controller")

	if !cache.WaitForNamedCacheSync(btc.name, stopCh, btc.btStoreSynced, btc.bvStoreSynced) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(btc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (btc *BackupTargetController) worker() {
	for btc.processNextWorkItem() {
	}
}

func (btc *BackupTargetController) processNextWorkItem() bool {
	key, quit := btc.queue.Get()
	if quit {
		return false
	}
	defer btc.queue.Done(key)
	err := btc.syncHandler(key.(string))
	btc.handleErr(err, key)
	return true
}

func (btc *BackupTargetController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: fail to sync %v", btc.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != btc.namespace {
		// Not ours, skip it
		return nil
	}
	if name != defaultBackupTargetName {
		return nil
	}
	return btc.reconcile(btc.logger)
}

func (btc *BackupTargetController) handleErr(err error, key interface{}) {
	if err == nil {
		btc.queue.Forget(key)
		return
	}

	if btc.queue.NumRequeues(key) < maxRetries {
		btc.logger.WithError(err).Warnf("Error syncing Longhorn backup target %v", key)
		btc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	btc.logger.WithError(err).Warnf("Dropping Longhorn backup target %v out of the queue", key)
	btc.queue.Forget(key)
}

func (btc *BackupTargetController) reconcile(log logrus.FieldLogger) (err error) {
	backupTarget, err := btc.ds.GetBackupTarget(defaultBackupTargetName)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return err
		}
		log.Warnf("Backup target %s be deleted", defaultBackupTargetName)
		return nil
	}

	if isResponsible, err := shouldProcess(btc.ds, btc.controllerID, backupTarget.Spec.PollInterval); err != nil || !isResponsible {
		if err != nil {
			log.WithError(err).Warn("Failed to select node, will try again next poll interval")
		}
		return nil
	}

	log = log.WithFields(logrus.Fields{
		"url":      backupTarget.Spec.BackupTargetURL,
		"cred":     backupTarget.Spec.CredentialSecret,
		"interval": backupTarget.Spec.PollInterval.Duration,
	})
	backupTargetAvailable := backupTarget.Status.Available
	backupTargetLastSyncedAt := backupTarget.Status.LastSyncedAt

	defer func(backupTargetAvailable *bool, backupTargetLastSyncedAt **metav1.Time) {
		if backupTargetAvailable == nil || backupTargetLastSyncedAt == nil {
			log.Info("BUG: backupTargetAvailable or backupTargetLastSyncedAt is nil")
			return
		}

		if backupTarget.Status.Available && !(*backupTargetAvailable) {
			// backup target is not available, clean up all BackupVolume CRs
			log.Info("Backup target status become unavailable, clean up all the BackupVolume CRs")

			clusterBackupVolumes, err := btc.ds.ListBackupVolume()
			if err != nil {
				log.WithError(err).Error("Error listing backup volumes in the cluster, proceeding with pull into cluster")
			}
			for backupVolumeName := range clusterBackupVolumes {
				if err = btc.ds.DeleteBackupVolume(backupVolumeName); err != nil {
					log.WithError(err).Errorf("Error deleting backup volume %s into cluster", backupVolumeName)
				}
			}
		}

		if backupTarget.Spec.ForceSync {
			backupTarget.Spec.ForceSync = false
			backupTarget, err = btc.ds.UpdateBackupTarget(backupTarget)
			if err != nil && !datastore.ErrorIsConflict(err) {
				log.WithError(err).Error("Error updating backup target spec")
			}
		}

		backupTarget.Status.Available = *backupTargetAvailable
		backupTarget.Status.LastSyncedAt = *backupTargetLastSyncedAt
		_, err = btc.ds.UpdateBackupTargetStatus(backupTarget)
		if err != nil && !datastore.ErrorIsConflict(err) {
			log.WithError(err).Error("Error updating backup target status")
		}
	}(&backupTargetAvailable, &backupTargetLastSyncedAt)

	// Check to force sync backup target or not
	if !backupTarget.Spec.ForceSync {
		// The user disables poll interval
		if backupTarget.Spec.PollInterval.Duration == 0 {
			backupTargetAvailable = false
			return nil
		}
		if !shouldSync(backupTargetLastSyncedAt, backupTarget.Spec.PollInterval) {
			return nil
		}
	}

	backupTargetClient, err := manager.GenerateBackupTarget(btc.ds)
	if err != nil {
		log.WithError(err).Error("Error generate backup target client")
		backupTargetAvailable = false
		backupTargetLastSyncedAt = &metav1.Time{Time: time.Now().UTC()}
		// Ignore error to prevent enqueue
		return nil
	}

	// Get a list of all the backup volumes that are stored in the backup target
	log.Debug("Pulling backup volumes from backup target")
	res, err := backupTargetClient.ListBackupVolumeNames()
	if err != nil {
		log.WithError(err).Error("Error listing backup volumes from backup target")
		backupTargetAvailable = false
		backupTargetLastSyncedAt = &metav1.Time{Time: time.Now().UTC()}
		// Ignore error to prevent enqueue
		return nil
	}

	backupStoreBackupVolumes := sets.NewString(res...)
	log.WithField("BackupVolumeCount", len(backupStoreBackupVolumes)).Debug("Got backup volumes from backup target")

	// Get a list of all the backup volumes that exist as custom resources in the cluster
	clusterBackupVolumes, err := btc.ds.ListBackupVolume()
	if err != nil {
		log.WithError(err).Error("Error listing backup volumes in the cluster, proceeding with pull into cluster")
	} else {
		log.WithField("clusterBackupVolumeCount", len(clusterBackupVolumes)).Debug("Got backup volumes in the cluster")
	}

	clusterBackupVolumesSet := sets.NewString()
	for _, b := range clusterBackupVolumes {
		clusterBackupVolumesSet.Insert(b.Name)
	}

	// Get a list of backup volumes that *are* in the backup target and *aren't* in the cluster
	// and create the BackupVolume CR in the cluster
	backupVolumesToPull := backupStoreBackupVolumes.Difference(clusterBackupVolumesSet)
	if count := backupVolumesToPull.Len(); count > 0 {
		log.Infof("Found %v backup volumes in the backup target that do not exist in the cluster and need to be pulled", count)
	}
	for backupVolumeName := range backupVolumesToPull {
		log.WithField("backupVolume", backupVolumeName).Info("Attempting to pull backup volume into cluster")

		backupVolumeMetadataURL := backupstore.EncodeMetadataURL("", backupVolumeName, backupTarget.Spec.BackupTargetURL)
		backupVolumeInfo, err := backupTargetClient.InspectBackupVolumeConfig(backupVolumeMetadataURL)
		if err != nil || backupVolumeInfo == nil {
			log.WithError(err).Error("Error getting backup volume metadata from backup target")
			continue
		}

		backupVolume := &longhorn.BackupVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: backupVolumeName,
			},
			Spec: types.BackupVolumeSpec{
				// Request backup_volume_controller to reconcile BackupVolume immediately.
				ForceSync: true,
			},
		}
		if _, err = btc.ds.CreateBackupVolume(backupVolume); err != nil {
			log.WithError(err).Errorf("Error creating backup volume %s into cluster", backupVolumeName)
		}
	}

	// Get a list of backup volumes that *are* in the cluster and *aren't* in the backup target
	// and delete the BackupVolume CR in the cluster
	backupVolumesToDelete := clusterBackupVolumesSet.Difference(backupStoreBackupVolumes)
	if count := backupVolumesToDelete.Len(); count > 0 {
		log.Infof("Found %v backup volumes in the backup target that do not exist in the backup target and need to be deleted", count)
	}
	for backupVolumeName := range backupVolumesToDelete {
		log.WithField("backupVolume", backupVolumeName).Info("Attempting to delete backup volume from cluster")
		if err = btc.ds.DeleteBackupVolume(backupVolumeName); err != nil {
			log.WithError(err).Errorf("Error deleting backup volume %s into cluster", backupVolumeName)
		}
	}

	// Update the backup target status
	backupTargetAvailable = true
	backupTargetLastSyncedAt = &metav1.Time{Time: time.Now().UTC()}
	return nil
}

// shouldProcess choose which node need to run the update
// since this is run on each node, but we only need a single update
// we pick a consistent random ready node for each poll run
func shouldProcess(ds *datastore.DataStore, controllerID string, duration metav1.Duration) (bool, error) {
	defaultEngineImage, err := ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if err != nil {
		return false, err
	}

	nodes, err := ds.ListReadyNodesWithEngineImage(defaultEngineImage)
	if err != nil {
		return false, err
	}

	// for the random ready evaluation
	// we sort the candidate list (this will normalize the list across nodes)
	var candidates []string
	for node := range nodes {
		candidates = append(candidates, node)
	}

	if len(candidates) == 0 {
		return false, fmt.Errorf("no ready nodes with engine image %v available", defaultEngineImage)
	}
	sort.Strings(candidates)

	// we use a time index to derive an index into the candidate list (normalizes time differences across nodes)
	// we arbitrarily choose the pollInterval as your time normalization factor, since this also has the benefit of
	// doing round robin across the at the time available candidate nodes.
	interval := int64(duration.Seconds()) + 1
	midPoint := (interval / 2) + 1
	timeIndex := int((time.Now().UTC().Unix() + midPoint) / interval)
	candidateIndex := timeIndex % len(candidates)
	responsibleNode := candidates[candidateIndex]
	return controllerID == responsibleNode, nil
}

func shouldSync(lastSyncedAt *metav1.Time, pollInterval metav1.Duration) bool {
	if lastSyncedAt != nil {
		nextSync := lastSyncedAt.Add(pollInterval.Duration)
		if time.Now().UTC().Before(nextSync) {
			return false
		}
	}
	return true
}

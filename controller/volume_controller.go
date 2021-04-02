package controller

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	batchv1 "k8s.io/api/batch/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/backupstore"
	imutil "github.com/longhorn/longhorn-instance-manager/pkg/util"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/scheduler"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
)

var (
	ownerKindVolume = longhorn.SchemeGroupVersion.WithKind("Volume").String()

	RetryInterval = 100 * time.Millisecond
	RetryCounts   = 20

	AutoSalvageTimeLimit = 1 * time.Minute
)

const (
	CronJobBackoffLimit               = 3
	CronJobSuccessfulJobsHistoryLimit = 1
	VolumeSnapshotsWarningThreshold   = 100

	LastAppliedCronJobSpecAnnotationKeySuffix = "last-applied-cronjob-spec"
)

type VolumeController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID   string
	ManagerImage   string
	ServiceAccount string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	vStoreSynced  cache.InformerSynced
	eStoreSynced  cache.InformerSynced
	rStoreSynced  cache.InformerSynced
	smStoreSynced cache.InformerSynced

	scheduler *scheduler.ReplicaScheduler

	backoff *flowcontrol.Backoff

	// for unit test
	nowHandler func() string
}

func NewVolumeController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	volumeInformer lhinformers.VolumeInformer,
	engineInformer lhinformers.EngineInformer,
	replicaInformer lhinformers.ReplicaInformer,
	shareManagerInformer lhinformers.ShareManagerInformer,
	kubeClient clientset.Interface,
	namespace, controllerID, serviceAccount string,
	managerImage string) *VolumeController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	vc := &VolumeController{
		baseController: newBaseController("longhorn-volume", logger),

		ds:             ds,
		namespace:      namespace,
		controllerID:   controllerID,
		ManagerImage:   managerImage,
		ServiceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-volume-controller"}),

		vStoreSynced:  volumeInformer.Informer().HasSynced,
		eStoreSynced:  engineInformer.Informer().HasSynced,
		rStoreSynced:  replicaInformer.Informer().HasSynced,
		smStoreSynced: shareManagerInformer.Informer().HasSynced,

		backoff: flowcontrol.NewBackOff(time.Minute, time.Minute*3),

		nowHandler: util.Now,
	}

	vc.scheduler = scheduler.NewReplicaScheduler(ds)

	volumeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    vc.enqueueVolume,
		UpdateFunc: func(old, cur interface{}) { vc.enqueueVolume(cur) },
		DeleteFunc: vc.enqueueVolume,
	})
	engineInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    vc.enqueueControlleeChange,
		UpdateFunc: func(old, cur interface{}) { vc.enqueueControlleeChange(cur) },
		DeleteFunc: vc.enqueueControlleeChange,
	})
	replicaInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    vc.enqueueControlleeChange,
		UpdateFunc: func(old, cur interface{}) { vc.enqueueControlleeChange(cur) },
		DeleteFunc: vc.enqueueControlleeChange,
	})
	shareManagerInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    vc.enqueueVolumesForShareManager,
		UpdateFunc: func(old, cur interface{}) { vc.enqueueVolumesForShareManager(cur) },
		DeleteFunc: vc.enqueueVolumesForShareManager,
	})
	return vc
}

func (vc *VolumeController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer vc.queue.ShutDown()

	vc.logger.Infof("Start Longhorn volume controller")
	defer vc.logger.Infof("Shutting down Longhorn volume controller")

	if !cache.WaitForNamedCacheSync("longhorn engines", stopCh, vc.vStoreSynced, vc.eStoreSynced, vc.rStoreSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(vc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (vc *VolumeController) worker() {
	for vc.processNextWorkItem() {
	}
}

func (vc *VolumeController) processNextWorkItem() bool {
	key, quit := vc.queue.Get()

	if quit {
		return false
	}
	defer vc.queue.Done(key)

	err := vc.syncVolume(key.(string))
	vc.handleErr(err, key)

	return true
}

func (vc *VolumeController) handleErr(err error, key interface{}) {
	if err == nil {
		vc.queue.Forget(key)
		return
	}

	if vc.queue.NumRequeues(key) < maxRetries {
		vc.logger.WithError(err).Warnf("Error syncing Longhorn volume %v", key)
		vc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	vc.logger.WithError(err).Warnf("Dropping Longhorn volume %v out of the queue", key)
	vc.queue.Forget(key)
}

func getLoggerForVolume(logger logrus.FieldLogger, v *longhorn.Volume) *logrus.Entry {
	log := logger.WithFields(
		logrus.Fields{
			"volume":     v.Name,
			"frontend":   v.Spec.Frontend,
			"state":      v.Status.State,
			"owner":      v.Status.OwnerID,
			"accessMode": v.Spec.AccessMode,
			"migratable": v.Spec.Migratable,
		},
	)

	if v.Spec.AccessMode == types.AccessModeReadWriteMany {
		log = log.WithFields(
			logrus.Fields{
				"shareState":    v.Status.ShareState,
				"shareEndpoint": v.Status.ShareEndpoint,
			})
	}

	return log
}

func (vc *VolumeController) syncVolume(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != vc.namespace {
		// Not ours, don't do anything
		return nil
	}

	volume, err := vc.ds.GetVolume(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			vc.logger.WithField("volume", name).Debug("Longhorn volume has been deleted")
			return nil
		}
		return err
	}

	log := getLoggerForVolume(vc.logger, volume)

	defaultEngineImage, err := vc.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if err != nil {
		return err
	}
	isResponsible, err := vc.isResponsibleFor(volume, defaultEngineImage)
	if err != nil {
		return err
	}
	if !isResponsible {
		// Not mines
		return nil
	}

	if volume.Status.OwnerID != vc.controllerID {
		volume.Status.OwnerID = vc.controllerID
		volume, err = vc.ds.UpdateVolumeStatus(volume)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Debugf("Volume got new owner %v", vc.controllerID)
	}

	engines, err := vc.ds.ListVolumeEngines(volume.Name)
	if err != nil {
		return err
	}
	replicas, err := vc.ds.ListVolumeReplicas(volume.Name)
	if err != nil {
		return err
	}

	if volume.DeletionTimestamp != nil {
		if volume.Status.State != types.VolumeStateDeleting {
			volume.Status.State = types.VolumeStateDeleting
			volume, err = vc.ds.UpdateVolumeStatus(volume)
			if err != nil {
				return err
			}
			vc.eventRecorder.Eventf(volume, v1.EventTypeNormal, EventReasonDelete, "Deleting volume %v", volume.Name)
		}

		if volume.Spec.AccessMode == types.AccessModeReadWriteMany {
			log.Info("Removing share manager for deleted volume")
			if err := vc.ds.DeleteShareManager(volume.Name); err != nil && !datastore.ErrorIsNotFound(err) {
				return err
			}
		}

		for _, job := range volume.Spec.RecurringJobs {
			if err := vc.ds.DeleteCronJob(types.GetCronJobNameForVolumeAndJob(volume.Name, job.Name)); err != nil {
				return err
			}
		}

		for _, e := range engines {
			if e.DeletionTimestamp == nil {
				if err := vc.ds.DeleteEngine(e.Name); err != nil {
					return err
				}
			}
		}
		for _, r := range replicas {
			if r.DeletionTimestamp == nil {
				if err := vc.ds.DeleteReplica(r.Name); err != nil {
					return err
				}
			}
		}

		kubeStatus := volume.Status.KubernetesStatus

		if kubeStatus.PVName != "" {
			if err := vc.ds.DeletePersisentVolume(kubeStatus.PVName); err != nil {
				if !datastore.ErrorIsNotFound(err) {
					return err
				}
			}
		}

		if kubeStatus.PVCName != "" && kubeStatus.LastPVCRefAt == "" {
			if err := vc.ds.DeletePersisentVolumeClaim(kubeStatus.Namespace, kubeStatus.PVCName); err != nil {
				if !datastore.ErrorIsNotFound(err) {
					return err
				}
			}
		}

		// now replicas and engines have been marked for deletion

		if engines, err := vc.ds.ListVolumeEngines(volume.Name); err != nil {
			return err
		} else if len(engines) > 0 {
			return nil
		}
		if replicas, err := vc.ds.ListVolumeReplicas(volume.Name); err != nil {
			return err
		} else if len(replicas) > 0 {
			return nil
		}
		// now replicas and engines are deleted

		return vc.ds.RemoveFinalizerForVolume(volume)
	}

	existingVolume := volume.DeepCopy()
	existingEngines := map[string]*longhorn.Engine{}
	for k, e := range engines {
		existingEngines[k] = e.DeepCopy()
	}
	existingReplicas := map[string]*longhorn.Replica{}
	for k, r := range replicas {
		existingReplicas[k] = r.DeepCopy()
	}
	defer func() {
		var lastErr error
		// create/delete engine/replica has been handled already
		// so we only need to worry about entries in the current list
		for k, r := range replicas {
			if existingReplicas[k] == nil ||
				!reflect.DeepEqual(existingReplicas[k].Spec, r.Spec) {
				if _, err := vc.ds.UpdateReplica(r); err != nil {
					lastErr = err
				}
			}
		}
		// stop updating if replicas weren't fully updated
		if lastErr == nil {
			for k, e := range engines {
				if existingEngines[k] == nil ||
					!reflect.DeepEqual(existingEngines[k].Spec, e.Spec) {
					if _, err := vc.ds.UpdateEngine(e); err != nil {
						lastErr = err
					}
				}
			}
		}
		// stop updating if engines and replicas weren't fully updated
		if lastErr == nil {
			// Make sure that we don't update condition's LastTransitionTime if the condition's values hasn't changed
			handleConditionLastTransitionTime(&existingVolume.Status, &volume.Status)
			if !reflect.DeepEqual(existingVolume.Status, volume.Status) {
				// reuse err
				_, err = vc.ds.UpdateVolumeStatus(volume)
			}
		}
		// requeue if it's conflict
		if apierrors.IsConflict(errors.Cause(err)) || apierrors.IsConflict(errors.Cause(lastErr)) {
			log.Debugf("Requeue volume due to error %v or %v", err, lastErr)
			vc.enqueueVolume(volume)
			err = nil
		}
	}()

	engine, err := vc.getCurrentEngine(volume, engines)
	if err != nil {
		return err
	}

	if len(engines) <= 1 {
		if err := vc.ReconcileEngineReplicaState(volume, engine, replicas); err != nil {
			return err
		}

		if err := vc.updateRecurringJobs(volume); err != nil {
			return err
		}

		if err := vc.upgradeEngineForVolume(volume, engine, replicas); err != nil {
			return err
		}
	}

	if err := vc.processMigration(volume, engines, replicas); err != nil {
		return err
	}

	if err := vc.ReconcileShareManagerState(volume); err != nil {
		return err
	}

	if err := vc.ReconcileVolumeState(volume, engines, replicas); err != nil {
		return err
	}

	if err := vc.cleanupReplicas(volume, engines, replicas); err != nil {
		return err
	}

	return nil
}

// handleConditionLastTransitionTime rollback to the existing condition object if condition's values hasn't changed
func handleConditionLastTransitionTime(existingStatus, newStatus *types.VolumeStatus) {
	for conditionType, condition := range newStatus.Conditions {
		existingCondition, ok := existingStatus.Conditions[conditionType]
		if ok &&
			existingCondition.Status == condition.Status &&
			existingCondition.Reason == condition.Reason &&
			existingCondition.Message == condition.Message {
			newStatus.Conditions[conditionType] = existingCondition
		}
	}
}

func (vc *VolumeController) getCurrentEngine(v *longhorn.Volume, es map[string]*longhorn.Engine) (*longhorn.Engine, error) {
	if len(es) == 0 {
		return nil, nil
	}

	if len(es) == 1 {
		for _, e := range es {
			return e, nil
		}
	}

	// len(es) > 1
	node := v.Status.CurrentNodeID
	if node != "" {
		for _, e := range es {
			if e.Spec.NodeID == node {
				return e, nil
			}
		}
		return nil, fmt.Errorf("BUG: multiple engines detected but none matched volume %v attached node %v", v.Name, node)
	}
	return nil, fmt.Errorf("BUG: multiple engines detected when volume %v is detached", v.Name)
}

// EvictReplicas do creating one more replica for eviction, if requested
func (vc *VolumeController) EvictReplicas(v *longhorn.Volume,
	e *longhorn.Engine, rs map[string]*longhorn.Replica, healthyCount int) (err error) {
	log := getLoggerForVolume(vc.logger, v)

	for _, replica := range rs {
		if replica.Status.EvictionRequested == true &&
			healthyCount == v.Spec.NumberOfReplicas {
			if err := vc.replenishReplicas(v, e, rs, ""); err != nil {
				log.WithError(err).Error("Failed to create new replica for replica eviction")
				vc.eventRecorder.Eventf(v, v1.EventTypeWarning,
					EventReasonFailedEviction,
					"volume %v failed to create one more replica", v.Name)
				return err
			}
			log.Debug("Creating one more replica for eviction")
			break
		}
	}

	return nil
}

// ReconcileEngineReplicaState will get the current main engine e.Status.ReplicaModeMap, e.Status.RestoreStatus,
// and e.Status.purgeStatus then update v and rs accordingly.
func (vc *VolumeController) ReconcileEngineReplicaState(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to reconcile engine/replica state for %v", v.Name)
		if v.Status.Robustness != types.VolumeRobustnessDegraded {
			v.Status.LastDegradedAt = ""
		}
	}()

	if e == nil {
		return nil
	}

	if e.Status.CurrentState == types.InstanceStateUnknown {
		if v.Status.Robustness != types.VolumeRobustnessUnknown {
			v.Status.Robustness = types.VolumeRobustnessUnknown
			vc.eventRecorder.Eventf(v, v1.EventTypeWarning, EventReasonUnknown, "volume %v robustness is unknown", v.Name)
		}
		return nil
	}
	if e.Status.CurrentState != types.InstanceStateRunning {
		return nil
	}

	// wait for monitoring to start
	if e.Status.ReplicaModeMap == nil {
		return nil
	}

	replicaList := []*longhorn.Replica{}
	for _, r := range rs {
		replicaList = append(replicaList, r)
	}

	restoreStatusMap := map[string]*types.RestoreStatus{}
	for addr, status := range e.Status.RestoreStatus {
		rName := datastore.ReplicaAddressToReplicaName(addr, replicaList)
		if _, exists := rs[rName]; exists {
			restoreStatusMap[rName] = status
		}
	}

	purgeStatusMap := map[string]*types.PurgeStatus{}
	for addr, status := range e.Status.PurgeStatus {
		rName := datastore.ReplicaAddressToReplicaName(addr, replicaList)
		if _, exists := rs[rName]; exists {
			purgeStatusMap[rName] = status
		}
	}

	// 1. remove ERR replicas
	// 2. count RW replicas
	healthyCount := 0
	for rName, mode := range e.Status.ReplicaModeMap {
		r := rs[rName]
		if r == nil {
			continue
		}
		restoreStatus := restoreStatusMap[rName]
		purgeStatus := purgeStatusMap[rName]
		if mode == types.ReplicaModeERR ||
			(restoreStatus != nil && restoreStatus.Error != "") ||
			(purgeStatus != nil && purgeStatus.Error != "") {
			if restoreStatus != nil && restoreStatus.Error != "" {
				vc.eventRecorder.Eventf(v, v1.EventTypeWarning, EventReasonFailedRestore, "replica %v failed the restore: %s", r.Name, restoreStatus.Error)
			}
			if purgeStatus != nil && purgeStatus.Error != "" {
				vc.eventRecorder.Eventf(v, v1.EventTypeWarning, EventReasonFailedSnapshotPurge, "replica %v failed the snapshot purge: %s", r.Name, purgeStatus.Error)
			}
			e.Spec.LogRequested = true
			r.Spec.LogRequested = true
			if r.Spec.FailedAt == "" {
				r.Spec.FailedAt = vc.nowHandler()
			}
			r.Spec.DesireState = types.InstanceStateStopped
		} else if mode == types.ReplicaModeRW {
			// record once replica became healthy, so if it
			// failed in the future, we can tell it apart
			// from replica failed during rebuilding
			if r.Spec.HealthyAt == "" {
				vc.backoff.DeleteEntry(r.Name)
				r.Spec.HealthyAt = vc.nowHandler()
				r.Spec.RebuildRetryCount = 0
			}
			healthyCount++
		}
	}
	// If a replica failed at attaching stage,
	// there is no record in e.Status.ReplicaModeMap
	for _, r := range rs {
		if r.Spec.FailedAt == "" && r.Status.CurrentState == types.InstanceStateError {
			e.Spec.LogRequested = true
			r.Spec.LogRequested = true
			r.Spec.FailedAt = vc.nowHandler()
			r.Spec.DesireState = types.InstanceStateStopped
		}
	}

	oldRobustness := v.Status.Robustness
	if healthyCount == 0 { // no healthy replica exists, going to faulted
		// ReconcileVolumeState() will deal with the faulted case
		return nil
	} else if healthyCount >= v.Spec.NumberOfReplicas {
		v.Status.Robustness = types.VolumeRobustnessHealthy
		if oldRobustness == types.VolumeRobustnessDegraded {
			vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonHealthy, "volume %v became healthy", v.Name)
		}

		// Evict replicas for the volume
		if err := vc.EvictReplicas(v, e, rs, healthyCount); err != nil {
			return err
		}

		// Migrate local replica when Data Locality is on
		// We turn off data locality while doing auto-attaching or restoring (e.g. frontend is disabled)
		if v.Status.State == types.VolumeStateAttached && !v.Status.FrontendDisabled &&
			!isDataLocalityDisabled(v) && !hasLocalReplicaOnSameNodeAsEngine(e, rs) {
			if err := vc.replenishReplicas(v, e, rs, e.Spec.NodeID); err != nil {
				return err
			}
		}

	} else { // healthyCount < v.Spec.NumberOfReplicas
		v.Status.Robustness = types.VolumeRobustnessDegraded
		if oldRobustness != types.VolumeRobustnessDegraded {
			v.Status.LastDegradedAt = vc.nowHandler()
			vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonDegraded, "volume %v became degraded", v.Name)
		}

		cliAPIVersion, err := vc.ds.GetEngineImageCLIAPIVersion(e.Status.CurrentImage)
		if err != nil {
			return err
		}
		// Rebuild is not supported when:
		//   1. the volume is old restore/DR volumes.
		//   2. the volume is expanding size.
		if !((v.Status.IsStandby || v.Status.RestoreRequired) && cliAPIVersion < engineapi.CLIVersionFour) &&
			v.Spec.Size == e.Status.CurrentSize {
			if err := vc.replenishReplicas(v, e, rs, ""); err != nil {
				return err
			}
		}
		// replicas will be started by ReconcileVolumeState() later
	}
	return nil
}

func (vc *VolumeController) getHealthyReplicaCount(rs map[string]*longhorn.Replica) int {
	var healthyCount int

	for _, r := range rs {
		if r.Spec.FailedAt == "" && r.Spec.HealthyAt != "" {
			healthyCount++
		}
	}

	return healthyCount
}

func (vc *VolumeController) hasReplicaEvictionRequested(rs map[string]*longhorn.Replica) bool {
	for _, r := range rs {
		if r.Status.EvictionRequested == true {
			return true
		}
	}

	return false
}

func (vc *VolumeController) cleanupReplicas(v *longhorn.Volume, es map[string]*longhorn.Engine, rs map[string]*longhorn.Replica) error {
	// TODO: I don't think it's a good idea to cleanup replicas during a migration or engine image update
	// 	since the getHealthyReplicaCount function doesn't differentiate between replicas of different engines
	// 	then during cleanupExtraHealthyReplicas the condition `healthyCount > v.Spec.NumberOfReplicas` will be true
	//  which can lead to incorrect deletion of replicas.
	if vc.isVolumeMigrating(v) || vc.isVolumeUpgrading(v) {
		return nil
	}

	e, err := vc.getCurrentEngine(v, es)
	if err != nil {
		return err
	}

	if err := vc.cleanupCorruptedOrStaleReplicas(v, rs); err != nil {
		return err
	}

	if err := vc.cleanupFailedToScheduledReplicas(v, rs); err != nil {
		return err
	}

	if err := vc.cleanupExtraHealthyReplicas(v, e, rs); err != nil {
		return err
	}
	return nil
}

func (vc *VolumeController) cleanupCorruptedOrStaleReplicas(v *longhorn.Volume, rs map[string]*longhorn.Replica) error {
	healthyCount := vc.getHealthyReplicaCount(rs)
	cleanupLeftoverReplicas := !vc.isVolumeUpgrading(v) && !vc.isVolumeMigrating(v)
	log := getLoggerForVolume(vc.logger, v)

	for _, r := range rs {
		if cleanupLeftoverReplicas {
			if !r.Spec.Active {
				// Leftover by live upgrade. Successful or not, there are replicas left to clean up
				if err := vc.deleteReplica(r, rs); err != nil {
					return err
				}
				continue
			} else if r.Spec.EngineImage != v.Spec.EngineImage {
				// r.Spec.Active shouldn't be set for the leftover replicas, something must wrong
				log.WithField("replica", r.Name).Errorf("BUG: replica engine image %v is different from volume engine image %v, "+
					"but replica spec.Active has been set", r.Spec.EngineImage, v.Spec.EngineImage)
			}
		}

		if r.Spec.FailedAt == "" {
			continue
		}

		if r.DeletionTimestamp != nil {
			continue
		}
		staled := false
		if v.Spec.StaleReplicaTimeout > 0 && util.TimestampAfterTimeout(r.Spec.FailedAt,
			time.Duration(int64(v.Spec.StaleReplicaTimeout*60))*time.Second) {

			staled = true
		}

		// 1. failed before ever became healthy (RW), except for replica reuse failure during rebuilding
		// 2. failed too long ago, became stale and unnecessary to keep
		// around, unless we don't any healthy replicas
		if (r.Spec.HealthyAt == "" && r.Spec.RebuildRetryCount == 0) || (healthyCount != 0 && staled) {
			log.WithField("replica", r.Name).Info("Cleaning up corrupted, staled replica")
			if err := vc.deleteReplica(r, rs); err != nil {
				return errors.Wrapf(err, "cannot cleanup staled replica %v", r.Name)
			}
		}
	}

	return nil
}

func (vc *VolumeController) cleanupFailedToScheduledReplicas(v *longhorn.Volume, rs map[string]*longhorn.Replica) (err error) {
	healthyCount := vc.getHealthyReplicaCount(rs)
	hasEvictionRequestedReplicas := vc.hasReplicaEvictionRequested(rs)

	if healthyCount >= v.Spec.NumberOfReplicas {
		for _, r := range rs {
			if !hasEvictionRequestedReplicas {
				if r.Spec.HealthyAt == "" && r.Spec.FailedAt == "" && r.Spec.NodeID == "" &&
					(isDataLocalityDisabled(v) || r.Spec.HardNodeAffinity != v.Status.CurrentNodeID) {
					logrus.Infof("Cleaning up failed to scheduled replica %v", r.Name)
					if err := vc.deleteReplica(r, rs); err != nil {
						return errors.Wrapf(err, "cannot cleanup failed to scheduled replica %v", r.Name)
					}
				}
			}
		}
	}

	return nil
}

func (vc *VolumeController) cleanupExtraHealthyReplicas(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) (err error) {
	healthyCount := vc.getHealthyReplicaCount(rs)

	if healthyCount > v.Spec.NumberOfReplicas {
		for _, r := range rs {
			// Clean up eviction requested replica
			if r.Status.EvictionRequested {
				if err := vc.deleteReplica(r, rs); err != nil {
					vc.eventRecorder.Eventf(v, v1.EventTypeWarning,
						EventReasonFailedEviction,
						"volume %v failed to evict replica %v",
						v.Name, r.Name)
					return err
				}
				logrus.Debugf("Evicted replica %v", r.Name)
				return nil
			}
		}
	}

	// Clean up extra healthy replica with data-locality finished
	healthyCount = vc.getHealthyReplicaCount(rs)

	if healthyCount > v.Spec.NumberOfReplicas &&
		!isDataLocalityDisabled(v) &&
		hasLocalReplicaOnSameNodeAsEngine(e, rs) {
		rNames, err := vc.getPreferredReplicaCandidatesForDeletion(rs)
		if err != nil {
			return err
		}

		// Randomly delete extra non-local healthy replicas in the preferred candidate list rNames
		// Sometime cleanupExtraHealthyReplicas() is called more than once with the same input (v,e,rs).
		// To make the deleting operation idempotent and prevent deleting more replica than needed,
		// we always delete the replica with the smallest name.
		sort.Strings(rNames)
		for _, rName := range rNames {
			r := rs[rName]
			if r.Spec.NodeID != e.Spec.NodeID {
				return vc.deleteReplica(r, rs)
			}
		}
	}

	return nil
}

// TODO: JM - Think this might only be relevant for reconcileVolumeCreation
func (vc VolumeController) reconcileVolumeScheduling(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) error {
	log := getLoggerForVolume(vc.logger, v)
	allowCreateDegraded, err := vc.ds.GetSettingAsBool(types.SettingNameAllowVolumeCreationWithDegradedAvailability)
	if err != nil {
		log.Warnf("Failed to parse setting %v unsetting allowCreateDegraded", types.SettingNameAllowVolumeCreationWithDegradedAvailability)
		allowCreateDegraded = false
	}

	atLeastOneReplicaAvailable := false
	allReplicasScheduled := true
	for _, r := range rs {
		scheduledReplica := r
		if r.Spec.NodeID == "" {
			scheduledReplica, err = vc.scheduler.ScheduleReplica(r, rs, v)
			if err != nil {
				return err
			}
		}

		isReplicaScheduled := scheduledReplica != nil && scheduledReplica.Spec.NodeID != ""
		isReplicaAvailable := isReplicaScheduled && scheduledReplica.Spec.FailedAt == ""
		allReplicasScheduled = allReplicasScheduled && isReplicaScheduled
		atLeastOneReplicaAvailable = atLeastOneReplicaAvailable || isReplicaAvailable

		if !isReplicaScheduled {
			log.WithField("replica", r.Name).Errorf("unable to schedule replica with HardNodeAffinity = %v", r.Spec.HardNodeAffinity)
			v.Status.Conditions = types.SetCondition(v.Status.Conditions,
				types.VolumeConditionTypeScheduled, types.ConditionStatusFalse,
				types.VolumeConditionReasonLocalReplicaSchedulingFailure, "")
		} else {
			rs[r.Name] = scheduledReplica
		}
	}

	if allReplicasScheduled {
		v.Status.Conditions = types.SetCondition(v.Status.Conditions,
			types.VolumeConditionTypeScheduled, types.ConditionStatusTrue, "", "")
	} else if allowCreateDegraded && atLeastOneReplicaAvailable {
		v.Status.Conditions = types.SetCondition(v.Status.Conditions,
			types.VolumeConditionTypeScheduled, types.ConditionStatusTrue, "",
			"Reset schedulable due to allow volume creation with degraded availability")
	}
	return nil
}

func (vc *VolumeController) reconcileVolumeAttachment(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) (err error) {
	oldState := v.Status.State
	log := getLoggerForVolume(vc.logger, v)

	// wait for offline engine upgrade to finish
	if v.Status.State == types.VolumeStateDetached && v.Status.CurrentImage != v.Spec.EngineImage {
		log.Debug("Wait for offline volume upgrade to finish")
		return nil
	}

	// the final state will be determined at the end of the clause
	v.Status.State = types.VolumeStateAttaching

	// if engine was running, then we are attached already
	// (but we may still need to start rebuilding replicas)
	if e.Status.CurrentState != types.InstanceStateRunning && e.Status.CurrentState != types.InstanceStateUnknown {
		e.Spec.UpgradedReplicaAddressMap = map[string]string{}
	}

	isCLIAPIVersionOne := false
	if v.Status.CurrentImage != "" {
		isCLIAPIVersionOne, err = vc.ds.IsEngineImageCLIAPIVersionOne(v.Status.CurrentImage)
		if err != nil {
			return err
		}
	}

	for _, r := range rs {
		// Don't attempt to start the replica or do anything else if it hasn't been scheduled.
		if r.Spec.NodeID == "" {
			continue
		}
		nodeDown, err := vc.ds.IsNodeDownOrDeleted(r.Spec.NodeID)
		if err != nil {
			return errors.Wrapf(err, "failed to evaluate status of node %v for replica %v", r.Spec.NodeID, r.Name)
		}
		if nodeDown {
			if r.Spec.FailedAt == "" {
				r.Spec.FailedAt = vc.nowHandler()
			}
			r.Spec.DesireState = types.InstanceStateStopped
		} else if r.Spec.FailedAt == "" && r.Spec.EngineImage == v.Status.CurrentImage {
			if r.Status.CurrentState == types.InstanceStateStopped {
				r.Spec.DesireState = types.InstanceStateRunning
			}
		}
	}

	replicaAddressMap := map[string]string{}
	for _, r := range rs {
		isValidReplicaForEngine := r.Spec.NodeID != "" && r.Spec.FailedAt == "" &&
			r.Status.CurrentState != types.InstanceStateError &&
			r.Spec.EngineName == e.Name && r.Spec.EngineImage == v.Status.CurrentImage
		if !isValidReplicaForEngine {
			continue
		}

		// wait for all potentially healthy replicas become running
		if r.Status.CurrentState != types.InstanceStateRunning {
			return nil
		}
		if r.Status.IP == "" {
			log.WithField("replica", r.Name).Error("BUG: replica is running but IP is empty")
			// TODO: JM - we should probably set desired state to stopped, so that we can restart this replica
			continue
		}
		if r.Status.Port == 0 {
			// Do not skip this replica if its engine image is CLIAPIVersion 1.
			if !isCLIAPIVersionOne {
				log.WithField("replica", r.Name).Error("BUG: replica is running but Port is empty")
				// TODO: JM - we should probably set desired state to stopped, so that we can restart this replica
				continue
			}
		}
		replicaAddressMap[r.Name] = imutil.GetURL(r.Status.IP, r.Status.Port)
	}
	if len(replicaAddressMap) == 0 {
		return fmt.Errorf("no healthy or scheduled replica available for volume attachment")
	}

	e.Spec.NodeID = v.Spec.NodeID
	e.Spec.ReplicaAddressMap = replicaAddressMap
	e.Spec.DesireState = types.InstanceStateRunning
	e.Spec.DisableFrontend = v.Status.FrontendDisabled
	e.Spec.Frontend = v.Spec.Frontend

	// wait for engine to be up
	if e.Status.CurrentState != types.InstanceStateRunning {
		return nil
	}

	if e.Spec.RequestedBackupRestore != "" {
		v.Status.Conditions = types.SetCondition(v.Status.Conditions,
			types.VolumeConditionTypeRestore, types.ConditionStatusTrue, types.VolumeConditionReasonRestoreInProgress, "")
	}

	// TODO: JM - this does not belong here, expansion should be handlded by dedicated controller or as part of reconcileVolumeSize
	if v.Status.ExpansionRequired {
		// The engine expansion is complete
		if v.Spec.Size == e.Status.CurrentSize {
			if v.Spec.Frontend == types.VolumeFrontendBlockDev {
				log.Info("Prepare to start frontend and expand the file system for volume")
				// Here is the exception that the fronend is enabled but the volume is auto attached.
				v.Status.FrontendDisabled = false
				e.Spec.DisableFrontend = false
				// Wait for the frontend to be up after e.Spec.DisableFrontend getting changed.
				// If the frontend is up, the engine endpoint won't be empty.
				if e.Status.Endpoint != "" {
					// Best effort. We don't know if there is a file system built in the volume.
					if err := util.ExpandFileSystem(v.Name); err != nil {
						log.WithError(err).Warn("Failed to expand the file system for volume")
					} else {
						log.Info("Succeeded to expand the file system for volume")
					}
					v.Status.ExpansionRequired = false
				}
			} else {
				log.Info("Expanding file system is not supported for volume frontend")
				v.Status.ExpansionRequired = false
			}
			vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonSucceededExpansion,
				"Succeeds to expand the volume %v to size %v, will automatically detach it if it's not DR volume", v.Name, e.Status.CurrentSize)
		}
	}

	// we can set currentNodeID and acknowledge attachment
	v.Status.CurrentNodeID = v.Spec.NodeID
	v.Status.State = types.VolumeStateAttached
	if oldState != v.Status.State {
		vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonAttached, "volume %v has been attached to %v", v.Name, v.Status.CurrentNodeID)
	}
	return nil
}

func (vc *VolumeController) reconcileVolumeDetachment(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) error {
	log := getLoggerForVolume(vc.logger, v)
	oldState := v.Status.State
	oldNode := v.Status.CurrentNodeID
	v.Status.State = types.VolumeStateDetaching

	v.Status.Conditions = types.SetCondition(v.Status.Conditions,
		types.VolumeConditionTypeRestore, types.ConditionStatusFalse, "", "")

	if v.Status.Robustness != types.VolumeRobustnessFaulted {
		v.Status.Robustness = types.VolumeRobustnessUnknown
	} else {
		if v.Status.RestoreRequired || v.Status.IsStandby {
			v.Status.Conditions = types.SetCondition(v.Status.Conditions,
				types.VolumeConditionTypeRestore, types.ConditionStatusFalse, types.VolumeConditionReasonRestoreFailure, "All replica restore failed and the volume became Faulted")
		}
	}

	// check if any replica has been RW yet
	hasHealthyReplica := false
	for _, r := range rs {
		hasHealthyReplica = hasHealthyReplica || r.Spec.HealthyAt != ""
	}

	// stop rebuilding
	if hasHealthyReplica {
		for _, r := range rs {
			if r.Spec.HealthyAt == "" && r.Spec.FailedAt == "" {
				r.Spec.FailedAt = vc.nowHandler()
				r.Spec.DesireState = types.InstanceStateStopped
			}
		}
	}

	// stop engine
	if e.Spec.DesireState != types.InstanceStateStopped || e.Spec.NodeID != "" {
		if v.Status.Robustness == types.VolumeRobustnessFaulted {
			e.Spec.LogRequested = true
		}
		// Prevent this field from being unset when restore/DR volumes crash unexpectedly.
		if !v.Status.RestoreRequired && !v.Status.IsStandby {
			e.Spec.BackupVolume = ""
		}
		e.Spec.RequestedBackupRestore = ""
		e.Spec.NodeID = ""
		e.Spec.DesireState = types.InstanceStateStopped
	}

	// must make sure engine stopped first before stopping replicas
	// otherwise we may corrupt the data
	if e.Status.CurrentState != types.InstanceStateStopped {
		log.WithField("engine", e.Name).Debugf("Waiting for engine in state %v to stop, before acknowledging detachment", e.Status.CurrentState)
		return nil
	}

	allReplicasStopped := true
	for _, r := range rs {
		if r.Spec.DesireState != types.InstanceStateStopped {
			if v.Status.Robustness == types.VolumeRobustnessFaulted {
				r.Spec.LogRequested = true
			}
			r.Spec.DesireState = types.InstanceStateStopped
		}

		if r.Status.CurrentState != types.InstanceStateStopped {
			log.WithField("replica", r.Name).Debugf("Waiting for replica in state %v to stop, before acknowledging detachment", r.Status.CurrentState)
			allReplicasStopped = false
		}
	}

	// wait for all replicas stopped
	if !allReplicasStopped {
		log.Debugf("Waiting for all replicas to stop, before acknowledging detachment")
		return nil
	}

	// we can unset current nodeID and acknowledge detachment
	v.Status.CurrentNodeID = ""
	v.Status.State = types.VolumeStateDetached
	if oldState != v.Status.State {
		log.Infof("Volume has been detached from %v", oldNode)
		vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonDetached, "volume %v has been detached from %v", v.Name, oldNode)
	}
	return nil
}

// ReconcileVolumeState handles the attaching and detaching of volume
func (vc *VolumeController) ReconcileVolumeState(v *longhorn.Volume, es map[string]*longhorn.Engine, rs map[string]*longhorn.Replica) (err error) {
	// remountRequired should be set anytime an error condition happens, that requires that the workload pods are restarted
	// so that they can go through a detach/attach cycle and start with a valid engine connection, ensure that the condition
	// you use to evaluate whether a remount is required, survives potentially missed volume status updates, since engine + replicas
	// get updated before the volume it's possible that the engine state between 2 reconcile cycles has changed so you need to ensure
	// that the condition would still hold even if the engine state has changed in the meantime.
	remountRequired := false
	defer func() {
		err = errors.Wrapf(err, "fail to reconcile volume state for %v", v.Name)
		if remountRequired {
			v.Status.RemountRequestedAt = vc.nowHandler()
			msg := fmt.Sprintf("Volume %v requested remount at %v", v.Name, v.Status.RemountRequestedAt)
			vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonRemount, msg)
		}
	}()

	log := getLoggerForVolume(vc.logger, v)

	e, err := vc.getCurrentEngine(v, es)
	if err != nil {
		return err
	}

	if v.Status.CurrentImage == "" {
		v.Status.CurrentImage = v.Spec.EngineImage
	}

	if err := vc.checkAndInitVolumeRestore(v); err != nil {
		return err
	}
	if err := vc.updateRequestedBackupForVolumeRestore(v, e); err != nil {
		return err
	}

	if v.Status.CurrentNodeID == "" {
		// `PendingNodeID != ""` means the engine dead unexpectedly.
		// The volume will be detached by cleaning up `CurrentNodeID` then be reattached by `v.Status.CurrentNodeID = v.Status.PendingNodeID`.
		// Hence this auto attachment logic shouldn't set `CurrentNodeID` in this case.
		if v.Status.PendingNodeID == "" && v.Status.Robustness != types.VolumeRobustnessFaulted {
			v.Status.CurrentNodeID = v.Spec.NodeID
		}
	} else if v.Spec.NodeID != "" && v.Spec.NodeID != v.Status.CurrentNodeID {
		// in the case where the currentNode id upgrade from the migration failed
		// we can check if we have a current active engine and if it's on v.Spec
		// then it's safe to switch the current node id to v.Spec
		// TODO: we still need to rework all of this, so that the user can just specify a nodeID
		// 	instead of having to go through the detach/attach cycle, like in the case of the share-manager
		if e != nil && e.Spec.NodeID == v.Spec.NodeID && e.DeletionTimestamp == nil {
			vc.logger.Infof("switching current node id from %v to %v since we have an engine on that node already", v.Status.CurrentNodeID, v.Spec.NodeID)
			v.Status.CurrentNodeID = v.Spec.NodeID

			// ensure that the replicas are marked as active
			if err := vc.switchActiveReplicas(rs, func(r *longhorn.Replica, engineName string) bool {
				return r.Spec.EngineName == engineName
			}, e.Name); err != nil {
				return err
			}
		}

		return fmt.Errorf("volume %v has already attached to node %v, but asked to attach to node %v", v.Name, v.Status.CurrentNodeID, v.Spec.NodeID)
	}

	// new volume creation
	newVolume := e == nil
	if e == nil {
		e, err = vc.createEngine(v)
		if err != nil {
			return err
		}
		es[e.Name] = e
	}

	if len(e.Status.Snapshots) != 0 {
		actualSize := int64(0)
		for _, snapshot := range e.Status.Snapshots {
			size, err := util.ConvertSize(snapshot.Size)
			if err != nil {
				log.WithField("snapshot", snapshot.Name).WithError(err).Errorf("BUG: unable to parse snapshot size %v", snapshot.Size)
				// continue checking for other snapshots
			}
			actualSize += size
		}
		v.Status.ActualSize = actualSize
	}

	if len(e.Status.Snapshots) > VolumeSnapshotsWarningThreshold {
		msg := fmt.Sprintf("Snapshots count is %v over the warning threshold %v", len(e.Status.Snapshots), VolumeSnapshotsWarningThreshold)
		v.Status.Conditions = types.SetCondition(v.Status.Conditions, types.VolumeConditionTypeTooManySnapshots, types.ConditionStatusTrue, types.VolumeConditionReasonTooManySnapshots, msg)
	} else {
		v.Status.Conditions = types.SetCondition(v.Status.Conditions, types.VolumeConditionTypeTooManySnapshots, types.ConditionStatusFalse, "", "")
	}

	// initial replica creation
	if len(rs) == 0 {
		if err := vc.replenishReplicas(v, e, rs, ""); err != nil {
			return err
		}
	}

	// reset log requested, if it has been fetched
	e.Spec.LogRequested = e.Spec.LogRequested && !e.Status.LogFetched
	for _, r := range rs {
		r.Spec.LogRequested = r.Spec.LogRequested && !r.Status.LogFetched
	}

	if err := vc.reconcileVolumeScheduling(v, e, rs); err != nil {
		return err
	}

	if err := vc.reconcileVolumeSize(v, e, rs); err != nil {
		return err
	}

	// TODO: JM - don't need the auto attach/detach, this should be handled by the appropriate controllers (salvage, expansion, dr/backup)
	//	if err := vc.checkForAutoAttachment(v, e, rs, scheduled); err != nil {
	//		return err
	//	}
	//	if err := vc.checkForAutoDetachment(v, e, rs); err != nil {
	//		return err
	//	}

	// Disabled the frontend if it's requested
	v.Status.FrontendDisabled = v.Spec.DisableFrontend

	// Clear SalvageRequested flag if SalvageExecuted flag has been set.
	// TODO: JM - handled by the salvage controller
	//	if e.Spec.SalvageRequested && e.Status.SalvageExecuted {
	//		e.Spec.SalvageRequested = false
	//	}

	allFaulted := true
	for _, r := range rs {
		allFaulted = allFaulted && r.Spec.FailedAt != ""
	}
	if allFaulted {
		v.Status.Robustness = types.VolumeRobustnessFaulted
	} else {
		// we only care about engine failures if the volume isn't fully detached
		if e.Status.CurrentState == types.InstanceStateError && (v.Spec.NodeID != "" || v.Status.CurrentNodeID != "") {
			msg := fmt.Sprintf("Engine of volume %v is in error state unexpectedly, remount required", v.Name)
			log.Warn(msg)
			vc.eventRecorder.Event(v, v1.EventTypeWarning, EventReasonDetachedUnexpectly, msg)
			e.Spec.LogRequested = true
			for _, r := range rs {
				if r.Status.CurrentState == types.InstanceStateRunning {
					r.Spec.LogRequested = true
				}
			}

			remountRequired = true
			// TODO: JM - we might miss the remount, in the case where the engine state is updated but the volume update fails
		}

		// The volume was faulty and there are now usable replicas.
		// Therefore, we set RemountRequestedAt so that KubernetesPodController restarts the workload pod
		if v.Status.Robustness == types.VolumeRobustnessFaulted {
			log.Info("Volume was faulted but now has valid replicas, remount required")
			v.Status.Robustness = types.VolumeRobustnessUnknown
			remountRequired = true
		}
	}

	// if the volume was fully attached but the engine node is out of sync with the volume node
	// we need to ensure that a remount happens, so that the workload pods can go through a detach/attach cycle
	// TODO: JM - we might miss the remount, in the case where the engine state is updated but the volume update fails
	if v.Status.State == types.VolumeStateAttached &&
		e.Spec.NodeID != "" && v.Spec.NodeID != "" &&
		e.Spec.NodeID != v.Spec.NodeID {

		log.Errorf("Engine is on node %v vs volume on %v, remount requried", e.Spec.NodeID, v.Status.CurrentNodeID)
		remountRequired = true
	}

	// if the v.Spec.NodeID has changed from the previous attachment, we need to go through a detach cycle
	// before processing the new node attachment, this is so we can cleanup engine & replica states
	processAttachment := v.Spec.NodeID != "" &&
		(v.Status.CurrentNodeID == "" || v.Status.CurrentNodeID == v.Spec.NodeID) &&
		(e.Spec.NodeID == "" || e.Spec.NodeID == v.Spec.NodeID)

	if newVolume {
		v.Status.State = types.VolumeStateCreating
		// TODO: JM - do volume creation operations
	} else if processAttachment {
		return vc.reconcileVolumeAttachment(v, e, rs)
	} else {
		return vc.reconcileVolumeDetachment(v, e, rs)
	}

	return nil
}

func (vc *VolumeController) getPreferredReplicaCandidatesForDeletion(rs map[string]*longhorn.Replica) ([]string, error) {
	diskToReplicaMap := make(map[string][]string)
	nodeToReplicaMap := make(map[string][]string)
	zoneToReplicaMap := make(map[string][]string)

	nodeList, err := vc.ds.ListNodes()
	if err != nil {
		return nil, err
	}

	for _, r := range rs {
		diskToReplicaMap[r.Spec.NodeID+r.Spec.DiskID] = append(diskToReplicaMap[r.Spec.NodeID+r.Spec.DiskID], r.Name)
		nodeToReplicaMap[r.Spec.NodeID] = append(nodeToReplicaMap[r.Spec.NodeID], r.Name)
		if node, ok := nodeList[r.Spec.NodeID]; ok {
			zoneToReplicaMap[node.Status.Zone] = append(zoneToReplicaMap[node.Status.Zone], r.Name)
		}
	}

	var deletionCandidates []string

	// prefer to delete replicas on the same disk
	deletionCandidates = findValueWithBiggestLength(diskToReplicaMap)
	if len(deletionCandidates) > 1 {
		return deletionCandidates, nil
	}

	// if all replicas are on different disks, prefer to delete replicas on the same node
	deletionCandidates = findValueWithBiggestLength(nodeToReplicaMap)
	if len(deletionCandidates) > 1 {
		return deletionCandidates, nil
	}

	// if all replicas are on different nodes, prefer to delete replicas on the same zone
	deletionCandidates = findValueWithBiggestLength(zoneToReplicaMap)
	if len(deletionCandidates) > 1 {
		return deletionCandidates, nil
	}

	// if all replicas are on different zones, return all replicas' names in the input RS
	deletionCandidates = make([]string, 0, len(rs))
	for rName := range rs {
		deletionCandidates = append(deletionCandidates, rName)
	}
	return deletionCandidates, nil
}

func findValueWithBiggestLength(m map[string][]string) []string {
	targetKey, currentMax := "", 0
	for k, v := range m {
		if len(v) > currentMax {
			targetKey, currentMax = k, len(v)
		}
	}
	return m[targetKey]
}

func isDataLocalityDisabled(v *longhorn.Volume) bool {
	return string(v.Spec.DataLocality) == "" || v.Spec.DataLocality == types.DataLocalityDisabled
}

// hasLocalReplicaOnSameNodeAsEngine returns true if one of the following condition is satisfied:
// 1. there exist a replica on the same node as engine
// 2. there exist a replica with HardNodeAffinity set to engine's NodeID
func hasLocalReplicaOnSameNodeAsEngine(e *longhorn.Engine, rs map[string]*longhorn.Replica) bool {
	for _, r := range rs {
		if e.Spec.NodeID != "" && (r.Spec.NodeID == e.Spec.NodeID || r.Spec.HardNodeAffinity == e.Spec.NodeID) {
			return true
		}
	}
	return false
}

// replenishReplicas will keep replicas count to v.Spec.NumberOfReplicas
// It will count all the potentially usable replicas, since some replicas maybe
// blank or in rebuilding state
func (vc *VolumeController) replenishReplicas(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica, hardNodeAffinity string) error {
	disableReplicaRebuild, err := vc.ds.GetSettingAsBool(types.SettingNameDisableReplicaRebuild)
	if err != nil {
		return err
	}

	// If disabled replica rebuild, skip all the rebuild except first time creation.
	if (len(rs) != 0) && (disableReplicaRebuild == true) {
		return nil
	}

	if vc.isVolumeMigrating(v) {
		return nil
	}

	if e == nil {
		return fmt.Errorf("BUG: replenishReplica needs a valid engine")
	}

	// To prevent duplicate IP for different replicas cause problem
	// Wait for engine to:
	// 1. make sure the existing healthy replicas have shown up in engine.spec.ReplicaAddressMap
	// 2. has recognized all the replicas from the spec.ReplicaAddressMap in the status.ReplicaModeMap
	// 3. has cleaned up the extra entries in the status.ReplicaModeMap
	// https://github.com/longhorn/longhorn/issues/687
	if !vc.hasEngineStatusSynced(e, rs) {
		return nil
	}

	if currentRebuilding := getRebuildingReplicaCount(e); currentRebuilding != 0 {
		return nil
	}

	log := getLoggerForVolume(vc.logger, v)

	replenishCount := vc.getReplenishReplicasCount(v, rs)
	// For regular rebuild case or data locality case, rebuild one replica at a time
	if (len(rs) != 0 && replenishCount > 0) || hardNodeAffinity != "" {
		replenishCount = 1
	}

	for i := 0; i < replenishCount; i++ {
		reusableFailedReplica, err := vc.scheduler.CheckAndReuseFailedReplica(rs, v, hardNodeAffinity)
		if err != nil {
			return errors.Wrapf(err, "failed to reuse a failed replica during replica replenishment")
		}
		if reusableFailedReplica != nil {
			if !vc.backoff.IsInBackOffSinceUpdate(reusableFailedReplica.Name, time.Now()) {
				log.Debugf("Failed replica %v will be reused during rebuilding", reusableFailedReplica.Name)
				reusableFailedReplica.Spec.FailedAt = ""
				reusableFailedReplica.Spec.HealthyAt = ""
				reusableFailedReplica.Spec.RebuildRetryCount++
				vc.backoff.Next(reusableFailedReplica.Name, time.Now())
				rs[reusableFailedReplica.Name] = reusableFailedReplica
				continue
			}
			log.Debugf("Cannot reuse failed replica %v immediately, backoff period is %v now",
				reusableFailedReplica.Name, vc.backoff.Get(reusableFailedReplica.Name).Seconds())
		}
		if vc.scheduler.RequireNewReplica(rs, v, hardNodeAffinity) {
			log.Debugf("A new replica will be replenished during rebuilding")
			r, err := vc.createReplica(v, e, rs, hardNodeAffinity)
			if err != nil {
				return err
			}
			rs[r.Name] = r
		}
	}
	return nil
}

func getRebuildingReplicaCount(e *longhorn.Engine) int {
	rebuilding := 0
	replicaExists := make(map[string]bool)
	// replicas are currently rebuilding
	for replica, mode := range e.Status.ReplicaModeMap {
		replicaExists[replica] = true
		if mode == types.ReplicaModeWO {
			rebuilding++
		}
	}
	// replicas are to be rebuilt
	for replica := range e.Status.CurrentReplicaAddressMap {
		if !replicaExists[replica] {
			rebuilding++
		}
	}
	return rebuilding
}

func (vc *VolumeController) getReplenishReplicasCount(v *longhorn.Volume, rs map[string]*longhorn.Replica) int {
	usableCount := 0
	for _, r := range rs {
		// The failed to schedule local replica shouldn't be counted
		if !isDataLocalityDisabled(v) && r.Spec.HealthyAt == "" && r.Spec.FailedAt == "" && r.Spec.NodeID == "" &&
			v.Status.CurrentNodeID != "" && r.Spec.HardNodeAffinity == v.Status.CurrentNodeID {
			continue
		}
		// Skip the replica has been requested eviction.
		if r.Spec.FailedAt == "" && (!r.Status.EvictionRequested) {
			usableCount++
		}
	}

	if v.Spec.NumberOfReplicas < usableCount {
		return 0
	}
	return v.Spec.NumberOfReplicas - usableCount
}

func (vc *VolumeController) hasEngineStatusSynced(e *longhorn.Engine, rs map[string]*longhorn.Replica) bool {
	connectedReplicaCount := 0
	for _, r := range rs {
		if r.Spec.FailedAt == "" && r.Spec.NodeID != "" {
			connectedReplicaCount++
		}
	}

	if len(e.Spec.ReplicaAddressMap) != connectedReplicaCount {
		return false
	}
	if len(e.Spec.ReplicaAddressMap) != len(e.Status.ReplicaModeMap) {
		return false
	}
	for rName := range e.Spec.ReplicaAddressMap {
		mode, exists := e.Status.ReplicaModeMap[rName]
		if !exists {
			return false
		}
		if mode == types.ReplicaModeERR {
			return false
		}
	}
	return true
}

func (vc *VolumeController) upgradeEngineForVolume(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) error {
	var err error

	if !vc.isVolumeUpgrading(v) {
		// it must be a rollback
		if e != nil && e.Spec.EngineImage != v.Spec.EngineImage {
			e.Spec.EngineImage = v.Spec.EngineImage
			e.Spec.UpgradedReplicaAddressMap = map[string]string{}
		}
		return nil
	}

	// If volume is detached accidentally during the live upgrade,
	// the live upgrade info and the inactive replicas are meaningless.
	if v.Status.State == types.VolumeStateDetached {
		if e.Spec.EngineImage != v.Spec.EngineImage {
			e.Spec.EngineImage = v.Spec.EngineImage
			e.Spec.UpgradedReplicaAddressMap = map[string]string{}
		}
		for _, r := range rs {
			if r.Spec.EngineImage != v.Spec.EngineImage {
				r.Spec.EngineImage = v.Spec.EngineImage
				rs[r.Name] = r
			}
			if !r.Spec.Active {
				if err := vc.deleteReplica(r, rs); err != nil {
					return err
				}
			}
		}
		// TODO current replicas should be calculated by checking if there is
		// any other image exists except for the v.Spec.EngineImage
		v.Status.CurrentImage = v.Spec.EngineImage
		return nil
	}

	// only start live upgrade if volume is healthy
	if v.Status.State != types.VolumeStateAttached || v.Status.Robustness != types.VolumeRobustnessHealthy {
		return nil
	}

	log := getLoggerForVolume(vc.logger, v)

	volumeAndReplicaNodes := []string{v.Status.CurrentNodeID}
	for _, r := range rs {
		volumeAndReplicaNodes = append(volumeAndReplicaNodes, r.Spec.NodeID)
	}

	oldImage, err := vc.getEngineImage(v.Status.CurrentImage)
	if err != nil {
		log.WithError(err).Warnf("Cannot get engine image %v for live upgrade", v.Status.CurrentImage)
		return nil
	}

	if isReady, err := vc.ds.CheckEngineImageReadiness(oldImage.Spec.Image, volumeAndReplicaNodes...); !isReady {
		log.WithError(err).Warnf("Engine live upgrade from %v, but the image wasn't ready", oldImage.Spec.Image)
		return nil
	}
	newImage, err := vc.getEngineImage(v.Spec.EngineImage)
	if err != nil {
		log.WithError(err).Warnf("Cannot get engine image %v for live upgrade", v.Spec.EngineImage)
		return nil
	}
	if isReady, err := vc.ds.CheckEngineImageReadiness(newImage.Spec.Image, volumeAndReplicaNodes...); !isReady {
		log.WithError(err).Warnf("Engine live upgrade to %v, but the image wasn't ready", newImage.Spec.Image)
		return nil
	}

	if oldImage.Status.GitCommit == newImage.Status.GitCommit {
		log.Warnf("Engine image %v and %v are identical, delay upgrade until detach for volume", oldImage.Spec.Image, newImage.Spec.Image)
		return nil
	}

	if oldImage.Status.ControllerAPIVersion > newImage.Status.ControllerAPIVersion ||
		oldImage.Status.ControllerAPIVersion < newImage.Status.ControllerAPIMinVersion {
		log.Warnf("Unable to live upgrade from %v to %v: the old controller version %v "+
			"is not compatible with the new controller version %v and the new controller minimal version %v",
			oldImage.Spec.Image, newImage.Spec.Image,
			oldImage.Status.ControllerAPIVersion, newImage.Status.ControllerAPIVersion, newImage.Status.ControllerAPIMinVersion)
		return nil
	}

	unknownReplicas := map[string]*longhorn.Replica{}
	dataPathToOldRunningReplica := map[string]*longhorn.Replica{}
	dataPathToNewReplica := map[string]*longhorn.Replica{}
	for _, r := range rs {
		dataPath := types.GetReplicaDataPath(r.Spec.DiskPath, r.Spec.DataDirectoryName)
		if r.Spec.EngineImage == v.Status.CurrentImage && r.Status.CurrentState == types.InstanceStateRunning {
			if mode, exists := e.Status.ReplicaModeMap[r.Name]; exists && mode == types.ReplicaModeRW {
				dataPathToOldRunningReplica[dataPath] = r
			}
		} else if r.Spec.EngineImage == v.Spec.EngineImage {
			dataPathToNewReplica[dataPath] = r
		} else {
			log.Warnf("Found unknown replica with image %v for live upgrade", r.Spec.EngineImage)
			unknownReplicas[r.Name] = r
		}
	}

	if err := vc.createAndStartMatchingReplicas(v, rs, dataPathToOldRunningReplica, dataPathToNewReplica, func(r *longhorn.Replica, engineImage string) {
		r.Spec.EngineImage = engineImage
	}, v.Spec.EngineImage); err != nil {
		return err
	}

	if e.Spec.EngineImage != v.Spec.EngineImage {
		replicaAddressMap := map[string]string{}
		for _, r := range dataPathToNewReplica {
			// wait for all potentially healthy replicas become running
			if r.Status.CurrentState != types.InstanceStateRunning {
				return nil
			}
			if r.Status.IP == "" {
				log.WithField("replica", r.Name).Error("BUG: replica is running but IP is empty")
				continue
			}
			if r.Status.Port == 0 {
				log.WithField("replica", r.Name).Error("BUG: replica is running but port is 0")
				continue
			}
			replicaAddressMap[r.Name] = imutil.GetURL(r.Status.IP, r.Status.Port)
		}
		// Only upgrade e.Spec.EngineImage if there are enough new upgraded replica.
		// This prevent the deadlock in the case that an upgrade from engine image
		// is followed immediately by an other upgrade.
		// More specifically, after the 1st upgrade, e.Status.ReplicaModeMap empty.
		// Therefore, dataPathToOldRunningReplica, dataPathToOldRunningReplica, and replicaAddressMap are also empty.
		// Now, if we set e.Spec.UpgradedReplicaAddressMap to an empty map in the second upgrade,
		// the second engine upgrade will be blocked since len(e.Spec.UpgradedReplicaAddressMap) == 0.
		// On the other hand, the engine controller blocks the engine's status from being refreshed
		// and keep the e.Status.ReplicaModeMap to be empty map. The system enter a deadlock for the volume.
		if len(replicaAddressMap) == v.Spec.NumberOfReplicas {
			e.Spec.UpgradedReplicaAddressMap = replicaAddressMap
			e.Spec.EngineImage = v.Spec.EngineImage
		}
	}
	if e.Status.CurrentImage != v.Spec.EngineImage ||
		e.Status.CurrentState != types.InstanceStateRunning {
		return nil
	}

	if err := vc.switchActiveReplicas(rs, func(r *longhorn.Replica, engineImage string) bool {
		return r.Spec.EngineImage == engineImage
	}, v.Spec.EngineImage); err != nil {
		return err
	}

	e.Spec.ReplicaAddressMap = e.Spec.UpgradedReplicaAddressMap
	e.Spec.UpgradedReplicaAddressMap = map[string]string{}
	// cleanupCorruptedOrStaleReplicas() will take care of old replicas
	log.Infof("Engine %v has been upgraded from %v to %v", e.Name, v.Status.CurrentImage, v.Spec.EngineImage)
	v.Status.CurrentImage = v.Spec.EngineImage

	return nil
}

func (vc *VolumeController) updateRequestedBackupForVolumeRestore(v *longhorn.Volume, e *longhorn.Engine) (err error) {
	if e == nil {
		return nil
	}

	if v.Spec.FromBackup == "" {
		return nil
	}
	if !v.Status.RestoreRequired && !v.Status.IsStandby {
		return nil
	}

	if v.Status.LastBackup != "" && v.Status.LastBackup != e.Spec.RequestedBackupRestore {
		e.Spec.RequestedBackupRestore = v.Status.LastBackup
	}

	return nil
}

func (vc *VolumeController) checkAndInitVolumeRestore(v *longhorn.Volume) error {
	log := getLoggerForVolume(vc.logger, v)

	if v.Spec.FromBackup == "" || v.Status.RestoreInitiated {
		return nil
	}

	backupTarget, err := manager.GenerateBackupTarget(vc.ds)
	if err != nil {
		return err
	}
	backup, err := backupTarget.GetBackup(v.Spec.FromBackup)
	if err != nil {
		return fmt.Errorf("cannot get backup %v: %v", v.Spec.FromBackup, err)
	}

	size, err := util.ConvertSize(backup.Size)
	if err != nil {
		return fmt.Errorf("cannot get the size of backup %v: %v", v.Spec.FromBackup, err)
	}
	v.Status.ActualSize = size

	// If KubernetesStatus is set on Backup, restore it.
	kubeStatus := &types.KubernetesStatus{}
	if statusJSON, ok := backup.Labels[types.KubernetesStatusLabel]; ok {
		if err := json.Unmarshal([]byte(statusJSON), kubeStatus); err != nil {
			log.WithError(err).Warnf("Ignore KubernetesStatus JSON for backup %v", backup.Name)
		} else {
			// We were able to unmarshal KubernetesStatus. Set the Ref fields.
			if kubeStatus.PVCName != "" && kubeStatus.LastPVCRefAt == "" {
				kubeStatus.LastPVCRefAt = backup.SnapshotCreated
			}
			if len(kubeStatus.WorkloadsStatus) != 0 && kubeStatus.LastPodRefAt == "" {
				kubeStatus.LastPodRefAt = backup.SnapshotCreated
			}

			// Do not restore the PersistentVolume fields.
			kubeStatus.PVName = ""
			kubeStatus.PVStatus = ""
		}
	}
	v.Status.KubernetesStatus = *kubeStatus

	if v.Spec.Standby {
		v.Status.IsStandby = true
	}

	v.Status.RestoreRequired = true
	v.Status.RestoreInitiated = true

	return nil
}

func (vc *VolumeController) reconcileVolumeSize(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) error {
	log := getLoggerForVolume(vc.logger, v)

	if e == nil || rs == nil {
		return nil
	}
	if e.Spec.VolumeSize == v.Spec.Size {
		return nil
	}

	// TODO: JM - this should probably also update the actual size

	// TODO: JM - refactor this, for example if replica Size update fails but engine size update passes
	// 	now they are out of sync, we should just always set Spec.VolumeSize = v.Spec.Size
	//  since if it's the same value no datastore update is necessary

	// The expansion is canceled or hasn't been started
	if e.Status.CurrentSize == v.Spec.Size {
		v.Status.ExpansionRequired = false
		vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonCanceledExpansion,
			"Canceled expanding the volume %v, will automatically detach it", v.Name)
	} else {
		log.Infof("Start volume expand from size %v to size %v", e.Spec.VolumeSize, v.Spec.Size)
		v.Status.ExpansionRequired = true
	}

	e.Spec.VolumeSize = v.Spec.Size
	for _, r := range rs {
		r.Spec.VolumeSize = v.Spec.Size
	}

	return nil
}

func (vc *VolumeController) checkForAutoAttachment(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica, scheduled bool) error {
	if v.Spec.NodeID != "" || v.Status.CurrentNodeID != "" {
		return nil
	}
	if !(v.Status.State == "" || v.Status.State == types.VolumeStateDetached) {
		return nil
	}
	// Do not intervene the auto reattachment workflow during the engine crashing and volume recovery.
	if v.Status.PendingNodeID != "" {
		return nil
	}
	// It's meaningless to do auto attachment if the volume scheduling fails
	if !scheduled {
		return nil
	}

	// Do auto attachment for:
	//   1. restoring/DR volumes.
	//   2. offline expansion.
	//   3. Eviction requested on this volume.
	if v.Status.RestoreRequired || v.Status.IsStandby ||
		v.Status.ExpansionRequired || vc.hasReplicaEvictionRequested(rs) {
		// Should use vc.controllerID or v.Status.OwnerID as CurrentNodeID,
		// otherwise they may be not equal
		v.Status.CurrentNodeID = v.Status.OwnerID
	}

	return nil
}

func (vc *VolumeController) checkForAutoDetachment(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) error {
	log := getLoggerForVolume(vc.logger, v)

	if v.Spec.NodeID != "" || v.Status.CurrentNodeID == "" || e == nil {
		return nil
	}

	if v.Status.ExpansionRequired {
		return nil
	}

	// Don't do auto-detachment if the eviction is going on.
	if vc.hasReplicaEvictionRequested(rs) {
		return nil
	}

	// Do auto-detachment for non-restore/DR volumes.
	if !v.Status.RestoreRequired && !v.Status.IsStandby {
		v.Status.CurrentNodeID = ""
		return nil
	}

	// Do auto-detachment for restore/DR volumes.
	if v.Status.CurrentNodeID != v.Status.OwnerID {
		log.Info("Found the restore/DR volume node is down, will detach it first then re-attach it to restart the restoring")
		v.Status.CurrentNodeID = ""
		return nil
	}
	// Can automatically detach/activate the restore/DR volume on the running node if the following conditions are satisfied:
	// 1) The restored backup is up-to-date;
	// 2) The volume is no longer a DR volume;
	// 3) The restore/DR volume is
	//   3.1) using the old engine image. And it's still running.
	//	 3.2) or using the latest engine image without purging snapshots. And
	//	   3.2.1) it's state `Healthy`;
	//	   3.2.2) or it's state `Degraded` with all the scheduled replica included in the engine
	cliAPIVersion, err := vc.ds.GetEngineImageCLIAPIVersion(v.Status.CurrentImage)
	if err != nil {
		return err
	}
	isPurging := false
	for _, status := range e.Status.PurgeStatus {
		if status.IsPurging {
			isPurging = true
			break
		}
	}
	if !(e.Spec.RequestedBackupRestore != "" && e.Spec.RequestedBackupRestore == e.Status.LastRestoredBackup &&
		!v.Spec.Standby) {
		return nil
	}
	allScheduledReplicasIncluded := true
	for _, r := range rs {
		// skip unscheduled replicas
		if r.Spec.NodeID == "" {
			continue
		}
		if mode := e.Status.ReplicaModeMap[r.Name]; mode != types.ReplicaModeRW {
			allScheduledReplicasIncluded = false
			break
		}
	}
	if (cliAPIVersion >= engineapi.CLIVersionFour && !isPurging && (v.Status.Robustness == types.VolumeRobustnessHealthy || v.Status.Robustness == types.VolumeRobustnessDegraded) && allScheduledReplicasIncluded) ||
		(cliAPIVersion < engineapi.CLIVersionFour && (v.Status.Robustness == types.VolumeRobustnessHealthy || v.Status.Robustness == types.VolumeRobustnessDegraded)) {
		log.Info("Prepare to do auto detachment for restore/DR volume")
		v.Status.CurrentNodeID = ""
		v.Status.IsStandby = false
		v.Status.RestoreRequired = false
	}

	return nil
}

func (vc *VolumeController) getInfoFromBackupURL(v *longhorn.Volume) (string, string, error) {
	if v.Spec.FromBackup == "" {
		return "", "", nil
	}

	backupVolumeName, err := backupstore.GetVolumeFromBackupURL(v.Spec.FromBackup)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to get backup volume name from backupURL %v", v.Spec.FromBackup)
	}

	backupName, err := backupstore.GetBackupFromBackupURL(v.Spec.FromBackup)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to get backup name from backupURL %v", v.Spec.FromBackup)
	}

	return backupVolumeName, backupName, nil

}

func (vc *VolumeController) createEngine(v *longhorn.Volume) (*longhorn.Engine, error) {
	log := getLoggerForVolume(vc.logger, v)

	engine := &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name:            types.GenerateEngineNameForVolume(v.Name),
			OwnerReferences: datastore.GetOwnerReferencesForVolume(v),
		},
		Spec: types.EngineSpec{
			InstanceSpec: types.InstanceSpec{
				VolumeName:  v.Name,
				VolumeSize:  v.Spec.Size,
				EngineImage: v.Status.CurrentImage,
				DesireState: types.InstanceStateStopped,
			},
			Frontend:                  v.Spec.Frontend,
			ReplicaAddressMap:         map[string]string{},
			UpgradedReplicaAddressMap: map[string]string{},
			RevisionCounterDisabled:   v.Spec.RevisionCounterDisabled,
		},
	}

	if v.Spec.FromBackup != "" {
		backupVolumeName, backupName, err := vc.getInfoFromBackupURL(v)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get backup volume when creating engine object of restored volume %v", v.Name)
		}
		engine.Spec.BackupVolume = backupVolumeName
		engine.Spec.RequestedBackupRestore = backupName

		log.Debugf("Created engine %v for restored volume, BackupVolume is %v, RequestedBackupRestore is %v",
			engine.Name, engine.Spec.BackupVolume, engine.Spec.RequestedBackupRestore)
	}

	return vc.ds.CreateEngine(engine)
}

func (vc *VolumeController) createReplica(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica, hardNodeAffinity string) (*longhorn.Replica, error) {
	replica := &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:            types.GenerateReplicaNameForVolume(v.Name),
			OwnerReferences: datastore.GetOwnerReferencesForVolume(v),
		},
		Spec: types.ReplicaSpec{
			InstanceSpec: types.InstanceSpec{
				VolumeName:  v.Name,
				VolumeSize:  v.Spec.Size,
				EngineImage: v.Status.CurrentImage,
				DesireState: types.InstanceStateStopped,
			},
			EngineName:              e.Name,
			Active:                  true,
			BackingImage:            v.Spec.BackingImage,
			HardNodeAffinity:        hardNodeAffinity,
			RevisionCounterDisabled: v.Spec.RevisionCounterDisabled,
		},
	}

	return vc.ds.CreateReplica(replica)
}

func (vc *VolumeController) createReplicaManifest(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) *longhorn.Replica {
	replica := &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:            types.GenerateReplicaNameForVolume(v.Name),
			OwnerReferences: datastore.GetOwnerReferencesForVolume(v),
		},
		Spec: types.ReplicaSpec{
			InstanceSpec: types.InstanceSpec{
				VolumeName:  v.Name,
				VolumeSize:  v.Spec.Size,
				EngineImage: v.Status.CurrentImage,
				DesireState: types.InstanceStateStopped,
			},
			EngineName:   e.Name,
			Active:       true,
			BackingImage: v.Spec.BackingImage,
		},
	}

	return replica
}

func (vc *VolumeController) duplicateReplica(r *longhorn.Replica, v *longhorn.Volume) *longhorn.Replica {
	replica := &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:            types.GenerateReplicaNameForVolume(r.Spec.VolumeName),
			OwnerReferences: datastore.GetOwnerReferencesForVolume(v),
		},
		Spec: r.DeepCopy().Spec,
	}
	return replica
}

func (vc *VolumeController) enqueueVolume(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	vc.queue.AddRateLimited(key)
}

func (vc *VolumeController) enqueueControlleeChange(obj interface{}) {
	if deletedState, ok := obj.(*cache.DeletedFinalStateUnknown); ok {
		obj = deletedState.Obj
	}

	metaObj, err := meta.Accessor(obj)
	if err != nil {
		vc.logger.WithError(err).Warnf("BUG: cannot convert obj %v to metav1.Object", obj)
		return
	}
	ownerRefs := metaObj.GetOwnerReferences()
	for _, ref := range ownerRefs {
		namespace := metaObj.GetNamespace()
		vc.ResolveRefAndEnqueue(namespace, &ref)
		return
	}
}

func (vc *VolumeController) ResolveRefAndEnqueue(namespace string, ref *metav1.OwnerReference) {
	if ref.Kind != types.LonghornKindVolume {
		// TODO: Will stop checking this wrong reference kind after all Longhorn components having used the new kinds
		if ref.Kind != ownerKindVolume {
			return
		}
	}
	volume, err := vc.ds.GetVolume(ref.Name)
	if err != nil {
		return
	}
	if volume.UID != ref.UID {
		// The controller we found with this Name is not the same one that the
		// OwnerRef points to.
		return
	}
	vc.enqueueVolume(volume)
}

func (vc *VolumeController) createCronJob(v *longhorn.Volume, job *types.RecurringJob, suspend bool) (*batchv1beta1.CronJob, error) {
	backoffLimit := int32(CronJobBackoffLimit)
	successfulJobsHistoryLimit := int32(CronJobSuccessfulJobsHistoryLimit)
	cmd := []string{
		"longhorn-manager", "-d",
		"snapshot", v.Name,
		"--manager-url", types.GetDefaultManagerURL(),
		"--snapshot-name", job.Name,
		"--labels", types.RecurringJobLabel + "=" + job.Name,
		"--retain", strconv.Itoa(job.Retain),
	}
	for key, val := range job.Labels {
		cmd = append(cmd, "--labels", key+"="+val)
	}
	if job.Task == types.RecurringJobTypeBackup {
		cmd = append(cmd, "--backup")
	}
	tolerations, err := vc.ds.GetSettingTaintToleration()
	if err != nil {
		return nil, err
	}
	// for mounting inside container
	privilege := true
	cronJob := &batchv1beta1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:            types.GetCronJobNameForVolumeAndJob(v.Name, job.Name),
			Namespace:       vc.namespace,
			OwnerReferences: datastore.GetOwnerReferencesForVolume(v),
		},
		Spec: batchv1beta1.CronJobSpec{
			Schedule:                   job.Cron,
			ConcurrencyPolicy:          batchv1beta1.ForbidConcurrent,
			Suspend:                    &suspend,
			SuccessfulJobsHistoryLimit: &successfulJobsHistoryLimit,
			JobTemplate: batchv1beta1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					BackoffLimit: &backoffLimit,
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name: types.GetCronJobNameForVolumeAndJob(v.Name, job.Name),
						},
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:    types.GetCronJobNameForVolumeAndJob(v.Name, job.Name),
									Image:   vc.ManagerImage,
									Command: cmd,
									SecurityContext: &v1.SecurityContext{
										Privileged: &privilege,
									},
									Env: []v1.EnvVar{
										{
											Name: "POD_NAMESPACE",
											ValueFrom: &v1.EnvVarSource{
												FieldRef: &v1.ObjectFieldSelector{
													FieldPath: "metadata.namespace",
												},
											},
										},
									},
									VolumeMounts: []v1.VolumeMount{
										{
											Name:      "engine-binaries",
											MountPath: types.EngineBinaryDirectoryOnHost,
										},
									},
								},
							},
							Volumes: []v1.Volume{
								{
									Name: "engine-binaries",
									VolumeSource: v1.VolumeSource{
										HostPath: &v1.HostPathVolumeSource{
											Path: types.EngineBinaryDirectoryOnHost,
										},
									},
								},
							},
							ServiceAccountName: vc.ServiceAccount,
							RestartPolicy:      v1.RestartPolicyOnFailure,
							Tolerations:        util.GetDistinctTolerations(tolerations),
						},
					},
				},
			},
		},
	}
	return cronJob, nil
}

func (vc *VolumeController) updateRecurringJobs(v *longhorn.Volume) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to update recurring jobs for %v", v.Name)
	}()

	suspended := vc.shouldSuspendRecurringJobs(v)

	// the cronjobs are RO in the map, but not the map itself
	appliedCronJobROs, err := vc.ds.ListVolumeCronJobROs(v.Name)
	if err != nil {
		return err
	}

	currentCronJobs := make(map[string]*batchv1beta1.CronJob)
	for _, job := range v.Spec.RecurringJobs {
		cronJob, err := vc.createCronJob(v, &job, suspended)
		if err != nil {
			return err
		}
		currentCronJobs[cronJob.Name] = cronJob
	}

	for name, cronJob := range currentCronJobs {
		appliedCronJob := appliedCronJobROs[name]
		cronJobSpecB, err := json.Marshal(cronJob)
		if err != nil {
			return errors.Wrapf(err, "fail to updateRecurringJobs %v", cronJob.GetName())
		}
		cronJobSpec := string(cronJobSpecB)

		if appliedCronJob == nil {
			util.SetAnnotation(cronJob, types.GetLonghornLabelKey(LastAppliedCronJobSpecAnnotationKeySuffix), cronJobSpec)
			_, err := vc.ds.CreateVolumeCronJob(v.Name, cronJob)
			if err != nil {
				return err
			}
			continue
		}

		lastAppliedSpec, err := util.GetAnnotation(appliedCronJob, types.GetLonghornLabelKey(LastAppliedCronJobSpecAnnotationKeySuffix))
		if err != nil {
			return err
		}
		if lastAppliedSpec == cronJobSpec {
			continue
		}

		util.SetAnnotation(cronJob, types.GetLonghornLabelKey(LastAppliedCronJobSpecAnnotationKeySuffix), cronJobSpec)
		if _, err := vc.ds.UpdateVolumeCronJob(v.Name, cronJob); err != nil {
			return err
		}
	}

	for name, job := range appliedCronJobROs {
		if currentCronJobs[name] == nil {
			if err := vc.ds.DeleteCronJob(name); err != nil {
				return err
			}
		}

		// TODO: Will Remove this reference kind correcting after all Longhorn components having used the new kinds
		if len(job.OwnerReferences) < 1 || job.OwnerReferences[0].Kind != types.LonghornKindVolume {
			job.OwnerReferences = datastore.GetOwnerReferencesForVolume(v)
			job, err = vc.kubeClient.BatchV1beta1().CronJobs(vc.namespace).Update(job)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (vc *VolumeController) shouldSuspendRecurringJobs(v *longhorn.Volume) bool {
	allowRecurringJobWhileVolumeDetached, err := vc.ds.GetSettingAsBool(types.SettingNameAllowRecurringJobWhileVolumeDetached)
	if err != nil {
		vc.logger.WithError(err).Warn("error getting allow-recurring-backup-while-volume-detached setting")
	}

	if v.Status.State == types.VolumeStateAttached || (v.Status.State == types.VolumeStateDetached && allowRecurringJobWhileVolumeDetached) {
		return false
	}
	return true
}

func (vc *VolumeController) isVolumeUpgrading(v *longhorn.Volume) bool {
	return v.Status.CurrentImage != v.Spec.EngineImage
}

func (vc *VolumeController) isVolumeMigrating(v *longhorn.Volume) bool {
	return v.Spec.MigrationNodeID != ""
}

func (vc *VolumeController) getEngineImage(image string) (*longhorn.EngineImage, error) {
	name := types.GetEngineImageChecksumName(image)
	img, err := vc.ds.GetEngineImage(name)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get engine image %v", image)
	}
	return img, nil
}

func (vc *VolumeController) getCurrentEngineAndExtras(v *longhorn.Volume, es map[string]*longhorn.Engine) (currentEngine *longhorn.Engine, otherEngines []*longhorn.Engine, err error) {
	for _, e := range es {
		if e.Spec.NodeID == v.Status.CurrentNodeID &&
			e.Spec.DesireState == types.InstanceStateRunning &&
			e.Status.CurrentState == types.InstanceStateRunning {
			currentEngine = e
		} else {
			otherEngines = append(otherEngines, e)
		}
	}
	if currentEngine == nil {
		return nil, nil, fmt.Errorf("volume %v is not attached", v.Name)
	}
	return
}

func (vc *VolumeController) getCurrentEngineAndCleanupOthers(v *longhorn.Volume, es map[string]*longhorn.Engine) (current *longhorn.Engine, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to clean up the extra engines for %v", v.Name)
	}()
	current, extras, err := vc.getCurrentEngineAndExtras(v, es)
	if err != nil {
		return nil, err
	}

	for _, extra := range extras {
		if extra.DeletionTimestamp == nil {
			if err := vc.deleteEngine(extra, es); err != nil {
				return nil, err
			}
		}
	}
	return current, nil
}

func (vc *VolumeController) createAndStartMatchingReplicas(v *longhorn.Volume,
	rs, pathToOldRunnngRs, pathToNewRs map[string]*longhorn.Replica,
	fixupFunc func(r *longhorn.Replica, obj string), obj string) error {
	log := getLoggerForVolume(vc.logger, v)

	if len(pathToNewRs) == v.Spec.NumberOfReplicas {
		return nil
	}

	if len(pathToOldRunnngRs) != v.Spec.NumberOfReplicas {
		log.Debugf("Volume old healthy replica count %v doesn't match the desired replica count %v",
			len(pathToOldRunnngRs), v.Spec.NumberOfReplicas)
		return nil
	}

	for path, r := range pathToOldRunnngRs {
		if pathToNewRs[path] != nil {
			continue
		}
		clone := vc.duplicateReplica(r, v)
		clone.Spec.DesireState = types.InstanceStateRunning
		clone.Spec.Active = false
		fixupFunc(clone, obj)
		newReplica, err := vc.ds.CreateReplica(clone)
		if err != nil {
			return errors.Wrapf(err, "cannot create matching replica %v for volume %v", clone.Name, v.Name)
		}
		pathToNewRs[path] = newReplica
		rs[newReplica.Name] = newReplica
	}
	return nil
}

func (vc *VolumeController) switchActiveReplicas(rs map[string]*longhorn.Replica,
	activeCondFunc func(r *longhorn.Replica, obj string) bool, obj string) error {

	// Deletion of an active replica will trigger the cleanup process to
	// delete the volume data on the disk.
	// Set `active` at last to prevent data loss
	for _, r := range rs {
		r.Spec.Active = activeCondFunc(r, obj)
	}
	return nil
}

func (vc *VolumeController) processMigration(v *longhorn.Volume, es map[string]*longhorn.Engine, rs map[string]*longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to process migration for %v", v.Name)
	}()

	if !v.Spec.Migratable || v.Spec.AccessMode != types.AccessModeReadWriteMany {
		return nil
	}

	// only process if volume is attached
	if v.Spec.NodeID == "" || len(es) == 0 {
		return nil
	}

	// cannot process migrate when upgrading
	if vc.isVolumeUpgrading(v) {
		return nil
	}

	// the only time there should be more then 1 engines is when we are migrating or upgrading
	// if there are more then 1 and we no longer have a migration id set we can cleanup the extra engine
	if !vc.isVolumeMigrating(v) {
		if len(es) < 2 {
			return nil
		}

		// in the case of a confirmation we need to switch the v.Status.CurrentNodeID to v.Spec.NodeID
		// so that currentEngine becomes the migration engine
		if v.Status.CurrentNodeID != v.Spec.NodeID {
			vc.logger.Infof("volume migration complete switching current node id from %v to %v", v.Status.CurrentNodeID, v.Spec.NodeID)
			v.Status.CurrentNodeID = v.Spec.NodeID
		}

		// current engine is based on the v.Status.CurrentNodeID
		// so in the case of a rollback all we have to do is cleanup the migration engine
		currentEngine, err := vc.getCurrentEngineAndCleanupOthers(v, es)
		if err != nil {
			return err
		}

		// cleanupCorruptedOrStaleReplicas() will take care of old replicas
		if err := vc.switchActiveReplicas(rs, func(r *longhorn.Replica, engineName string) bool {
			return r.Spec.EngineName == engineName
		}, currentEngine.Name); err != nil {
			return err
		}

		return nil
	}

	// volume is migrating
	currentEngine, extras, err := vc.getCurrentEngineAndExtras(v, es)
	if err != nil {
		return err
	}

	var activeEngines int
	var migrationEngine *longhorn.Engine
	for _, extra := range extras {
		if extra.DeletionTimestamp != nil {
			continue
		}

		// a valid migration engine is either a newly created engine that hasn't been assigned a node yet
		// or an existing engine that is running on the migration node, any additional extra engines would be unexpected.
		activeEngines++
		isValidMigrationEngine := extra.Spec.NodeID == "" || extra.Spec.NodeID == v.Spec.MigrationNodeID
		if isValidMigrationEngine {
			migrationEngine = extra
		}
	}

	// verify that we are in a valid state for migration
	// we only consider active engines as candidates for the migration engine
	unexpectedEngineCount := activeEngines > 1
	invalidMigrationEngine := activeEngines > 0 && migrationEngine == nil
	if unexpectedEngineCount || invalidMigrationEngine {
		_, _ = vc.getCurrentEngineAndCleanupOthers(v, es)
		return fmt.Errorf("unexpected state for migration, current engine count %v has invalid migration engine %v",
			len(es), invalidMigrationEngine)
	}

	if migrationEngine == nil {
		migrationEngine, err = vc.createEngine(v)
		if err != nil {
			return err
		}
		es[migrationEngine.Name] = migrationEngine
	}

	currentReplicas := map[string]*longhorn.Replica{}
	migrationReplicas := map[string]*longhorn.Replica{}
	unknownReplicas := map[string]*longhorn.Replica{}
	for _, r := range rs {
		dataPath := types.GetReplicaDataPath(r.Spec.DiskPath, r.Spec.DataDirectoryName)
		if r.Spec.FailedAt != "" {
			continue
		}
		if r.Spec.EngineName == currentEngine.Name {
			currentReplicas[dataPath] = r
		} else if r.Spec.EngineName == migrationEngine.Name {
			migrationReplicas[dataPath] = r
		} else {
			vc.logger.Warnf("During migration found unknown replica with engine %v", r.Spec.EngineName)
			unknownReplicas[dataPath] = r
		}
	}

	if err := vc.createAndStartMatchingReplicas(v, rs, currentReplicas, migrationReplicas, func(r *longhorn.Replica, engineName string) {
		r.Spec.EngineName = engineName
	}, migrationEngine.Name); err != nil {
		return err
	}

	if migrationEngine.Spec.DesireState != types.InstanceStateRunning {
		replicaAddressMap := map[string]string{}
		for _, r := range migrationReplicas {
			// wait for all potentially healthy replicas become running
			if r.Status.CurrentState != types.InstanceStateRunning {
				return nil
			}
			if r.Status.IP == "" {
				vc.logger.Errorf("BUG: replica %v is running but IP is empty", r.Name)
				continue
			}
			if r.Status.Port == 0 {
				vc.logger.Errorf("BUG: replica %v is running but Port is empty", r.Name)
				continue
			}
			replicaAddressMap[r.Name] = imutil.GetURL(r.Status.IP, r.Status.Port)
		}
		if migrationEngine.Spec.NodeID != "" && migrationEngine.Spec.NodeID != v.Spec.MigrationNodeID {
			return fmt.Errorf("volume %v: engine is on node %v vs volume migration on %v",
				v.Name, migrationEngine.Spec.NodeID, v.Spec.NodeID)
		}
		migrationEngine.Spec.NodeID = v.Spec.MigrationNodeID
		migrationEngine.Spec.ReplicaAddressMap = replicaAddressMap
		migrationEngine.Spec.DesireState = types.InstanceStateRunning
		es[migrationEngine.Name] = migrationEngine
	}

	if migrationEngine.Status.CurrentState != types.InstanceStateRunning {
		return nil
	}

	vc.logger.Infof("volume migration engine on node %v is ready", v.Spec.MigrationNodeID)
	return nil
}

func (vc *VolumeController) isResponsibleFor(v *longhorn.Volume, defaultEngineImage string) (bool, error) {
	var err error
	defer func() {
		err = errors.Wrap(err, "error while checking isResponsibleFor")
	}()

	readyNodesWithDefaultEI, err := vc.ds.ListReadyNodesWithEngineImage(defaultEngineImage)
	if err != nil {
		return false, err
	}

	isResponsible := isControllerResponsibleFor(vc.controllerID, vc.ds, v.Name, v.Spec.NodeID, v.Status.OwnerID)

	if len(readyNodesWithDefaultEI) == 0 {
		return isResponsible, nil
	}

	preferredOwnerEngineAvailable, err := vc.ds.CheckEngineImageReadiness(defaultEngineImage, v.Spec.NodeID)
	if err != nil {
		return false, err
	}
	currentOwnerEngineAvailable, err := vc.ds.CheckEngineImageReadiness(defaultEngineImage, v.Status.OwnerID)
	if err != nil {
		return false, err
	}
	currentNodeEngineAvailable, err := vc.ds.CheckEngineImageReadiness(defaultEngineImage, vc.controllerID)
	if err != nil {
		return false, err
	}

	return (isResponsible && currentNodeEngineAvailable) || (!preferredOwnerEngineAvailable && !currentOwnerEngineAvailable && currentNodeEngineAvailable), nil
}

func (vc *VolumeController) deleteReplica(r *longhorn.Replica, rs map[string]*longhorn.Replica) error {
	// Must call Update before removal to keep the fields up to date
	if _, err := vc.ds.UpdateReplica(r); err != nil {
		return err
	}
	if err := vc.ds.DeleteReplica(r.Name); err != nil {
		return err
	}
	delete(rs, r.Name)
	return nil
}

func (vc *VolumeController) deleteEngine(e *longhorn.Engine, es map[string]*longhorn.Engine) error {
	// Must call Update before removal to keep the fields up to date
	if _, err := vc.ds.UpdateEngine(e); err != nil {
		return err
	}
	if err := vc.ds.DeleteEngine(e.Name); err != nil {
		return err
	}
	delete(es, e.Name)
	return nil
}

// enqueueVolumesForShareManager enqueues all volumes that are currently claimed by this share manager
func (vc *VolumeController) enqueueVolumesForShareManager(obj interface{}) {
	sm, isShareManager := obj.(*longhorn.ShareManager)
	if !isShareManager {
		deletedState, ok := obj.(*cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to requeue the claimed volumes
		sm, ok = deletedState.Obj.(*longhorn.ShareManager)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained non ShareManager object: %#v", deletedState.Obj))
			return
		}
	}

	// we can queue the key directly since a share manager only manages a single volume from it's own namespace
	// and there is no need for us to retrieve the whole object, since we already know the volume name
	key := sm.Namespace + "/" + sm.Name
	vc.queue.AddRateLimited(key)
}

// ReconcileShareManagerState is responsible for syncing the state of shared volumes with their share manager
func (vc *VolumeController) ReconcileShareManagerState(volume *longhorn.Volume) error {
	log := getLoggerForVolume(vc.logger, volume)
	sm, err := vc.ds.GetShareManager(volume.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		log.WithError(err).Error("Failed to get share manager for volume")
		return err
	}

	if volume.Spec.AccessMode != types.AccessModeReadWriteMany || volume.Spec.Migratable {
		if sm != nil {
			log.Info("Removing share manager for non shared volume")
			if err := vc.ds.DeleteShareManager(volume.Name); err != nil && !datastore.ErrorIsNotFound(err) {
				return err
			}
		}
		return nil
	}

	image, err := vc.ds.GetSettingValueExisted(types.SettingNameDefaultShareManagerImage)
	if err != nil {
		return err
	}

	// no ShareManager create a new one
	if sm == nil {
		sm, err = vc.createShareManagerForVolume(volume, image)
		if err != nil {
			log.WithError(err).Errorf("Failed to create share manager %v", volume.Name)
			return err
		}
	}

	if image != "" && sm.Spec.Image != image {
		sm.Spec.Image = image
		sm.ObjectMeta.Labels = types.GetShareManagerLabels(volume.Name, image)
		if sm, err = vc.ds.UpdateShareManager(sm); err != nil {
			return err
		}

		log.Infof("Updated image for share manager from %v to %v", sm.Spec.Image, image)
	}

	// kill the workload pods, when the share manager goes into error state
	// easiest approach is to set the RemountRequestedAt variable,
	// since that is already responsible for killing the workload pods
	if sm.Status.State == types.ShareManagerStateError || sm.Status.State == types.ShareManagerStateUnknown {
		volume.Status.RemountRequestedAt = vc.nowHandler()
		msg := fmt.Sprintf("Volume %v requested remount at %v", volume.Name, volume.Status.RemountRequestedAt)
		vc.eventRecorder.Eventf(volume, v1.EventTypeNormal, EventReasonRemount, msg)
	}

	// sync the share state and endpoint
	volume.Status.ShareState = sm.Status.State
	volume.Status.ShareEndpoint = sm.Status.Endpoint
	return nil
}

func (vc *VolumeController) createShareManagerForVolume(volume *longhorn.Volume, image string) (*longhorn.ShareManager, error) {
	sm := &longhorn.ShareManager{
		ObjectMeta: metav1.ObjectMeta{
			Name:            volume.Name,
			Namespace:       vc.namespace,
			Labels:          types.GetShareManagerLabels(volume.Name, image),
			OwnerReferences: datastore.GetOwnerReferencesForVolume(volume),
		},
		Spec: types.ShareManagerSpec{
			Image: image,
		},
	}

	return vc.ds.CreateShareManager(sm)
}

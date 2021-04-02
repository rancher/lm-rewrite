package controller

import (
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

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

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/scheduler"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
)

type VolumeExpansionController struct {
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

	informersSynced []cache.InformerSynced

	scheduler *scheduler.ReplicaScheduler

	backoff *flowcontrol.Backoff

	// for unit test
	nowHandler func() string
}

func NewVolumeExpansionController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	volumeInformer lhinformers.VolumeInformer,
	engineInformer lhinformers.EngineInformer,
	replicaInformer lhinformers.ReplicaInformer,
	kubeClient clientset.Interface,
	namespace, controllerID, serviceAccount string,
	managerImage string) *VolumeExpansionController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	c := &VolumeExpansionController{
		baseController: newBaseController("longhorn-volume-expansion", logger),

		ds:             ds,
		namespace:      namespace,
		controllerID:   controllerID,
		ManagerImage:   managerImage,
		ServiceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-volume-expansion-controller"}),

		// when adding a new informer ensure to also add
		// the HasSynced check function to this list
		informersSynced: []cache.InformerSynced{
			volumeInformer.Informer().HasSynced,
			engineInformer.Informer().HasSynced,
			replicaInformer.Informer().HasSynced,
		},

		backoff: flowcontrol.NewBackOff(time.Minute, time.Minute*3),

		nowHandler: util.Now,
	}

	c.scheduler = scheduler.NewReplicaScheduler(ds)

	volumeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueVolume,
		UpdateFunc: func(old, cur interface{}) { c.enqueueVolume(cur) },
		DeleteFunc: c.enqueueVolume,
	})
	engineInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueControlleeChange,
		UpdateFunc: func(old, cur interface{}) { c.enqueueControlleeChange(cur) },
		DeleteFunc: c.enqueueControlleeChange,
	})
	replicaInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueControlleeChange,
		UpdateFunc: func(old, cur interface{}) { c.enqueueControlleeChange(cur) },
		DeleteFunc: c.enqueueControlleeChange,
	})

	return c
}

func (c *VolumeExpansionController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Infof("Start Longhorn volume expansion controller")
	defer c.logger.Infof("Shutting down Longhorn volume expansion controller")

	if !cache.WaitForNamedCacheSync("volume expansion controller", stopCh, c.informersSynced...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (c *VolumeExpansionController) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *VolumeExpansionController) processNextWorkItem() bool {
	key, quit := c.queue.Get()

	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncVolume(key.(string))
	c.handleErr(err, key)

	return true
}

func (c *VolumeExpansionController) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	if c.queue.NumRequeues(key) < maxRetries {
		c.logger.WithError(err).Warnf("Error syncing Longhorn volume %v", key)
		c.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	c.logger.WithError(err).Warnf("Dropping Longhorn volume %v out of the queue", key)
	c.queue.Forget(key)
}

func (c *VolumeExpansionController) syncVolume(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil || namespace != c.namespace {
		return err
	}

	volume, err := c.ds.GetVolume(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return err
	}

	log := getLoggerForVolume(c.logger, volume)

	// we only deal with salvage operations other responsibilities lie with volume controller
	shouldHandleVolume := volume.Status.OwnerID == c.controllerID && volume.DeletionTimestamp == nil
	if !shouldHandleVolume {
		return
	}

	engines, err := c.ds.ListVolumeEngines(volume.Name)
	if err != nil {
		return err
	}
	replicas, err := c.ds.ListVolumeReplicas(volume.Name)
	if err != nil {
		return err
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
				if _, err := c.ds.UpdateReplica(r); err != nil {
					lastErr = err
				}
			}
		}
		// stop updating if replicas weren't fully updated
		if lastErr == nil {
			for k, e := range engines {
				if existingEngines[k] == nil ||
					!reflect.DeepEqual(existingEngines[k].Spec, e.Spec) {
					if _, err := c.ds.UpdateEngine(e); err != nil {
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
				_, err = c.ds.UpdateVolumeStatus(volume)
			}
		}
		// requeue if it's conflict
		if apierrors.IsConflict(errors.Cause(err)) || apierrors.IsConflict(errors.Cause(lastErr)) {
			log.Debugf("Requeue volume due to error %v or %v", err, lastErr)
			c.enqueueVolume(volume)
			err = nil
		}
	}()

	return c.ReconcileVolumeState(volume, engines, replicas)
}

func (c *VolumeExpansionController) getCurrentEngine(v *longhorn.Volume, es map[string]*longhorn.Engine) (*longhorn.Engine, error) {
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

// ReconcileVolumeState handles initiation and completion of salvage operations for a volume
func (c *VolumeExpansionController) ReconcileVolumeState(v *longhorn.Volume, es map[string]*longhorn.Engine, rs map[string]*longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to reconcile volume state for %v", v.Name)
	}()

	// no need to consider expansion operations in the case where there is no engine or replicas
	// this is a new volume, where the volume controller will create an engine first
	e, err := c.getCurrentEngine(v, es)
	if err != nil || e == nil || len(rs) == 0 {
		return err
	}

	// expansion controller is only responsible for doing volume expansion in maintenance mode
	// TODO: JM implement expansion, put volume in maintenance mode first, process expansion, unset maintenance mode + expansionRequired spec.
	return nil
}

func (c *VolumeExpansionController) enqueueVolume(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	c.queue.AddRateLimited(key)
}

func (c *VolumeExpansionController) enqueueControlleeChange(obj interface{}) {
	if deletedState, ok := obj.(*cache.DeletedFinalStateUnknown); ok {
		obj = deletedState.Obj
	}

	metaObj, err := meta.Accessor(obj)
	if err != nil {
		c.logger.WithError(err).Warnf("BUG: cannot convert obj %v to metav1.Object", obj)
		return
	}
	ownerRefs := metaObj.GetOwnerReferences()
	for _, ref := range ownerRefs {
		namespace := metaObj.GetNamespace()
		c.ResolveRefAndEnqueue(namespace, &ref)
		return
	}
}

func (c *VolumeExpansionController) ResolveRefAndEnqueue(namespace string, ref *metav1.OwnerReference) {
	if ref.Kind != types.LonghornKindVolume {
		// TODO: Will stop checking this wrong reference kind after all Longhorn components having used the new kinds
		if ref.Kind != ownerKindVolume {
			return
		}
	}
	volume, err := c.ds.GetVolume(ref.Name)
	if err != nil {
		return
	}
	if volume.UID != ref.UID {
		// The controller we found with this Name is not the same one that the
		// OwnerRef points to.
		return
	}
	c.enqueueVolume(volume)
}

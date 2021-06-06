package controller

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	bimapi "github.com/longhorn/backing-image-manager/api"
	bimtypes "github.com/longhorn/backing-image-manager/pkg/types"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
)

const (
	BackingImageManagerPodContainerName = "backing-image-manager"
)

type BackingImageManagerController struct {
	*baseController

	namespace      string
	controllerID   string
	serviceAccount string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	bimStoreSynced cache.InformerSynced
	biStoreSynced  cache.InformerSynced
	nStoreSynced   cache.InformerSynced
	pStoreSynced   cache.InformerSynced

	lock       *sync.RWMutex
	monitorMap map[string]chan struct{}

	versionUpdater func(*longhorn.BackingImageManager) error
}

type BackingImageManagerMonitor struct {
	Name         string
	controllerID string

	ds                 *datastore.DataStore
	log                logrus.FieldLogger
	lock               *sync.Mutex
	updateNotification bool
	// Receive stop signals from main sync loop
	stopCh chan struct{}
	// The monitor should voluntarily exit if the streaming doesn't work,
	// or the ownership of the related manager is taken over by others.
	monitorVoluntaryStopCh chan struct{}
	done                   bool

	client *engineapi.BackingImageManagerClient
	stream *bimapi.BackingImageStream
}

func updateBackingImageManagerVersion(bim *longhorn.BackingImageManager) error {
	cli, err := engineapi.NewBackingImageManagerClient(bim)
	if err != nil {
		return err
	}
	apiMinVersion, apiVersion, err := cli.VersionGet()
	if err != nil {
		return err
	}
	bim.Status.APIMinVersion = apiMinVersion
	bim.Status.APIVersion = apiVersion
	return nil
}

func NewBackingImageManagerController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	bimInformer lhinformers.BackingImageManagerInformer,
	biInformer lhinformers.BackingImageInformer,
	nodeInformer lhinformers.NodeInformer,
	pInformer coreinformers.PodInformer,
	kubeClient clientset.Interface,
	namespace, controllerID, serviceAccount string) *BackingImageManagerController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	c := &BackingImageManagerController{
		baseController: newBaseController("longhorn-backing-image-manager", logger),

		namespace:      namespace,
		controllerID:   controllerID,
		serviceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-backing-image-manager-controller"}),

		ds: ds,

		bimStoreSynced: bimInformer.Informer().HasSynced,
		biStoreSynced:  biInformer.Informer().HasSynced,
		nStoreSynced:   nodeInformer.Informer().HasSynced,
		pStoreSynced:   pInformer.Informer().HasSynced,

		lock:       &sync.RWMutex{},
		monitorMap: map[string]chan struct{}{},

		versionUpdater: updateBackingImageManagerVersion,
	}

	bimInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueBackingImageManager,
		UpdateFunc: func(old, cur interface{}) { c.enqueueBackingImageManager(cur) },
		DeleteFunc: c.enqueueBackingImageManager,
	})

	biInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueForBackingImage,
		UpdateFunc: func(old, cur interface{}) { c.enqueueForBackingImage(cur) },
		DeleteFunc: c.enqueueForBackingImage,
	})

	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, cur interface{}) { c.enqueueForLonghornNode(cur) },
		DeleteFunc: c.enqueueForLonghornNode,
	})

	pInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: isBackingImageManagerPod,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    c.enqueueForBackingImageManagerPod,
			UpdateFunc: func(old, cur interface{}) { c.enqueueForBackingImageManagerPod(cur) },
			DeleteFunc: c.enqueueForBackingImageManagerPod,
		},
	})

	return c
}

func (c *BackingImageManagerController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	logrus.Infof("Starting Longhorn backing image manager controller")
	defer logrus.Infof("Shutting down Longhorn backing image manager controller")

	if !cache.WaitForNamedCacheSync("longhorn backing image manager", stopCh, c.bimStoreSynced, c.biStoreSynced, c.nStoreSynced, c.pStoreSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (c *BackingImageManagerController) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *BackingImageManagerController) processNextWorkItem() bool {
	key, quit := c.queue.Get()

	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncBackingImageManager(key.(string))
	c.handleErr(err, key)

	return true
}

func (c *BackingImageManagerController) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	if c.queue.NumRequeues(key) < maxRetries {
		logrus.Warnf("Error syncing Longhorn backing image manager %v: %v", key, err)
		c.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logrus.Warnf("Dropping Longhorn backing image manager %v out of the queue: %v", key, err)
	c.queue.Forget(key)
}

func getLoggerForBackingImageManager(logger logrus.FieldLogger, bim *longhorn.BackingImageManager) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"backingImageManager": bim.Name,
			"nodeID":              bim.Spec.NodeID,
			"diskUUID":            bim.Spec.DiskUUID,
		},
	)
}

func (c *BackingImageManagerController) syncBackingImageManager(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "BackingImageManagerController failed to sync %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != c.namespace {
		return nil
	}

	bim, err := c.ds.GetBackingImageManager(name)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			c.logger.WithField("backingImageManager", name).WithError(err).Error("Failed to retrieve backing image manager from datastore")
			return err
		}
		c.logger.WithField("backingImageManager", name).Debug("Can't find backing image manager, may have been deleted")
		return nil
	}

	log := getLoggerForBackingImageManager(c.logger, bim)

	if !c.isResponsibleFor(bim) {
		return nil
	}
	if bim.Status.OwnerID != c.controllerID {
		bim.Status.OwnerID = c.controllerID
		bim, err = c.ds.UpdateBackingImageManagerStatus(bim)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Debugf("BackingImageManagerController on node %v picked up backing image manager", c.controllerID)
	}

	if bim.DeletionTimestamp != nil {
		if err := c.cleanupBackingImageManager(bim); err != nil {
			return err
		}
		return c.ds.RemoveFinalizerForBackingImageManager(bim)
	}

	existingBIM := bim.DeepCopy()
	defer func() {
		if err == nil && !reflect.DeepEqual(existingBIM.Status, bim.Status) {
			_, err = c.ds.UpdateBackingImageManagerStatus(bim)
		}
		if apierrors.IsConflict(errors.Cause(err)) {
			logrus.Debugf("Requeue %v due to conflict: %v", key, err)
			c.enqueueBackingImageManager(bim)
			err = nil
		}
	}()

	if bim.Status.BackingImageFileMap == nil {
		bim.Status.BackingImageFileMap = map[string]types.BackingImageFileInfo{}
	}

	node, diskName, err := c.ds.GetReadyDiskNode(bim.Spec.DiskUUID)
	if err != nil && !types.ErrorIsNotFound(err) {
		return err
	}
	noReadyDisk := node == nil
	diskMigrated := node != nil && (node.Name != bim.Spec.NodeID || node.Spec.Disks[diskName].Path != bim.Spec.DiskPath)
	if noReadyDisk || diskMigrated {
		if bim.Status.CurrentState != types.BackingImageManagerStateUnknown {
			if noReadyDisk {
				log.Warnf("Node or disk is not ready, will update state from %v to %v then return", bim.Status.CurrentState, types.BackingImageManagerStateUnknown)
				c.eventRecorder.Eventf(bim, v1.EventTypeWarning, EventReasonUnknown, "Node or disk is not ready, will update state from %v to %v then return", bim.Status.CurrentState, types.BackingImageManagerStateUnknown)
			}
			if diskMigrated {
				log.Warnf("Disk %v(%v) is migrated to path %v on node %v; will update state from %v to %v then return", diskName, bim.Spec.DiskUUID, node.Spec.Disks[diskName].Path, node.Name, bim.Status.CurrentState, types.BackingImageManagerStateUnknown)
				c.eventRecorder.Eventf(bim, v1.EventTypeWarning, EventReasonUnknown, "Disk %v(%v) is migrated to path %v on node %v; will update state from %v to %v, do cleanup, then wait for spec update", diskName, bim.Spec.DiskUUID, node.Spec.Disks[diskName].Path, node.Name, bim.Status.CurrentState, types.BackingImageManagerStateError)
			}
			bim.Status.CurrentState = types.BackingImageManagerStateUnknown
			c.updateForUnknownBackingImageManager(bim)
		}
		return nil
	}

	if err := c.syncBackingImageManagerPod(bim); err != nil {
		return err
	}

	if err := c.handleBackingImageFiles(bim); err != nil {
		return err
	}

	return nil
}

func (c *BackingImageManagerController) cleanupBackingImageManager(bim *longhorn.BackingImageManager) (err error) {
	log := getLoggerForBackingImageManager(c.logger, bim)

	// Do file cleanup for default manager only.
	defaultImage, err := c.ds.GetSettingValueExisted(types.SettingNameDefaultBackingImageManagerImage)
	if err != nil {
		return err
	}
	if bim.Spec.Image == defaultImage && bim.Status.CurrentState == types.BackingImageManagerStateRunning && bim.Status.IP != "" {
		cli, err := engineapi.NewBackingImageManagerClient(bim)
		if err != nil {
			log.WithError(err).Warnf("Failed to launch a gRPC client during cleanup, will skip deleting all files")
		} else {
			log.Infof("Start to delete all backing image files during cleanup")
			for biName := range bim.Status.BackingImageFileMap {
				if err := cli.Delete(biName); err != nil {
					log.WithError(err).Warnf("Failed to launch a gRPC client during cleanup, will skip deleting the file for backing image %v", biName)
					continue
				}
			}
			log.Infof("Deleted all backing image files during cleanup")
		}
	}
	if c.isMonitoring(bim.Name) {
		c.stopMonitoring(bim.Name)
	}
	if err := c.ds.DeletePod(bim.Name); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
}

func (c *BackingImageManagerController) updateForUnknownBackingImageManager(bim *longhorn.BackingImageManager) {
	if bim.Status.CurrentState != types.BackingImageManagerStateUnknown {
		return
	}

	if c.isMonitoring(bim.Name) {
		c.stopMonitoring(bim.Name)
	}

	for biName, info := range bim.Status.BackingImageFileMap {
		if info.State == types.BackingImageStateFailed {
			continue
		}
		info.State = types.BackingImageStateUnknown
		bim.Status.BackingImageFileMap[biName] = info
	}

	return
}

func (c *BackingImageManagerController) syncBackingImageManagerPod(bim *longhorn.BackingImageManager) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync backing image manager pod")
	}()

	log := getLoggerForBackingImageManager(c.logger, bim)

	pod, err := c.ds.GetPod(bim.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to get pod for backing image manager %v", bim.Name)
	}

	// Sync backing image manager status with related pod
	if pod == nil {
		isNewBackingImageManager := bim.Status.CurrentState == "" || bim.Status.CurrentState == types.BackingImageManagerStateStopped
		if isNewBackingImageManager {
			bim.Status.CurrentState = types.BackingImageManagerStateStopped
		} else {
			log.Errorf("No pod for backing image manager with state %v, will update to state %v", bim.Status.CurrentState, types.BackingImageManagerStateError)
			c.eventRecorder.Eventf(bim, v1.EventTypeWarning, EventReasonUpdate, "No pod for backing image manager with state %v, will update to state %v", bim.Status.CurrentState, types.BackingImageManagerStateError)
			bim.Status.CurrentState = types.BackingImageManagerStateError
		}
	} else if pod.Spec.NodeName != bim.Spec.NodeID {
		if bim.Status.CurrentState != types.BackingImageManagerStateError {
			log.Errorf("Pod node name %v doesn't match backing image manager node ID %v, will update to state %v", pod.Spec.NodeName, bim.Spec.NodeID, types.BackingImageManagerStateError)
			c.eventRecorder.Eventf(bim, v1.EventTypeWarning, EventReasonUpdate, "Pod node name %v doesn't match backing image manager node ID %v, will update to state %v", pod.Spec.NodeName, bim.Spec.NodeID, types.BackingImageManagerStateError)
			bim.Status.CurrentState = types.BackingImageManagerStateError
		}
	} else if pod.DeletionTimestamp != nil {
		if bim.Status.CurrentState != types.BackingImageManagerStateError {
			log.Errorf("Pod deletion timestamp is set for backing image manager with state %v, will update to state %v", bim.Status.CurrentState, types.BackingImageManagerStateError)
			c.eventRecorder.Eventf(bim, v1.EventTypeWarning, EventReasonUpdate, "Pod deletion timestamp is set for backing image manager with state %v, will update to state %v", bim.Status.CurrentState, types.BackingImageManagerStateError)
			bim.Status.CurrentState = types.BackingImageManagerStateError
		}
	} else {
		switch pod.Status.Phase {
		case v1.PodPending:
			if bim.Status.CurrentState == types.BackingImageManagerStateRunning {
				log.Errorf("Backing image manager is state %v but the related pod is pending", types.BackingImageManagerStateRunning)
				c.eventRecorder.Eventf(bim, v1.EventTypeWarning, EventReasonUpdate, "Backing image manager is state %v but the related pod is pending", types.BackingImageManagerStateRunning)
				bim.Status.CurrentState = types.BackingImageManagerStateError
			} else {
				bim.Status.CurrentState = types.BackingImageManagerStateStarting
			}
		case v1.PodRunning:
			// Make sure readiness probe has passed.
			isReady := true
			for _, st := range pod.Status.ContainerStatuses {
				if !st.Ready {
					isReady = false
					break
				}
			}
			if !isReady && bim.Status.CurrentState == types.BackingImageManagerStateRunning {
				log.Errorf("Backing image manager is state %v but the related pod container not ready, will update to state %v", types.BackingImageManagerStateRunning, types.BackingImageManagerStateError)
				c.eventRecorder.Eventf(bim, v1.EventTypeWarning, EventReasonUpdate, "Backing image manager is state %v but the related pod container not ready, will update to state %v", types.BackingImageManagerStateRunning, types.BackingImageManagerStateError)
				bim.Status.CurrentState = types.BackingImageManagerStateError
			} else if isReady && bim.Status.CurrentState != types.BackingImageManagerStateRunning {
				log.Infof("Backing image manager becomes state %v", types.BackingImageManagerStateRunning)
				c.eventRecorder.Eventf(bim, v1.EventTypeNormal, EventReasonUpdate, "Backing image manager becomes state %v", types.BackingImageManagerStateRunning)
				bim.Status.CurrentState = types.BackingImageManagerStateRunning
			}

			if bim.Status.CurrentState == types.BackingImageManagerStateRunning {
				bim.Status.IP = pod.Status.PodIP
			}
		default:
			log.Errorf("Unexpected pod phase %v, will update backing image manager to state %v", pod.Status.Phase, types.BackingImageManagerStateError)
			c.eventRecorder.Eventf(bim, v1.EventTypeWarning, EventReasonUpdate, "Unexpected pod phase %v, will update backing image manager to state %v", pod.Status.Phase, types.BackingImageManagerStateError)
			bim.Status.CurrentState = types.BackingImageManagerStateError
		}
	}

	if bim.Status.CurrentState == types.BackingImageManagerStateRunning {
		if bim.Status.APIVersion == engineapi.UnknownBackingImageManagerAPIVersion {
			if err := c.versionUpdater(bim); err != nil {
				return err
			}
		}
	} else {
		bim.Status.APIVersion = engineapi.UnknownBackingImageManagerAPIVersion
		bim.Status.APIMinVersion = engineapi.UnknownBackingImageManagerAPIVersion
	}

	// It's meaningless to start or monitor a pod for an old manager
	// since it will cleaned up immediately.
	defaultImage, err := c.ds.GetSettingValueExisted(types.SettingNameDefaultBackingImageManagerImage)
	if err != nil {
		return err
	}
	if bim.Spec.Image != defaultImage {
		return nil
	}

	if bim.Status.CurrentState == types.BackingImageManagerStateRunning && !c.isMonitoring(bim.Name) {
		c.startMonitoring(bim)
	} else if bim.Status.CurrentState != types.BackingImageManagerStateRunning && c.isMonitoring(bim.Name) {
		c.stopMonitoring(bim.Name)
	}

	// Delete and restart backing image manager pod.
	if bim.Status.CurrentState == types.BackingImageManagerStateError || bim.Status.CurrentState == types.BackingImageManagerStateStopped {
		for name, file := range bim.Status.BackingImageFileMap {
			if file.State == types.BackingImageStateFailed {
				continue
			}
			file.State = types.BackingImageStateUnknown
			file.Message = "Backing image manager pod is not running"
			bim.Status.BackingImageFileMap[name] = file
		}

		pod, err := c.ds.GetPod(bim.Name)
		if err != nil {
			return err
		}
		if pod != nil && pod.DeletionTimestamp == nil {
			if err := c.ds.DeletePod(pod.Name); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			log.Info("Deleting pod before recreation")
		} else if pod == nil {
			// Similar to InstanceManagerController.
			// Longhorn shouldn't create the pod when users set taints with NoExecute effect on a node the bim is preferred.
			if c.controllerID == bim.Spec.NodeID {
				if err := c.createBackingImageManagerPod(bim); err != nil {
					return err
				}
				bim.Status.CurrentState = types.BackingImageManagerStateStarting
				c.eventRecorder.Eventf(bim, v1.EventTypeNormal, EventReasonCreate, "Creating backing image manager pod %v for disk %v on node %v. Backing image manager state will become %v", bim.Name, bim.Spec.DiskUUID, bim.Spec.NodeID, types.BackingImageManagerStateStarting)
			}
		}
	}

	return nil
}

func (c *BackingImageManagerController) handleBackingImageFiles(bim *longhorn.BackingImageManager) (err error) {
	log := getLoggerForBackingImageManager(c.logger, bim)

	if bim.Status.CurrentState != types.BackingImageManagerStateRunning {
		return nil
	}

	if err := engineapi.CheckBackingImageManagerCompatibilty(bim.Status.APIMinVersion, bim.Status.APIVersion); err != nil {
		log.Debug("BackingImageManagerController will skip handling files for incompatible backing image manager")
		return nil
	}

	defaultImage, err := c.ds.GetSettingValueExisted(types.SettingNameDefaultBackingImageManagerImage)
	if err != nil {
		return err
	}
	if bim.Spec.Image != defaultImage {
		return nil
	}

	cli, err := engineapi.NewBackingImageManagerClient(bim)
	if err != nil {
		return err
	}

	if err := c.deleteInvalidBackingImages(bim, cli, log); err != nil {
		return err
	}

	if err := c.downloadBackingImages(bim, cli, log); err != nil {
		return err
	}

	return nil
}

func (c *BackingImageManagerController) deleteInvalidBackingImages(bim *longhorn.BackingImageManager, cli *engineapi.BackingImageManagerClient, log logrus.FieldLogger) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to do cleanup for invalid backing images")
	}()

	for biName := range bim.Status.BackingImageFileMap {
		bi, err := c.ds.GetBackingImage(biName)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		biUUID, exists := bim.Spec.BackingImages[biName]
		isInvalid := bi == nil || !exists || bi.Status.UUID != biUUID
		if !isInvalid {
			continue
		}
		log.Debugf("Start to delete the file for invalid backing image %v", biName)
		if err := cli.Delete(biName); err != nil && !types.ErrorIsNotFound(err) {
			return err
		}
		delete(bim.Status.BackingImageFileMap, biName)
		log.Debugf("Deleted the file for invalid backing image %v", biName)
		c.eventRecorder.Eventf(bim, v1.EventTypeNormal, EventReasonDelete, "Deleted backing image %v in disk %v on node %v", biName, bim.Spec.DiskUUID, bim.Spec.NodeID)
	}

	return nil
}

func (c *BackingImageManagerController) downloadBackingImages(currentBIM *longhorn.BackingImageManager, cli *engineapi.BackingImageManagerClient, bimLog logrus.FieldLogger) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to download backing images")
	}()

	defaultImage, err := c.ds.GetSettingValueExisted(types.SettingNameDefaultBackingImageManagerImage)
	if err != nil {
		return err
	}

	bims, err := c.ds.ListBackingImageManagers()
	if err != nil {
		return err
	}
	for biName := range currentBIM.Spec.BackingImages {
		currentInfo, exists := currentBIM.Status.BackingImageFileMap[biName]
		requireDownload := !exists || currentInfo.State == types.BackingImageStateFailed
		if !requireDownload {
			continue
		}

		bi, err := c.ds.GetBackingImage(biName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		log := bimLog.WithFields(logrus.Fields{"backingImage": biName, "backingImageUUID": bi.Status.UUID})

		pullRequired := true
		var senderCandidate *longhorn.BackingImageManager
		for _, bim := range bims {
			if bim.Status.CurrentState != types.BackingImageManagerStateRunning {
				continue
			}
			info, exists := bim.Status.BackingImageFileMap[biName]
			if !exists {
				continue
			}
			if info.State == types.BackingImageStateFailed {
				continue
			}
			pullRequired = false
			// Use images in default manager as senders only
			if bim.Spec.Image == defaultImage && info.State == types.BackingImageStateReady && info.SendingReference < bimtypes.SendingLimit {
				senderCandidate = bim
				break
			}
		}

		if pullRequired {
			isEligible, err := c.isEligibleForPulling(currentBIM, biName)
			if err != nil {
				return err
			}
			if !isEligible {
				log.Debugf("Current backing image manager is not eligible for pulling")
				continue
			}
			log.Debugf("Start to pull backing image")
			if _, err := cli.Pull(bi.Name, bi.Spec.ImageURL, bi.Status.UUID); err != nil {
				if types.ErrorAlreadyExists(err) {
					log.Debugf("Backing image already exists, no need to pull it again")
					continue
				}
				return err
			}
			log.Debugf("Pulling backing image")
			c.eventRecorder.Eventf(currentBIM, v1.EventTypeNormal, EventReasonPulling, "Pulling backing image %v in disk %v on node %v", bi.Name, currentBIM.Spec.DiskUUID, currentBIM.Spec.NodeID)
			continue
		}
		if senderCandidate != nil {
			log.WithFields(logrus.Fields{"fromHost": senderCandidate.Status.IP, "toHost": currentBIM.Status.IP, "size": bi.Status.Size}).Debugf("Start to sync backing image")
			if _, err := cli.Sync(biName, bi.Status.UUID, bi.Status.Checksum, senderCandidate.Status.IP, currentBIM.Status.IP, bi.Status.Size); err != nil {
				if types.ErrorAlreadyExists(err) {
					log.WithFields(logrus.Fields{"fromHost": senderCandidate.Status.IP, "toHost": currentBIM.Status.IP, "size": bi.Status.Size}).Debugf("Backing image already exists, no need to sync from others")
					continue
				}
				return err
			}
			log.WithFields(logrus.Fields{"fromHost": senderCandidate.Status.IP, "toHost": currentBIM.Status.IP, "size": bi.Status.Size}).Debugf("Syncing backing image")
			c.eventRecorder.Eventf(currentBIM, v1.EventTypeNormal, EventReasonSyncing, "Syncing backing image %v in disk %v on node %v from %v(%v)", bi.Name, currentBIM.Spec.DiskUUID, currentBIM.Spec.NodeID, senderCandidate.Name, senderCandidate.Status.IP)
			continue
		}
	}

	return nil
}

func (c *BackingImageManagerController) isEligibleForPulling(currentBIM *longhorn.BackingImageManager, biName string) (bool, error) {
	defaultBIMs, err := c.ds.ListDefaultBackingImageManagers()
	if err != nil {
		return false, err
	}

	candidateChecksumMap := map[string]string{}
	candidateChecksumList := []string{}
	for bimName, bim := range defaultBIMs {
		if _, exists := bim.Spec.BackingImages[biName]; !exists {
			continue
		}
		if bim.Status.CurrentState == types.BackingImageManagerStateError {
			continue
		}
		_, _, err := c.ds.GetReadyDiskNode(bim.Spec.DiskUUID)
		if err != nil {
			if !types.ErrorIsNotFound(err) {
				return false, err
			}
			continue
		}
		// Cannot use backing image manager name to calculate the checksum here.
		// For a backing image, Longhorn needs to make sure the pull-eligible BIM is always in the same disk regardless of the backing image manager version.
		// Otherwise, when the BIM upgrade happens when the 1st file is just pulled from the remote,
		// the downloaded file cannot be reused if the pull-eligible BIM for the new managers is not in the disk containing the file.
		cksum := util.GetStringChecksum(biName + bim.Spec.DiskUUID)
		candidateChecksumMap[cksum] = bimName
		candidateChecksumList = append(candidateChecksumList, cksum)
	}
	sort.Strings(candidateChecksumList)
	if len(candidateChecksumList) > 0 && currentBIM.Name == candidateChecksumMap[candidateChecksumList[0]] {
		return true, nil
	}
	return false, nil
}

func (c *BackingImageManagerController) createBackingImageManagerPod(bim *longhorn.BackingImageManager) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to create backing image manager pod")
	}()

	log := getLoggerForBackingImageManager(c.logger, bim)

	log.Infof("Start to create backing image manager pod")

	tolerations, err := c.ds.GetSettingTaintToleration()
	if err != nil {
		return err
	}
	nodeSelector, err := c.ds.GetSettingSystemManagedComponentsNodeSelector()
	if err != nil {
		return err
	}
	registrySecretSetting, err := c.ds.GetSetting(types.SettingNameRegistrySecret)
	if err != nil {
		return err
	}
	registrySecret := registrySecretSetting.Value

	podManifest, err := c.generateBackingImageManagerPodManifest(bim, tolerations, registrySecret, nodeSelector)
	if err != nil {
		return err
	}
	if _, err := c.ds.CreatePod(podManifest); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}

	log.Infof("Created backing image manager pod")

	return nil
}

func (c *BackingImageManagerController) generateBackingImageManagerPodManifest(bim *longhorn.BackingImageManager, tolerations []v1.Toleration, registrySecret string, nodeSelector map[string]string) (*v1.Pod, error) {
	tolerationsByte, err := json.Marshal(tolerations)
	if err != nil {
		return nil, err
	}

	priorityClass, err := c.ds.GetSetting(types.SettingNamePriorityClass)
	if err != nil {
		return nil, err
	}

	imagePullPolicy, err := c.ds.GetSettingImagePullPolicy()
	if err != nil {
		return nil, err
	}

	node, diskName, err := c.ds.GetReadyDiskNode(bim.Spec.DiskUUID)
	if err != nil {
		return nil, err
	}

	podSpec := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            bim.Name,
			Namespace:       c.namespace,
			OwnerReferences: datastore.GetOwnerReferencesForBackingImageManager(bim),
			Labels:          types.GetBackingImageManagerLabels(bim.Spec.NodeID, bim.Spec.DiskUUID),
			Annotations:     map[string]string{types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix): string(tolerationsByte)},
		},
		Spec: v1.PodSpec{
			ServiceAccountName: c.serviceAccount,
			Tolerations:        util.GetDistinctTolerations(tolerations),
			NodeSelector:       nodeSelector,
			PriorityClassName:  priorityClass.Value,
			Containers: []v1.Container{
				{
					Name:            BackingImageManagerPodContainerName,
					Image:           bim.Spec.Image,
					ImagePullPolicy: imagePullPolicy,
					Command: []string{
						"backing-image-manager", "--debug",
						"daemon",
						"--listen", fmt.Sprintf("%s:%d", "0.0.0.0", engineapi.BackingImageManagerDefaultPort),
						"--disk-path", node.Spec.Disks[diskName].Path,
					},
					ReadinessProbe: &v1.Probe{
						Handler: v1.Handler{
							TCPSocket: &v1.TCPSocketAction{
								Port: intstr.FromInt(engineapi.BackingImageManagerDefaultPort),
							},
						},
						InitialDelaySeconds: datastore.PodProbeInitialDelay,
						TimeoutSeconds:      datastore.PodProbeTimeoutSeconds,
						PeriodSeconds:       datastore.PodProbePeriodSeconds,
					},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "disk-path",
							MountPath: bimtypes.DiskPathInContainer,
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "disk-path",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: bim.Spec.DiskPath,
						},
					},
				},
			},
			NodeName:      bim.Spec.NodeID,
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	if registrySecret != "" {
		podSpec.Spec.ImagePullSecrets = []v1.LocalObjectReference{
			{
				Name: registrySecret,
			},
		}
	}

	return podSpec, nil
}

func (c *BackingImageManagerController) enqueueBackingImageManager(backingImageManager interface{}) {
	key, err := controller.KeyFunc(backingImageManager)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", backingImageManager, err))
		return
	}

	c.queue.AddRateLimited(key)
}

func isBackingImageManagerPod(obj interface{}) bool {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		deletedState, ok := obj.(*cache.DeletedFinalStateUnknown)
		if !ok {
			return false
		}

		// use the last known state, to enqueue, dependent objects
		pod, ok = deletedState.Obj.(*v1.Pod)
		if !ok {
			return false
		}
	}

	if pod.Labels[types.GetLonghornLabelComponentKey()] == types.LonghornLabelBackingImageManager {
		return true
	}
	return false
}

func (c *BackingImageManagerController) enqueueForBackingImage(obj interface{}) {
	backingImage, ok := obj.(*longhorn.BackingImage)
	if !ok {
		deletedState, ok := obj.(*cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to enqueue, dependent objects
		backingImage, ok = deletedState.Obj.(*longhorn.BackingImage)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	backingImage, err := c.ds.GetBackingImage(backingImage.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		utilruntime.HandleError(fmt.Errorf("Couldn't get backing image %v: %v ", backingImage.Name, err))
		return
	}

	bims, err := c.ds.ListBackingImageManagers()
	if err != nil {
		if apierrors.IsNotFound(err) {
			c.logger.WithField("backingImage", backingImage.Name).Warnf("Can't list backing image managers for a backing image, may be deleted")
			return
		}
		utilruntime.HandleError(fmt.Errorf("couldn't list backing image manager: %v", err))
		return
	}

	for _, bim := range bims {
		if _, exists := bim.Spec.BackingImages[backingImage.Name]; exists {
			c.enqueueBackingImageManager(bim)
		}
	}
}

func (c *BackingImageManagerController) enqueueForLonghornNode(obj interface{}) {
	node, ok := obj.(*longhorn.Node)
	if !ok {
		deletedState, ok := obj.(*cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to enqueue, dependent objects
		node, ok = deletedState.Obj.(*longhorn.Node)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	node, err := c.ds.GetNode(node.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// there is no Longhorn node created for the Kubernetes
			// node (e.g. controller/etcd node). Skip it
			return
		}
		utilruntime.HandleError(fmt.Errorf("Couldn't get node %v: %v ", node.Name, err))
		return
	}

	bims, err := c.ds.ListBackingImageManagersByNode(node.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			c.logger.WithField("node", node.Name).Warnf("Can't list backing image managers for a node, may be deleted")
			return
		}
		utilruntime.HandleError(fmt.Errorf("couldn't get backing image manager: %v", err))
		return
	}

	for _, bim := range bims {
		c.enqueueBackingImageManager(bim)
	}
}

func (c *BackingImageManagerController) enqueueForBackingImageManagerPod(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		deletedState, ok := obj.(*cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to enqueue, dependent objects
		pod, ok = deletedState.Obj.(*v1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	bim, err := c.ds.GetBackingImageManager(pod.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			c.logger.WithField("pod", pod.Name).Warnf("Can't find backing image manager for pod, may be deleted")
			return
		}
		utilruntime.HandleError(fmt.Errorf("couldn't get backing image manager: %v", err))
		return
	}
	c.enqueueBackingImageManager(bim)
}

func (c *BackingImageManagerController) startMonitoring(bim *longhorn.BackingImageManager) {
	log := getLoggerForBackingImageManager(c.logger, bim)

	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.monitorMap[bim.Name]; ok {
		log.Error("BUG: Monitoring goroutine already exists")
		return
	}

	client, err := engineapi.NewBackingImageManagerClient(bim)
	if err != nil {
		log.Error("Failed to launch gRPC client for backing image manager before monitoring")
		return
	}
	stream, err := client.Watch()
	if err != nil {
		log.Error("Failed to launch gRPC watching stream for backing image manager before monitoring")
		return
	}

	stopCh := make(chan struct{}, 1)
	monitorVoluntaryStopCh := make(chan struct{})
	monitor := &BackingImageManagerMonitor{
		Name:         bim.Name,
		controllerID: c.controllerID,

		ds:                     c.ds,
		log:                    log,
		lock:                   &sync.Mutex{},
		stopCh:                 stopCh,
		monitorVoluntaryStopCh: monitorVoluntaryStopCh,
		done:                   false,
		updateNotification:     true,

		client: client,
		stream: stream,
	}
	c.monitorMap[bim.Name] = stopCh

	go monitor.Run()
	go func() {
		<-monitorVoluntaryStopCh
		c.stopMonitoring(bim.Name)
	}()
}

func (c *BackingImageManagerController) stopMonitoring(bimName string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	log := c.logger.WithField("backingImageManager", bimName)
	log.Infof("Stopping monitoring")
	stopCh, ok := c.monitorMap[bimName]
	if !ok {
		log.Warn("No monitor goroutine for stopping")
		return
	}
	select {
	case <-stopCh:
		// channel is already closed
	default:
		close(stopCh)
	}

	delete(c.monitorMap, bimName)
	log.Infof("Stopped monitoring")

	return
}

func (c *BackingImageManagerController) isMonitoring(bimName string) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	_, ok := c.monitorMap[bimName]
	return ok
}

func (m *BackingImageManagerMonitor) Run() {
	m.log.Debugf("Start monitoring")
	defer func() {
		m.log.Debugf("Stop monitoring")
		if err := m.stream.Close(); err != nil {
			m.log.Errorf("Failed to close streaming when stopping monitoring")
		}
		close(m.monitorVoluntaryStopCh)
	}()

	go func() {
		continuousFailureCount := 0
		for {
			if continuousFailureCount >= engineapi.MaxMonitorRetryCount {
				m.done = true
			}

			if m.done {
				return
			}

			if err := m.stream.Recv(); err != nil {
				m.log.WithError(err).Errorf("error receiving next item")
				continuousFailureCount++
				time.Sleep(engineapi.MinPollCount * engineapi.PollInterval)
			} else {
				continuousFailureCount = 0
				m.lock.Lock()
				m.updateNotification = true
				m.lock.Unlock()
			}
		}
	}()

	needUpdate := false
	timer := 0
	ticker := time.NewTicker(engineapi.MinPollCount * engineapi.PollInterval)
	defer ticker.Stop()
	tick := ticker.C
	for {
		select {
		case <-tick:
			if m.done {
				return
			}

			m.lock.Lock()
			needUpdate = false
			timer++
			if timer >= engineapi.MaxPollCount || m.updateNotification {
				needUpdate = true
				m.updateNotification = false
				timer = 0
			}
			m.lock.Unlock()

			if needUpdate {
				if needStop := m.pollAndUpdateBackingImageFileMap(); needStop {
					m.done = true
					return
				}
			}
		case <-m.stopCh:
			m.done = true
			return
		}
	}
}

func (m *BackingImageManagerMonitor) pollAndUpdateBackingImageFileMap() (needStop bool) {
	var monitorErr error
	defer func() {
		if monitorErr != nil {
			m.log.WithError(monitorErr).Error("Failed to poll and update backing image file map in monitor goroutine")
		}
	}()
	bim, err := m.ds.GetBackingImageManager(m.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			m.log.Info("stop monitoring because the backing image manager no longer exists")
			return true
		}
		monitorErr = err
		return false
	}

	if bim.Status.OwnerID != m.controllerID {
		m.log.Info("stop monitoring because the backing image manager owner ID becomes %v", bim.Status.OwnerID)
		return true
	}

	resp, err := m.client.List()
	if err != nil {
		monitorErr = err
		return false
	}

	if reflect.DeepEqual(bim.Status.BackingImageFileMap, resp) {
		return false
	}

	bim.Status.BackingImageFileMap = resp
	if _, err := m.ds.UpdateBackingImageManagerStatus(bim); err != nil {
		monitorErr = err
		return false
	}

	return false
}

func (c *BackingImageManagerController) isResponsibleFor(bim *longhorn.BackingImageManager) bool {
	return isControllerResponsibleFor(c.controllerID, c.ds, bim.Name, bim.Spec.NodeID, bim.Status.OwnerID)
}

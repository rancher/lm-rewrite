package controller

import (
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	typedv1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
)

type BackingImageController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the backing image
	controllerID   string
	serviceAccount string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	biStoreSynced  cache.InformerSynced
	bimStoreSynced cache.InformerSynced
	rStoreSynced   cache.InformerSynced
}

func NewBackingImageController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	backingImageInformer lhinformers.BackingImageInformer,
	backingImageManagerInformer lhinformers.BackingImageManagerInformer,
	replicaInformer lhinformers.ReplicaInformer,
	kubeClient clientset.Interface,
	namespace string, controllerID, serviceAccount string) *BackingImageController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&typedv1core.EventSinkImpl{Interface: typedv1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	bic := &BackingImageController{
		baseController: newBaseController("longhorn-backing-image", logger),

		namespace:      namespace,
		controllerID:   controllerID,
		serviceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-backing-image-controller"}),

		ds: ds,

		biStoreSynced:  backingImageInformer.Informer().HasSynced,
		bimStoreSynced: backingImageManagerInformer.Informer().HasSynced,
		rStoreSynced:   replicaInformer.Informer().HasSynced,
	}

	backingImageInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    bic.enqueueBackingImage,
		UpdateFunc: func(old, cur interface{}) { bic.enqueueBackingImage(cur) },
		DeleteFunc: bic.enqueueBackingImage,
	})

	backingImageManagerInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    bic.enqueueBackingImageForBackingImageManager,
		UpdateFunc: func(old, cur interface{}) { bic.enqueueBackingImageForBackingImageManager(cur) },
		DeleteFunc: bic.enqueueBackingImageForBackingImageManager,
	})

	replicaInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    bic.enqueueBackingImageForReplica,
		UpdateFunc: func(old, cur interface{}) { bic.enqueueBackingImageForReplica(cur) },
		DeleteFunc: bic.enqueueBackingImageForReplica,
	})

	return bic
}

func (bic *BackingImageController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer bic.queue.ShutDown()

	logrus.Infof("Start Longhorn Backing Image controller")
	defer logrus.Infof("Shutting down Longhorn Backing Image controller")

	if !cache.WaitForNamedCacheSync("longhorn backing images", stopCh, bic.biStoreSynced, bic.bimStoreSynced, bic.rStoreSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(bic.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (bic *BackingImageController) worker() {
	for bic.processNextWorkItem() {
	}
}

func (bic *BackingImageController) processNextWorkItem() bool {
	key, quit := bic.queue.Get()

	if quit {
		return false
	}
	defer bic.queue.Done(key)

	err := bic.syncBackingImage(key.(string))
	bic.handleErr(err, key)

	return true
}

func (bic *BackingImageController) handleErr(err error, key interface{}) {
	if err == nil {
		bic.queue.Forget(key)
		return
	}

	if bic.queue.NumRequeues(key) < maxRetries {
		logrus.Warnf("Error syncing Longhorn backing image %v: %v", key, err)
		bic.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logrus.Warnf("Dropping Longhorn backing image %v out of the queue: %v", key, err)
	bic.queue.Forget(key)
}

func getLoggerForBackingImage(logger logrus.FieldLogger, bi *longhorn.BackingImage) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"backingImageName": bi.Name,
			"imageURL":         bi.Spec.ImageURL,
		},
	)
}

func (bic *BackingImageController) syncBackingImage(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync backing image for %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != bic.namespace {
		return nil
	}

	backingImage, err := bic.ds.GetBackingImage(name)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			bic.logger.WithField("backingImage", name).WithError(err).Error("Failed to retrieve backing image from datastore")
			return err
		}
		bic.logger.WithField("backingImage", name).Debug("Can't find backing image, may have been deleted")
		return nil
	}

	log := getLoggerForBackingImage(bic.logger, backingImage)

	if !bic.isResponsibleFor(backingImage) {
		return nil
	}
	if backingImage.Status.OwnerID != bic.controllerID {
		backingImage.Status.OwnerID = bic.controllerID
		backingImage, err = bic.ds.UpdateBackingImageStatus(backingImage)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("Backing Image got new owner %v", bic.controllerID)
	}

	if backingImage.DeletionTimestamp != nil {
		replicas, err := bic.ds.ListReplicasByBackingImage(backingImage.Name)
		if err != nil {
			return err
		}
		if len(replicas) != 0 {
			log.Info("Need to wait for all replicas stopping using this backing image before removing the finalizer")
			return nil
		}
		log.Info("No replica is using this backing image, will clean up the record for backing image managers and remove the finalizer then")
		if err := bic.cleanupBackingImageManagers(backingImage); err != nil {
			return err
		}
		return bic.ds.RemoveFinalizerForBackingImage(backingImage)
	}

	// UUID is immutable once it's set.
	// Should make sure UUID is not empty before syncing with other resources.
	if backingImage.Status.UUID == "" {
		backingImage.Status.UUID = util.RandomID()
		if backingImage, err = bic.ds.UpdateBackingImageStatus(backingImage); err != nil {
			if !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
			log.WithError(err).Debugf("Requeue %v due to conflict", key)
			bic.enqueueBackingImage(backingImage)
			return nil
		}
		bic.eventRecorder.Eventf(backingImage, corev1.EventTypeNormal, EventReasonUpdate, "Initialized UUID to %v", backingImage.Status.UUID)
	}

	existingBackingImage := backingImage.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if reflect.DeepEqual(existingBackingImage.Status, backingImage.Status) {
			return
		}
		if _, err := bic.ds.UpdateBackingImageStatus(backingImage); err != nil && apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", key)
			bic.enqueueBackingImage(backingImage)
		}
	}()

	if backingImage.Status.DiskFileStateMap == nil {
		backingImage.Status.DiskFileStateMap = map[string]types.BackingImageState{}
	}
	if backingImage.Status.DiskFileHandlingProgressMap == nil {
		backingImage.Status.DiskFileHandlingProgressMap = map[string]int{}
	}
	if backingImage.Status.DiskLastRefAtMap == nil {
		backingImage.Status.DiskLastRefAtMap = map[string]string{}
	}

	if err := bic.handleBackingImageManagers(backingImage); err != nil {
		return err
	}

	if err := bic.syncBackingImageDownloadInfo(backingImage); err != nil {
		return err
	}

	if err := bic.updateDiskLastReferenceMap(backingImage); err != nil {
		return err
	}

	return nil
}

func (bic *BackingImageController) cleanupBackingImageManagers(bi *longhorn.BackingImage) (err error) {
	defaultImage, err := bic.ds.GetSettingValueExisted(types.SettingNameDefaultBackingImageManagerImage)
	if err != nil {
		return err
	}

	log := getLoggerForBackingImage(bic.logger, bi)

	bimMap, err := bic.ds.ListBackingImageManagers()
	if err != nil {
		return err
	}
	for _, bim := range bimMap {
		if bim.DeletionTimestamp != nil {
			continue
		}

		// Directly clean up old backing image managers (including incompatible managers).
		// New backing image managers can detect and reuse the existing backing image files if necessary.
		if bim.Spec.Image != defaultImage {
			log.WithField("backingImageManager", bim.Name).Info("Start to delete old backing image manager")
			if err := bic.ds.DeleteBackingImageManager(bim.Name); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			log.WithField("backingImageManager", bim.Name).Info("Deleting old backing image manager")
			bic.eventRecorder.Eventf(bi, corev1.EventTypeNormal, EventReasonDelete, "delete old backing image manager %v in disk %v on node %v", bim.Name, bim.Spec.DiskUUID, bim.Spec.NodeID)
			continue
		}

		// This sync loop cares about the backing image managers related to the current backing image only.
		// The backing image manager doesn't contain the current backing image.
		if _, isRelatedToCurrentBI := bim.Spec.BackingImages[bi.Name]; !isRelatedToCurrentBI {
			continue
		}
		// The entry in the backing image manager matches the current backing image.
		if _, isStillRequiredByCurrentBI := bi.Spec.Disks[bim.Spec.DiskUUID]; isStillRequiredByCurrentBI && bi.DeletionTimestamp == nil {
			if bim.Spec.BackingImages[bi.Name] == bi.Status.UUID {
				continue
			}
		}

		// The current backing image doesn't require this manager any longer, or the entry in backing image manager doesn't match the backing image uuid.
		delete(bim.Spec.BackingImages, bi.Name)
		if bim, err = bic.ds.UpdateBackingImageManager(bim); err != nil {
			return err
		}
		if len(bim.Spec.BackingImages) == 0 {
			log.WithField("backingImageManager", bim.Name).Info("Start to delete unused backing image manager")
			if err := bic.ds.DeleteBackingImageManager(bim.Name); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			log.WithField("backingImageManager", bim.Name).Info("Deleting unused backing image manager")
			bic.eventRecorder.Eventf(bi, corev1.EventTypeNormal, EventReasonDelete, "delete unused backing image manager %v in disk %v on node %v", bim.Name, bim.Spec.DiskUUID, bim.Spec.NodeID)
			continue
		}
	}

	return nil
}

func (bic *BackingImageController) handleBackingImageManagers(bi *longhorn.BackingImage) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to handle backing image managers")
	}()

	log := getLoggerForBackingImage(bic.logger, bi)

	if err := bic.cleanupBackingImageManagers(bi); err != nil {
		return err
	}

	defaultImage, err := bic.ds.GetSettingValueExisted(types.SettingNameDefaultBackingImageManagerImage)
	if err != nil {
		return err
	}

	for diskUUID := range bi.Spec.Disks {
		noDefaultBIM := true
		requiredBIs := map[string]string{}

		bimMap, err := bic.ds.ListBackingImageManagersByDiskUUID(diskUUID)
		if err != nil {
			return err
		}
		for _, bim := range bimMap {
			if bim.Spec.Image == defaultImage {
				if uuidInManager, exists := bim.Spec.BackingImages[bi.Name]; !exists || uuidInManager != bi.Status.UUID {
					bim.Spec.BackingImages[bi.Name] = bi.Status.UUID
					if bim, err = bic.ds.UpdateBackingImageManager(bim); err != nil {
						return err
					}
				}
				noDefaultBIM = false
				break
			}
			for biName, uuid := range bim.Spec.BackingImages {
				requiredBIs[biName] = uuid
			}
		}

		if noDefaultBIM {
			log.Infof("Cannot find default backing image manager for disk %v, will create it", diskUUID)

			node, diskName, err := bic.ds.GetReadyDiskNode(diskUUID)
			if err != nil {
				if types.ErrorIsNotFound(err) {
					log.WithField("diskUUID", diskUUID).WithError(err).Warnf("Disk is not ready hence there is no way to create backing image manager then")
					continue
				}
				return err
			}
			requiredBIs[bi.Name] = bi.Status.UUID
			manifest := bic.generateBackingImageManagerManifest(node, diskName, defaultImage, requiredBIs)
			bim, err := bic.ds.CreateBackingImageManager(manifest)
			if err != nil {
				return err
			}

			log.WithFields(logrus.Fields{"backingImageManager": bim.Name, "diskUUID": diskUUID}).Infof("Created default backing image manager")
			bic.eventRecorder.Eventf(bi, corev1.EventTypeNormal, EventReasonCreate, "created default backing image manager %v in disk %v on node %v", bim.Name, bim.Spec.DiskUUID, bim.Spec.NodeID)
		}
	}

	return nil
}

// syncBackingImageDownloadInfo blindly updates the disk download info based on the results of backing image managers.
func (bic *BackingImageController) syncBackingImageDownloadInfo(bi *longhorn.BackingImage) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync backing image download state")
	}()

	diskFileStateMap := map[string]types.BackingImageState{}
	diskFileHandlingProgressMap := map[string]int{}
	bimMap, err := bic.ds.ListBackingImageManagers()
	if err != nil {
		return err
	}
	for _, bim := range bimMap {
		if bim.DeletionTimestamp != nil {
			continue
		}
		info, exists := bim.Status.BackingImageFileMap[bi.Name]
		if !exists {
			continue
		}
		if info.UUID != bi.Status.UUID {
			continue
		}
		diskFileStateMap[bim.Spec.DiskUUID] = info.State
		diskFileHandlingProgressMap[bim.Spec.DiskUUID] = info.Progress

		if info.UploadPort != 0 && info.State == types.BackingImageStateStarting {
			uploadServerAddress := fmt.Sprintf("%s:%d", bim.Status.IP, info.UploadPort)
			bi.Status.UploadAddress = uploadServerAddress
		}

		if info.Size > 0 {
			if bi.Status.Size == 0 {
				bi.Status.Size = info.Size
				bic.eventRecorder.Eventf(bi, corev1.EventTypeNormal, EventReasonUpdate, "Set size to %v", bi.Status.Size)
			}
			if bi.Status.Size != info.Size {
				return fmt.Errorf("BUG: found mismatching size %v in disk %v, the current size is %v", info.Size, bim.Spec.DiskUUID, bi.Status.Size)
			}
		}
	}

	bi.Status.DiskFileStateMap = diskFileStateMap
	bi.Status.DiskFileHandlingProgressMap = diskFileHandlingProgressMap

	return nil
}

func (bic *BackingImageController) updateDiskLastReferenceMap(backingImage *longhorn.BackingImage) error {
	replicas, err := bic.ds.ListReplicasByBackingImage(backingImage.Name)
	if err != nil {
		return err
	}
	disksInUse := map[string]struct{}{}
	for _, replica := range replicas {
		disksInUse[replica.Spec.DiskID] = struct{}{}
		delete(backingImage.Status.DiskLastRefAtMap, replica.Spec.DiskID)
	}
	for diskUUID := range backingImage.Status.DiskLastRefAtMap {
		if _, exists := backingImage.Spec.Disks[diskUUID]; !exists {
			delete(backingImage.Status.DiskLastRefAtMap, diskUUID)
		}
	}
	for diskUUID := range backingImage.Spec.Disks {
		_, isActiveDisk := disksInUse[diskUUID]
		_, isRecordedHistoricDisk := backingImage.Status.DiskLastRefAtMap[diskUUID]
		if !isActiveDisk && !isRecordedHistoricDisk {
			backingImage.Status.DiskLastRefAtMap[diskUUID] = util.Now()
		}
	}
	if !backingImage.Spec.RequireUpload {
		return nil
	}

	// For upload backing image, Longhorn should retain at least one ready
	// file or the uploading file.
	containsActiveDownloadedDisk := false
	downloadedDiskRetainCandidate := ""
	for diskUUID := range backingImage.Spec.Disks {
		if backingImage.Status.DiskFileStateMap[diskUUID] != types.BackingImageStateReady {
			continue
		}
		if backingImage.Status.DiskLastRefAtMap[diskUUID] == "" {
			containsActiveDownloadedDisk = true
			break
		}
		if downloadedDiskRetainCandidate == "" {
			downloadedDiskRetainCandidate = diskUUID
		}
	}
	if containsActiveDownloadedDisk {
		return nil
	}
	// Retain one ready file.
	if downloadedDiskRetainCandidate != "" {
		delete(backingImage.Status.DiskLastRefAtMap, downloadedDiskRetainCandidate)
		return nil
	}
	// No ready entry. The upload is still in progress.
	for diskUUID := range backingImage.Spec.Disks {
		if backingImage.Status.DiskFileStateMap[diskUUID] == types.BackingImageStateFailed {
			continue
		}
		delete(backingImage.Status.DiskLastRefAtMap, diskUUID)
	}
	return nil
}

func (bic *BackingImageController) generateBackingImageManagerManifest(node *longhorn.Node, diskName, defaultImage string, requiredBackingImages map[string]string) *longhorn.BackingImageManager {
	return &longhorn.BackingImageManager{
		ObjectMeta: metav1.ObjectMeta{
			Labels: types.GetBackingImageManagerLabels(node.Name, node.Status.DiskStatus[diskName].DiskUUID),
			Name:   types.GetBackingImageManagerName(defaultImage, node.Status.DiskStatus[diskName].DiskUUID),
		},
		Spec: types.BackingImageManagerSpec{
			Image:         defaultImage,
			NodeID:        node.Name,
			DiskUUID:      node.Status.DiskStatus[diskName].DiskUUID,
			DiskPath:      node.Spec.Disks[diskName].Path,
			BackingImages: requiredBackingImages,
		},
	}
}

func (bic *BackingImageController) enqueueBackingImage(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	bic.queue.AddRateLimited(key)
}

func (bic *BackingImageController) enqueueBackingImageForBackingImageManager(obj interface{}) {
	bim, isBIM := obj.(*longhorn.BackingImageManager)
	if !isBIM {
		deletedState, ok := obj.(*cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		bim, ok = deletedState.Obj.(*longhorn.BackingImageManager)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained non BackingImageManager object: %#v", deletedState.Obj))
			return
		}
	}

	for biName := range bim.Spec.BackingImages {
		key := bim.Namespace + "/" + biName
		bic.queue.AddRateLimited(key)
	}
	for biName := range bim.Status.BackingImageFileMap {
		key := bim.Namespace + "/" + biName
		bic.queue.AddRateLimited(key)
	}
}

func (bic *BackingImageController) enqueueBackingImageForReplica(obj interface{}) {
	replica, isReplica := obj.(*longhorn.Replica)
	if !isReplica {
		deletedState, ok := obj.(*cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		replica, ok = deletedState.Obj.(*longhorn.Replica)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained non Replica object: %#v", deletedState.Obj))
			return
		}
	}

	if replica.Spec.BackingImage != "" {
		bic.logger.WithField("replica", replica.Name).WithField("backingImage", replica.Spec.BackingImage).Trace("Enqueuing backing image for replica")
		key := replica.Namespace + "/" + replica.Spec.BackingImage
		bic.queue.AddRateLimited(key)
		return
	}
}

func (bic *BackingImageController) isResponsibleFor(bi *longhorn.BackingImage) bool {
	return isControllerResponsibleFor(bic.controllerID, bic.ds, bi.Name, "", bi.Status.OwnerID)
}

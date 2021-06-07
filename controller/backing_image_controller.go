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

	biStoreSynced   cache.InformerSynced
	bimStoreSynced  cache.InformerSynced
	bidsStoreSynced cache.InformerSynced
	rStoreSynced    cache.InformerSynced
}

func NewBackingImageController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	backingImageInformer lhinformers.BackingImageInformer,
	backingImageManagerInformer lhinformers.BackingImageManagerInformer,
	backingImageDataSourceInformer lhinformers.BackingImageDataSourceInformer,
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

		biStoreSynced:   backingImageInformer.Informer().HasSynced,
		bimStoreSynced:  backingImageManagerInformer.Informer().HasSynced,
		bidsStoreSynced: backingImageDataSourceInformer.Informer().HasSynced,
		rStoreSynced:    replicaInformer.Informer().HasSynced,
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

	backingImageDataSourceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    bic.enqueueBackingImageForBackingImageDataSource,
		UpdateFunc: func(old, cur interface{}) { bic.enqueueBackingImageForBackingImageDataSource(cur) },
		DeleteFunc: bic.enqueueBackingImageForBackingImageDataSource,
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

	if !cache.WaitForNamedCacheSync("longhorn backing images", stopCh, bic.biStoreSynced, bic.bimStoreSynced, bic.bidsStoreSynced, bic.rStoreSynced) {
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

	if backingImage.Status.DiskFileStatusMap == nil {
		backingImage.Status.DiskFileStatusMap = map[string]*types.BackingImageDiskFileStatus{}
	}
	if backingImage.Status.DiskLastRefAtMap == nil {
		backingImage.Status.DiskLastRefAtMap = map[string]string{}
	}

	if err := bic.handleBackingImageDataSource(backingImage); err != nil {
		return err
	}

	if err := bic.handleBackingImageManagers(backingImage); err != nil {
		return err
	}

	if err := bic.syncBackingImageFileInfo(backingImage); err != nil {
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

func (bic *BackingImageController) handleBackingImageDataSource(bi *longhorn.BackingImage) (err error) {
	log := getLoggerForBackingImage(bic.logger, bi)

	allFilesFailed := false
	for _, fileStatus := range bi.Status.DiskFileStatusMap {
		allFilesFailed = true
		if fileStatus.State != types.BackingImageStateFailed {
			allFilesFailed = false
			break
		}
	}

	bids, err := bic.ds.GetBackingImageDataSource(bi.Name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		log.Warn("Cannot find backing image data source, then controller will skip handle it since the file is ready")
		return nil
	}
	existingBIDS := bids.DeepCopy()

	// Check if the data source already finished the 1st file preparing.
	if !bids.Spec.Started && bids.Status.CurrentState == types.BackingImageStateReady {
		// Cannot rely on backingImage.Status.DiskFileStatusMap[bids.Spec.DiskUUID]
		// Need to make sure the backing image manager take over the file before marking the data source as started
		defaultImage, err := bic.ds.GetSettingValueExisted(types.SettingNameDefaultBackingImageManagerImage)
		if err != nil {
			return err
		}
		bims, err := bic.ds.ListBackingImageManagersByDiskUUID(bids.Spec.DiskUUID)
		if err != nil {
			return err
		}
		var defaultBIM *longhorn.BackingImageManager
		for _, bim := range bims {
			if bim.Spec.Image == defaultImage {
				defaultBIM = bim
				break
			}
		}
		if defaultBIM != nil {
			if fileInfo, exists := defaultBIM.Status.BackingImageFileMap[bi.Name]; exists && fileInfo.State == types.BackingImageStateReady && fileInfo.UUID == bi.Status.UUID {
				bids.Spec.Started = true
				log.Infof("Backing image manager %v already took over the file prepared by the backing image data source, will mark the data source as started", defaultBIM.Name)
			}
		}
	} else if bids.Spec.Started && allFilesFailed {
		switch bids.Spec.SourceType {
		case types.BackingImageDataSourceTypeDownload:
			log.Info("Prepare to re-download backing image via backing image data source since all existing files become unavailable")
			bids.Spec.Started = false
		default:
			log.Warnf("Cannot recover backing image after all existing files becoming unavailable, since the backing image data source with type %v doesn't support restarting", bids.Spec.SourceType)
		}
	}

	if !bids.Spec.Started {
		if _, _, err := bic.ds.GetReadyDiskNode(bids.Spec.DiskUUID); err != nil && types.ErrorIsNotFound(err) {
			nodes, err := bic.ds.ListReadyNodes()
			if err != nil {
				return err
			}
			for _, node := range nodes {
				updated := false
				for diskName, diskSpec := range node.Spec.Disks {
					diskStatus, exists := node.Status.DiskStatus[diskName]
					if !exists {
						continue
					}
					if types.GetCondition(diskStatus.Conditions, types.DiskConditionTypeSchedulable).Status != types.ConditionStatusTrue {
						continue
					}
					bids.Spec.DiskUUID = diskStatus.DiskUUID
					bids.Spec.DiskPath = diskSpec.Path
					bids.Spec.NodeID = node.Name
					updated = true
				}
				if updated {
					break
				}
			}
		}
	}

	if !reflect.DeepEqual(bids, existingBIDS) {
		if _, err := bic.ds.UpdateBackingImageDataSource(bids); err != nil {
			return err
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
			// Add current backing image record to the default manager
			if bim.DeletionTimestamp == nil && bim.Spec.Image == defaultImage {
				if uuidInManager, exists := bim.Spec.BackingImages[bi.Name]; !exists || uuidInManager != bi.Status.UUID {
					bim.Spec.BackingImages[bi.Name] = bi.Status.UUID
					if bim, err = bic.ds.UpdateBackingImageManager(bim); err != nil {
						return err
					}
				}
				noDefaultBIM = false
				break
			}
			// Otherwise, migrate records from non-default managers
			for biName, uuid := range bim.Spec.BackingImages {
				requiredBIs[biName] = uuid
			}
		}

		if noDefaultBIM {
			log.Infof("Cannot find default backing image manager for disk %v, will create it", diskUUID)

			node, diskName, err := bic.ds.GetReadyDiskNode(diskUUID)
			if err != nil {
				if !types.ErrorIsNotFound(err) {
					return err
				}
				log.WithField("diskUUID", diskUUID).WithError(err).Warnf("Disk is not ready hence there is no way to create backing image manager then")
				continue
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

// syncBackingImageFileInfo blindly updates the disk file info based on the results of backing image managers.
func (bic *BackingImageController) syncBackingImageFileInfo(bi *longhorn.BackingImage) (err error) {
	log := getLoggerForBackingImage(bic.logger, bi)
	defer func() {
		err = errors.Wrapf(err, "failed to sync backing image file state")
	}()

	currentDiskFiles := map[string]struct{}{}

	// Sync with backing image data source when the first file is not ready and not taken over by the backing image manager.
	bids, err := bic.ds.GetBackingImageDataSource(bi.Name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		log.Warn("Cannot find backing image data source, but controller will continue syncing backing image")
	}
	if bids != nil && !bids.Spec.Started {
		currentDiskFiles[bids.Spec.DiskUUID] = struct{}{}
		if _, exists := bi.Status.DiskFileStatusMap[bids.Spec.DiskUUID]; !exists {
			bi.Status.DiskFileStatusMap[bids.Spec.DiskUUID] = &types.BackingImageDiskFileStatus{}
		}
		bi.Status.DiskFileStatusMap[bids.Spec.DiskUUID].State = bids.Status.CurrentState
		bi.Status.DiskFileStatusMap[bids.Spec.DiskUUID].Progress = bids.Status.Progress
		bi.Status.DiskFileStatusMap[bids.Spec.DiskUUID].Message = bids.Status.Message
	}

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
		// If the backing image data source is up and preparing the 1st file,
		// backing image should ignore the file info of the related backing image manager.
		if bids != nil && !bids.Spec.Started && bids.Spec.DiskUUID == bim.Spec.DiskUUID {
			continue
		}
		currentDiskFiles[bim.Spec.DiskUUID] = struct{}{}

		if _, exists := bi.Status.DiskFileStatusMap[bim.Spec.DiskUUID]; !exists {
			bi.Status.DiskFileStatusMap[bim.Spec.DiskUUID] = &types.BackingImageDiskFileStatus{}
		}
		bi.Status.DiskFileStatusMap[bim.Spec.DiskUUID].State = info.State
		bi.Status.DiskFileStatusMap[bim.Spec.DiskUUID].Progress = info.Progress
		bi.Status.DiskFileStatusMap[bim.Spec.DiskUUID].Message = info.Message

		if info.Size > 0 {
			if bi.Status.Size == 0 {
				bi.Status.Size = info.Size
				bic.eventRecorder.Eventf(bi, corev1.EventTypeNormal, EventReasonUpdate, "Set size to %v", bi.Status.Size)
			}
			if bi.Status.Size != info.Size {
				return fmt.Errorf("BUG: found mismatching size %v in disk %v, the current size is %v", info.Size, bim.Spec.DiskUUID, bi.Status.Size)
			}
		}

		if info.CurrentChecksum != "" {
			if bi.Status.Checksum != "" && bi.Status.Checksum != info.CurrentChecksum {
				return fmt.Errorf("BUG: backing image recorded checksum %v doesn't match the file checksum %v in disk %v", bi.Status.Checksum, info.CurrentChecksum, bim.Spec.DiskUUID)
			}
			bi.Status.Checksum = info.CurrentChecksum
		}
	}

	for diskUUID := range bi.Status.DiskFileStatusMap {
		if _, exists := currentDiskFiles[diskUUID]; !exists {
			delete(bi.Status.DiskFileStatusMap, diskUUID)
		}
	}

	return nil
}

func (bic *BackingImageController) updateDiskLastReferenceMap(bi *longhorn.BackingImage) error {
	log := getLoggerForBackingImage(bic.logger, bi)

	bids, err := bic.ds.GetBackingImageDataSource(bi.Name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		log.Warn("Cannot find backing image data source, but controller will ignore it and continue updating disk last reference map")
	}

	replicas, err := bic.ds.ListReplicasByBackingImage(bi.Name)
	if err != nil {
		return err
	}
	filesInUse := map[string]struct{}{}
	for _, replica := range replicas {
		filesInUse[replica.Spec.DiskID] = struct{}{}
		delete(bi.Status.DiskLastRefAtMap, replica.Spec.DiskID)
	}
	for diskUUID := range bi.Status.DiskLastRefAtMap {
		if _, exists := bi.Spec.Disks[diskUUID]; !exists {
			delete(bi.Status.DiskLastRefAtMap, diskUUID)
		}
	}
	// Set timestamp for all candidates based on the replica usage first.
	for diskUUID := range bi.Spec.Disks {
		if _, exists := bi.Status.DiskFileStatusMap[diskUUID]; !exists {
			continue
		}
		_, isActiveFile := filesInUse[diskUUID]
		_, isRecordedHistoricFile := bi.Status.DiskLastRefAtMap[diskUUID]
		isFirstFile := bids != nil && !bids.Spec.Started && diskUUID == bids.Spec.DiskUUID
		if !isActiveFile && !isRecordedHistoricFile && !isFirstFile {
			// The timestamp may be removed later based on the HA requirement.
			bi.Status.DiskLastRefAtMap[diskUUID] = util.Now()
		}
	}

	// Determine the final cleanup entries based on HA requirement as well as
	// the current file state map.
	// TODO: Make `haBackingImageCount` configure when introducing HA backing image feature
	haBackingImageCount := 1

	var readyActiveFileCount, handlingActiveFileCount, failedActiveFileCount int
	readyInactiveFiles := map[string]struct{}{}
	handlingInactiveFiles := map[string]struct{}{}
	failedInactiveFiles := map[string]struct{}{}
	for diskUUID := range bi.Spec.Disks {
		// Consider non-existing files as pending backing image files.
		fileStatus, exists := bi.Status.DiskFileStatusMap[diskUUID]
		if !exists {
			fileStatus = &types.BackingImageDiskFileStatus{}
		}
		switch fileStatus.State {
		case types.BackingImageStateReady:
			if bi.Status.DiskLastRefAtMap[diskUUID] == "" {
				readyActiveFileCount++
			} else {
				readyInactiveFiles[diskUUID] = struct{}{}
			}
		case types.BackingImageStateFailed:
			if bi.Status.DiskLastRefAtMap[diskUUID] == "" {
				failedActiveFileCount++
			} else {
				failedInactiveFiles[diskUUID] = struct{}{}
			}
		default:
			if bi.Status.DiskLastRefAtMap[diskUUID] == "" {
				handlingActiveFileCount++
			} else {
				handlingInactiveFiles[diskUUID] = struct{}{}
			}
		}
	}

	// If there are enough ready files, controller will try to retain
	// those ready files to guarantee HA requirement
	inactiveFileCountShouldBeRetained := haBackingImageCount - readyActiveFileCount
	for diskUUID := range readyInactiveFiles {
		if inactiveFileCountShouldBeRetained <= 0 {
			break
		}
		inactiveFileCountShouldBeRetained--
		delete(bi.Status.DiskLastRefAtMap, diskUUID)
	}
	if inactiveFileCountShouldBeRetained <= 0 {
		return nil
	}

	// If there are enough ready/in-progress/pending files, controller will
	// try to retain those files to guarantee HA requirement
	inactiveFileCountShouldBeRetained -= handlingActiveFileCount
	for diskUUID := range handlingInactiveFiles {
		if inactiveFileCountShouldBeRetained <= 0 {
			break
		}
		inactiveFileCountShouldBeRetained--
		delete(bi.Status.DiskLastRefAtMap, diskUUID)
	}
	if inactiveFileCountShouldBeRetained <= 0 {
		return nil
	}

	// Otherwise, controller will try to retain all files including failed ones
	// to guarantee HA requirement
	inactiveFileCountShouldBeRetained -= failedActiveFileCount
	for diskUUID := range failedInactiveFiles {
		if inactiveFileCountShouldBeRetained <= 0 {
			break
		}
		inactiveFileCountShouldBeRetained--
		delete(bi.Status.DiskLastRefAtMap, diskUUID)
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

func (bic *BackingImageController) enqueueBackingImageForBackingImageDataSource(obj interface{}) {
	bic.enqueueBackingImage(obj)
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

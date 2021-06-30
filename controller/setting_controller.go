package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
)

const (
	VersionTagLatest = "latest"
)

var (
	upgradeCheckInterval          = time.Hour
	settingControllerResyncPeriod = time.Hour
	checkUpgradeURL               = "https://longhorn-upgrade-responder.rancher.io/v1/checkupgrade"
)

type SettingController struct {
	*baseController

	namespace    string
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	sStoreSynced cache.InformerSynced
	nStoreSynced cache.InformerSynced

	// upgrade checker
	lastUpgradeCheckedTimestamp time.Time
	version                     string

	// backup store monitor
	bsMonitor *BackupStoreMonitor
}

type BackupStoreMonitor struct {
	logger       logrus.FieldLogger
	controllerID string

	backupTarget                 string
	backupTargetCredentialSecret string

	pollInterval time.Duration

	target *engineapi.BackupTarget
	ds     *datastore.DataStore
	stopCh chan struct{}
}

type Version struct {
	Name        string // must be in semantic versioning
	ReleaseDate string
	Tags        []string
}

type CheckUpgradeRequest struct {
	LonghornVersion   string `json:"longhornVersion"`
	KubernetesVersion string `json:"kubernetesVersion"`
}

type CheckUpgradeResponse struct {
	Versions []Version `json:"versions"`
}

func NewSettingController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	settingInformer lhinformers.SettingInformer,
	nodeInformer lhinformers.NodeInformer,
	kubeClient clientset.Interface,
	namespace, controllerID, version string) *SettingController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	sc := &SettingController{
		baseController: newBaseController("longhorn-setting", logger),

		namespace:    namespace,
		controllerID: controllerID,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-setting-controller"}),

		ds: ds,

		sStoreSynced: settingInformer.Informer().HasSynced,
		nStoreSynced: nodeInformer.Informer().HasSynced,

		version: version,
	}

	settingInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.enqueueSetting,
		UpdateFunc: func(old, cur interface{}) { sc.enqueueSetting(cur) },
		DeleteFunc: sc.enqueueSetting,
	}, settingControllerResyncPeriod)

	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.enqueueSettingForNode,
		UpdateFunc: func(old, cur interface{}) { sc.enqueueSettingForNode(cur) },
	})

	return sc
}

func (sc *SettingController) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer sc.queue.ShutDown()

	sc.logger.Info("Start Longhorn Setting controller")
	defer sc.logger.Info("Shutting down Longhorn Setting controller")

	if !cache.WaitForNamedCacheSync("longhorn settings", stopCh, sc.sStoreSynced, sc.nStoreSynced) {
		return
	}

	// must remain single threaded since backup store monitor is not thread-safe now
	go wait.Until(sc.worker, time.Second, stopCh)

	<-stopCh
}

func (sc *SettingController) worker() {
	for sc.processNextWorkItem() {
	}
}

func (sc *SettingController) processNextWorkItem() bool {
	key, quit := sc.queue.Get()

	if quit {
		return false
	}
	defer sc.queue.Done(key)

	err := sc.syncSetting(key.(string))
	sc.handleErr(err, key)

	return true
}

func (sc *SettingController) handleErr(err error, key interface{}) {
	if err == nil {
		sc.queue.Forget(key)
		return
	}

	if sc.queue.NumRequeues(key) < maxRetries {
		sc.logger.WithError(err).Warnf("Error syncing Longhorn setting %v", key)
		sc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	sc.logger.WithError(err).Warnf("Dropping Longhorn setting %v out of the queue", key)
	sc.queue.Forget(key)
}

func (sc *SettingController) syncSetting(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync setting for %v", key)
	}()

	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	switch name {
	case string(types.SettingNameUpgradeChecker):
		if err := sc.syncUpgradeChecker(); err != nil {
			return err
		}
	case string(types.SettingNameBackupTargetCredentialSecret):
		fallthrough
	case string(types.SettingNameBackupTarget):
		if err := sc.syncBackupTarget(); err != nil {
			return err
		}
	case string(types.SettingNameBackupstorePollInterval):
		if err := sc.updateBackupstorePollInterval(); err != nil {
			return err
		}
	case string(types.SettingNameTaintToleration):
		if err := sc.updateTaintToleration(); err != nil {
			return err
		}
	case string(types.SettingNameSystemManagedComponentsNodeSelector):
		if err := sc.updateNodeSelector(); err != nil {
			return err
		}
	case string(types.SettingNameGuaranteedEngineManagerCPU):
	case string(types.SettingNameGuaranteedReplicaManagerCPU):
		if err := sc.updateInstanceManagerCPURequest(); err != nil {
			return err
		}
	case string(types.SettingNamePriorityClass):
		if err := sc.updatePriorityClass(); err != nil {
			return err
		}
	default:
	}

	return nil
}

func (sc *SettingController) syncBackupTarget() (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync backup target")
	}()

	targetSetting, err := sc.ds.GetSetting(types.SettingNameBackupTarget)
	if err != nil {
		return err
	}

	secretSetting, err := sc.ds.GetSetting(types.SettingNameBackupTargetCredentialSecret)
	if err != nil {
		return err
	}

	interval, err := sc.ds.GetSettingAsInt(types.SettingNameBackupstorePollInterval)
	if err != nil {
		return err
	}

	if sc.bsMonitor != nil {
		if sc.bsMonitor.backupTarget == targetSetting.Value &&
			sc.bsMonitor.backupTargetCredentialSecret == secretSetting.Value {
			// already monitoring
			return nil
		}
		sc.logger.Infof("Restarting backup store monitor because backup target changed from %v to %v", sc.bsMonitor.backupTarget, targetSetting.Value)
		sc.bsMonitor.Stop()
		sc.bsMonitor = nil
		manager.SyncVolumesLastBackupWithBackupVolumes(nil,
			sc.ds.ListVolumes, sc.ds.GetVolume, sc.ds.UpdateVolumeStatus)
	}

	if targetSetting.Value == "" {
		return nil
	}

	target, err := manager.GenerateBackupTarget(sc.ds)
	if err != nil {
		return err
	}
	sc.bsMonitor = &BackupStoreMonitor{
		logger:       sc.logger.WithField("component", "backup-store-monitor"),
		controllerID: sc.controllerID,

		backupTarget:                 targetSetting.Value,
		backupTargetCredentialSecret: secretSetting.Value,

		pollInterval: time.Duration(interval) * time.Second,

		target: target,
		ds:     sc.ds,
		stopCh: make(chan struct{}),
	}
	go sc.bsMonitor.Start()
	return nil
}

func (sc *SettingController) updateBackupstorePollInterval() (err error) {
	if sc.bsMonitor == nil {
		return nil
	}

	defer func() {
		err = errors.Wrapf(err, "failed to sync backup target")
	}()

	interval, err := sc.ds.GetSettingAsInt(types.SettingNameBackupstorePollInterval)
	if err != nil {
		return err
	}

	if sc.bsMonitor.pollInterval == time.Duration(interval)*time.Second {
		return nil
	}

	sc.bsMonitor.Stop()

	sc.bsMonitor.pollInterval = time.Duration(interval) * time.Second
	sc.bsMonitor.stopCh = make(chan struct{})

	go sc.bsMonitor.Start()
	return nil
}

func (sc *SettingController) updateTaintToleration() error {
	setting, err := sc.ds.GetSetting(types.SettingNameTaintToleration)
	if err != nil {
		return err
	}
	newTolerations := setting.Value
	newTolerationsList, err := types.UnmarshalTolerations(newTolerations)
	if err != nil {
		return err
	}
	newTolerationsMap := util.TolerationListToMap(newTolerationsList)

	daemonsetList, err := sc.ds.ListDaemonSetWithLabels(types.GetBaseLabelsForSystemManagedComponent())
	if err != nil {
		return errors.Wrapf(err, "failed to list Longhorn daemonsets for toleration update")
	}

	deploymentList, err := sc.ds.ListDeploymentWithLabels(types.GetBaseLabelsForSystemManagedComponent())
	if err != nil {
		return errors.Wrapf(err, "failed to list Longhorn deployments for toleration update")
	}

	imPodList, err := sc.ds.ListInstanceManagerPods()
	if err != nil {
		return errors.Wrapf(err, "failed to list instance manager pods for toleration update")
	}

	smPodList, err := sc.ds.ListShareManagerPods()
	if err != nil {
		return errors.Wrapf(err, "failed to list share manager pods for toleration update")
	}

	bimPodList, err := sc.ds.ListBackingImageManagerPods()
	if err != nil {
		return errors.Wrapf(err, "failed to list backing image manager pods for toleration update")
	}

	for _, dp := range deploymentList {
		lastAppliedTolerationsList, err := getLastAppliedTolerationsList(dp)
		if err != nil {
			return err
		}
		if reflect.DeepEqual(util.TolerationListToMap(lastAppliedTolerationsList), newTolerationsMap) {
			continue
		}
		if err := sc.updateTolerationForDeployment(dp, lastAppliedTolerationsList, newTolerationsList); err != nil {
			return err
		}
	}

	for _, ds := range daemonsetList {
		lastAppliedTolerationsList, err := getLastAppliedTolerationsList(ds)
		if err != nil {
			return err
		}
		if reflect.DeepEqual(util.TolerationListToMap(lastAppliedTolerationsList), newTolerationsMap) {
			continue
		}
		if err := sc.updateTolerationForDaemonset(ds, lastAppliedTolerationsList, newTolerationsList); err != nil {
			return err
		}
	}

	pods := append(imPodList, smPodList...)
	pods = append(pods, bimPodList...)
	for _, pod := range pods {
		lastAppliedTolerations, err := getLastAppliedTolerationsList(pod)
		if err != nil {
			return err
		}
		if reflect.DeepEqual(util.TolerationListToMap(lastAppliedTolerations), newTolerationsMap) {
			continue
		}

		if err := sc.ds.DeletePod(pod.Name); err != nil {
			return err
		}
	}

	return nil
}

func (sc *SettingController) updateTolerationForDeployment(dp *appsv1.Deployment, lastAppliedTolerations, newTolerations []v1.Toleration) error {
	existingTolerationsMap := util.TolerationListToMap(dp.Spec.Template.Spec.Tolerations)
	lastAppliedTolerationsMap := util.TolerationListToMap(lastAppliedTolerations)
	newTolerationsMap := util.TolerationListToMap(newTolerations)
	dp.Spec.Template.Spec.Tolerations = getFinalTolerations(existingTolerationsMap, lastAppliedTolerationsMap, newTolerationsMap)
	newTolerationsByte, err := json.Marshal(newTolerations)
	if err != nil {
		return err
	}
	if err := util.SetAnnotation(dp, types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix), string(newTolerationsByte)); err != nil {
		return err
	}
	if _, err := sc.ds.UpdateDeployment(dp); err != nil {
		return err
	}
	return nil
}

func (sc *SettingController) updateTolerationForDaemonset(ds *appsv1.DaemonSet, lastAppliedTolerations, newTolerations []v1.Toleration) error {
	existingTolerationsMap := util.TolerationListToMap(ds.Spec.Template.Spec.Tolerations)
	lastAppliedTolerationsMap := util.TolerationListToMap(lastAppliedTolerations)
	newTolerationsMap := util.TolerationListToMap(newTolerations)
	ds.Spec.Template.Spec.Tolerations = getFinalTolerations(existingTolerationsMap, lastAppliedTolerationsMap, newTolerationsMap)
	newTolerationsByte, err := json.Marshal(newTolerations)
	if err != nil {
		return err
	}
	if err := util.SetAnnotation(ds, types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix), string(newTolerationsByte)); err != nil {
		return err
	}
	if _, err := sc.ds.UpdateDaemonSet(ds); err != nil {
		return err
	}
	return nil
}

func getLastAppliedTolerationsList(obj runtime.Object) ([]v1.Toleration, error) {
	lastAppliedTolerations, err := util.GetAnnotation(obj, types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix))
	if err != nil {
		return nil, err
	}

	if lastAppliedTolerations == "" {
		lastAppliedTolerations = "[]"
	}

	lastAppliedTolerationsList := []v1.Toleration{}
	if err := json.Unmarshal([]byte(lastAppliedTolerations), &lastAppliedTolerationsList); err != nil {
		return nil, err
	}

	return lastAppliedTolerationsList, nil
}

func (sc *SettingController) updatePriorityClass() error {
	setting, err := sc.ds.GetSetting(types.SettingNamePriorityClass)
	if err != nil {
		return err
	}
	newPriorityClass := setting.Value

	daemonsetList, err := sc.ds.ListDaemonSetWithLabels(types.GetBaseLabelsForSystemManagedComponent())
	if err != nil {
		return errors.Wrapf(err, "failed to list Longhorn daemonsets for priority class update")
	}

	deploymentList, err := sc.ds.ListDeploymentWithLabels(types.GetBaseLabelsForSystemManagedComponent())
	if err != nil {
		return errors.Wrapf(err, "failed to list Longhorn deployments for priority class update")
	}

	imPodList, err := sc.ds.ListInstanceManagerPods()
	if err != nil {
		return errors.Wrapf(err, "failed to list instance manager pods for priority class update")
	}

	smPodList, err := sc.ds.ListShareManagerPods()
	if err != nil {
		return errors.Wrapf(err, "failed to list share manager pods for priority class update")
	}

	bimPodList, err := sc.ds.ListBackingImageManagerPods()
	if err != nil {
		return errors.Wrapf(err, "failed to list backing image manager pods for priority class update")
	}

	for _, dp := range deploymentList {
		if dp.Spec.Template.Spec.PriorityClassName == newPriorityClass {
			continue
		}
		dp.Spec.Template.Spec.PriorityClassName = newPriorityClass
		if _, err := sc.ds.UpdateDeployment(dp); err != nil {
			return err
		}
	}
	for _, ds := range daemonsetList {
		if ds.Spec.Template.Spec.PriorityClassName == newPriorityClass {
			continue
		}
		ds.Spec.Template.Spec.PriorityClassName = newPriorityClass
		if _, err := sc.ds.UpdateDaemonSet(ds); err != nil {
			return err
		}
	}

	pods := append(imPodList, smPodList...)
	pods = append(pods, bimPodList...)
	for _, pod := range pods {
		if pod.Spec.PriorityClassName == newPriorityClass {
			continue
		}
		if err := sc.ds.DeletePod(pod.Name); err != nil {
			return err
		}
	}

	return nil
}

func getFinalTolerations(existingTolerations, lastAppliedTolerations, newTolerations map[string]v1.Toleration) []v1.Toleration {
	resultMap := make(map[string]v1.Toleration)

	for k, v := range existingTolerations {
		resultMap[k] = v
	}

	for k := range lastAppliedTolerations {
		delete(resultMap, k)
	}

	for k, v := range newTolerations {
		resultMap[k] = v
	}

	resultSlice := []v1.Toleration{}
	for _, v := range resultMap {
		resultSlice = append(resultSlice, v)
	}

	return resultSlice
}

func (sc *SettingController) updateNodeSelector() error {
	setting, err := sc.ds.GetSetting(types.SettingNameSystemManagedComponentsNodeSelector)
	if err != nil {
		return err
	}
	newNodeSelector, err := types.UnmarshalNodeSelector(setting.Value)
	if err != nil {
		return err
	}
	deploymentList, err := sc.ds.ListDeploymentWithLabels(types.GetBaseLabelsForSystemManagedComponent())
	if err != nil {
		return errors.Wrapf(err, "failed to list Longhorn deployments for node selector update")
	}
	daemonsetList, err := sc.ds.ListDaemonSetWithLabels(types.GetBaseLabelsForSystemManagedComponent())
	if err != nil {
		return errors.Wrapf(err, "failed to list Longhorn daemonsets for node selector update")
	}
	imPodList, err := sc.ds.ListInstanceManagerPods()
	if err != nil {
		return errors.Wrapf(err, "failed to list instance manager pods for node selector update")
	}
	smPodList, err := sc.ds.ListShareManagerPods()
	if err != nil {
		return errors.Wrapf(err, "failed to list share manager pods for node selector update")
	}
	bimPodList, err := sc.ds.ListBackingImageManagerPods()
	if err != nil {
		return errors.Wrapf(err, "failed to list backing image manager pods for node selector update")
	}
	for _, dp := range deploymentList {
		if dp.Spec.Template.Spec.NodeSelector == nil {
			if len(newNodeSelector) == 0 {
				continue
			}
		}
		if reflect.DeepEqual(dp.Spec.Template.Spec.NodeSelector, newNodeSelector) {
			continue
		}
		dp.Spec.Template.Spec.NodeSelector = newNodeSelector
		if _, err := sc.ds.UpdateDeployment(dp); err != nil {
			return err
		}
	}
	for _, ds := range daemonsetList {
		if ds.Spec.Template.Spec.NodeSelector == nil {
			if len(newNodeSelector) == 0 {
				continue
			}
		}
		if reflect.DeepEqual(ds.Spec.Template.Spec.NodeSelector, newNodeSelector) {
			continue
		}
		ds.Spec.Template.Spec.NodeSelector = newNodeSelector
		if _, err := sc.ds.UpdateDaemonSet(ds); err != nil {
			return err
		}
	}
	pods := append(imPodList, smPodList...)
	pods = append(pods, bimPodList...)
	for _, pod := range pods {
		if pod.Spec.NodeSelector == nil {
			if len(newNodeSelector) == 0 {
				continue
			}
		}
		if reflect.DeepEqual(pod.Spec.NodeSelector, newNodeSelector) {
			continue
		}
		if pod.DeletionTimestamp == nil {
			if err := sc.ds.DeletePod(pod.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (bm *BackupStoreMonitor) Start() {
	log := bm.logger.WithFields(logrus.Fields{
		"backupTarget": bm.target.URL,
		"pollInterval": bm.pollInterval,
	})
	if bm.pollInterval == time.Duration(0) {
		log.Info("Disabling backup store monitoring")
		return
	}
	log.Debug("Start backup store monitoring")
	defer func() {
		log.Debug("Stop backup store monitoring")
	}()

	// since this is run on each node, but we only need a single update
	// we pick a consistent random ready node for each poll run
	shouldProcess := func() (bool, error) {
		defaultEngineImage, err := bm.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
		if err != nil {
			return false, err
		}

		nodes, err := bm.ds.ListReadyNodesWithEngineImage(defaultEngineImage)
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
		interval := int64(bm.pollInterval.Seconds())
		midPoint := interval / 2
		timeIndex := int((time.Now().UTC().Unix() + midPoint) / interval)
		candidateIndex := timeIndex % len(candidates)
		responsibleNode := candidates[candidateIndex]
		return bm.controllerID == responsibleNode, nil
	}

	wait.Until(func() {
		if isResponsible, err := shouldProcess(); err != nil || !isResponsible {
			if err != nil {
				log.WithError(err).Warn("Failed to select node for backup store monitoring, will try again next poll interval")
			}
			return
		}

		bm.logger.Debug("Polling backup store for new volume backups")
		backupVolumeNames, err := bm.target.ListBackupVolumeNames()
		if err != nil {
			bm.logger.WithError(err).Warn("Failed to list backup volumes, cannot update volume names last backup")
		}

		backupVolumes := make(map[string]*engineapi.BackupVolume)
		for _, backupVolumeName := range backupVolumeNames {
			backupVolume, err := bm.target.InspectBackupVolumeMetadata(backupVolumeName)
			if err != nil {
				bm.logger.WithError(err).Warn("Failed to inspect backup volume metadata, cannot update volume names last backup")
				continue
			}
			backupVolumes[backupVolumeName] = backupVolume
		}
		manager.SyncVolumesLastBackupWithBackupVolumes(backupVolumes,
			bm.ds.ListVolumes, bm.ds.GetVolume, bm.ds.UpdateVolumeStatus)
		bm.logger.Debug("Refreshed all volumes last backup based on backup store information")
	}, bm.pollInterval, bm.stopCh)
}

func (bm *BackupStoreMonitor) Stop() {
	if bm.pollInterval != time.Duration(0) {
		bm.stopCh <- struct{}{}
	}
}

func (sc *SettingController) syncUpgradeChecker() error {
	upgradeCheckerEnabled, err := sc.ds.GetSettingAsBool(types.SettingNameUpgradeChecker)
	if err != nil {
		return err
	}

	latestLonghornVersion, err := sc.ds.GetSetting(types.SettingNameLatestLonghornVersion)
	if err != nil {
		return err
	}

	if upgradeCheckerEnabled == false {
		if latestLonghornVersion.Value != "" {
			latestLonghornVersion.Value = ""
			if _, err := sc.ds.UpdateSetting(latestLonghornVersion); err != nil {
				return err
			}
		}
		// reset timestamp so it can be triggered immediately when
		// setting changes next time
		sc.lastUpgradeCheckedTimestamp = time.Time{}
		return nil
	}

	now := time.Now()
	if now.Before(sc.lastUpgradeCheckedTimestamp.Add(upgradeCheckInterval)) {
		return nil
	}

	oldVersion := latestLonghornVersion.Value
	latestLonghornVersion.Value, err = sc.CheckLatestLonghornVersion()
	if err != nil {
		// non-critical error, don't retry
		sc.logger.WithError(err).Debug("Failed to check for the latest upgrade")
		return nil
	}

	sc.lastUpgradeCheckedTimestamp = now

	if latestLonghornVersion.Value != oldVersion {
		sc.logger.Infof("Latest Longhorn version is %v", latestLonghornVersion.Value)
		if _, err := sc.ds.UpdateSetting(latestLonghornVersion); err != nil {
			// non-critical error, don't retry
			sc.logger.WithError(err).Debug("Cannot update latest Longhorn version")
			return nil
		}
	}
	return nil
}

func (sc *SettingController) CheckLatestLonghornVersion() (string, error) {
	var (
		resp    CheckUpgradeResponse
		content bytes.Buffer
	)
	kubeVersion, err := sc.kubeClient.Discovery().ServerVersion()
	if err != nil {
		return "", errors.Wrap(err, "failed to get Kubernetes server version")
	}

	req := &CheckUpgradeRequest{
		LonghornVersion:   sc.version,
		KubernetesVersion: kubeVersion.GitVersion,
	}
	if err := json.NewEncoder(&content).Encode(req); err != nil {
		return "", err
	}
	r, err := http.Post(checkUpgradeURL, "application/json", &content)
	if err != nil {
		return "", err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		message := ""
		messageBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			message = err.Error()
		} else {
			message = string(messageBytes)
		}
		return "", fmt.Errorf("query return status code %v, message %v", r.StatusCode, message)
	}
	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		return "", err
	}

	latestVersion := ""
	for _, v := range resp.Versions {
		found := false
		for _, tag := range v.Tags {
			if tag == VersionTagLatest {
				found = true
				break
			}
		}
		if found {
			latestVersion = v.Name
			break
		}
	}
	if latestVersion == "" {
		return "", fmt.Errorf("cannot find latest version in response: %+v", resp)
	}

	return latestVersion, nil
}

func (sc *SettingController) enqueueSetting(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	sc.queue.AddRateLimited(key)
}

func (sc *SettingController) enqueueSettingForNode(obj interface{}) {
	if _, ok := obj.(*longhorn.Node); !ok {
		// Ignore deleted node
		return
	}

	sc.queue.AddRateLimited(sc.namespace + "/" + string(types.SettingNameGuaranteedEngineManagerCPU))
	sc.queue.AddRateLimited(sc.namespace + "/" + string(types.SettingNameGuaranteedReplicaManagerCPU))
}

func (sc *SettingController) updateInstanceManagerCPURequest() error {
	imPodList, err := sc.ds.ListInstanceManagerPods()
	if err != nil {
		return errors.Wrapf(err, "failed to list instance manager pods for toleration update")
	}
	imMap, err := sc.ds.ListInstanceManagers()
	if err != nil {
		return err
	}
	for _, imPod := range imPodList {
		if _, exists := imMap[imPod.Name]; !exists {
			continue
		}
		lhNode, err := sc.ds.GetNode(imPod.Spec.NodeName)
		if err != nil {
			return err
		}
		if types.GetCondition(lhNode.Status.Conditions, types.NodeConditionTypeReady).Status != types.ConditionStatusTrue {
			continue
		}

		resourceReq, err := GetInstanceManagerCPURequirement(sc.ds, imPod.Name)
		if err != nil {
			return err
		}
		podResourceReq := imPod.Spec.Containers[0].Resources
		if IsSameGuaranteedCPURequirement(resourceReq, &podResourceReq) {
			continue
		}
		sc.logger.Infof("Delete instance manager pod %v to refresh CPU request option", imPod.Name)
		if err := sc.ds.DeletePod(imPod.Name); err != nil {
			return err
		}
	}

	return nil
}

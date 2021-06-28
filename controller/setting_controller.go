package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	appsv1 "k8s.io/api/apps/v1"
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
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
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
		fallthrough
	case string(types.SettingNameBackupstorePollInterval):
		if err := sc.syncBackupTarget(); err != nil {
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
	pollInterval := time.Duration(interval) * time.Second

	backupTarget, err := sc.ds.GetBackupTarget(defaultBackupTargetName)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return err
		}

		// Create the default BackupTarget CR if not present
		backupTarget, err = sc.ds.CreateBackupTarget(&longhorn.BackupTarget{
			ObjectMeta: metav1.ObjectMeta{
				Name: defaultBackupTargetName,
			},
		})
	}
	// Update the default BackupTarget CR
	backupTarget.Spec.BackupTargetURL = targetSetting.Value
	backupTarget.Spec.CredentialSecret = secretSetting.Value
	backupTarget.Spec.PollInterval = metav1.Duration{Duration: pollInterval}
	if _, err = sc.ds.UpdateBackupTarget(backupTarget); !datastore.ErrorIsConflict(err) {
		return err
	}
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

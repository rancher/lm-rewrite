package controller

import (
	"fmt"

	"github.com/sirupsen/logrus"

	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

type EngineImageControllerTestCase struct {
	// For DaemonSet related tests
	node *longhorn.Node

	// For ref count check
	volume *longhorn.Volume
	engine *longhorn.Engine

	// For ref count related test
	upgradedEngineImage *longhorn.EngineImage

	// For expired engine image cleanup
	defaultEngineImage string

	currentEngineImage  *longhorn.EngineImage
	currentDaemonSet    *appv1.DaemonSet
	currentDaemonSetPod *corev1.Pod

	expectedEngineImage *longhorn.EngineImage
	expectedDaemonSet   *appv1.DaemonSet
}

func newTestEngineImageController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset) *EngineImageController {

	volumeInformer := lhInformerFactory.Longhorn().V1beta1().Volumes()
	engineInformer := lhInformerFactory.Longhorn().V1beta1().Engines()
	replicaInformer := lhInformerFactory.Longhorn().V1beta1().Replicas()
	engineImageInformer := lhInformerFactory.Longhorn().V1beta1().EngineImages()
	nodeInformer := lhInformerFactory.Longhorn().V1beta1().Nodes()
	settingInformer := lhInformerFactory.Longhorn().V1beta1().Settings()
	imInformer := lhInformerFactory.Longhorn().V1beta1().InstanceManagers()
	shareManagerInformer := lhInformerFactory.Longhorn().V1beta1().ShareManagers()
	backingImageInformer := lhInformerFactory.Longhorn().V1beta1().BackingImages()
	backingImageManagerInformer := lhInformerFactory.Longhorn().V1beta1().BackingImageManagers()
	backupTargetInformer := lhInformerFactory.Longhorn().V1beta1().BackupTargets()
	backupVolumeInformer := lhInformerFactory.Longhorn().V1beta1().BackupVolumes()
	backupInformer := lhInformerFactory.Longhorn().V1beta1().Backups()

	podInformer := kubeInformerFactory.Core().V1().Pods()
	persistentVolumeInformer := kubeInformerFactory.Core().V1().PersistentVolumes()
	persistentVolumeClaimInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()
	configMapInformer := kubeInformerFactory.Core().V1().ConfigMaps()
	secretInformer := kubeInformerFactory.Core().V1().Secrets()
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()
	daemonSetInformer := kubeInformerFactory.Apps().V1().DaemonSets()
	deploymentInformer := kubeInformerFactory.Apps().V1().Deployments()
	kubeNodeInformer := kubeInformerFactory.Core().V1().Nodes()
	priorityClassInformer := kubeInformerFactory.Scheduling().V1().PriorityClasses()
	csiDriverInformer := kubeInformerFactory.Storage().V1beta1().CSIDrivers()
	storageclassInformer := kubeInformerFactory.Storage().V1().StorageClasses()
	pdbInformer := kubeInformerFactory.Policy().V1beta1().PodDisruptionBudgets()
	serviceInformer := kubeInformerFactory.Core().V1().Services()

	// Skip the Lister check that occurs on creation of an Instance Manager.
	datastore.SkipListerCheck = true

	ds := datastore.NewDataStore(
		volumeInformer, engineInformer, replicaInformer,
		engineImageInformer, nodeInformer, settingInformer,
		imInformer, shareManagerInformer,
		backingImageInformer, backingImageManagerInformer,
		backupTargetInformer, backupVolumeInformer, backupInformer,
		lhClient,
		podInformer, cronJobInformer, daemonSetInformer,
		deploymentInformer, persistentVolumeInformer, persistentVolumeClaimInformer,
		configMapInformer, secretInformer, kubeNodeInformer, priorityClassInformer,
		csiDriverInformer, storageclassInformer,
		pdbInformer,
		serviceInformer,
		kubeClient, TestNamespace)

	logger := logrus.StandardLogger()
	ic := NewEngineImageController(
		logger,
		ds, scheme.Scheme,
		engineImageInformer, volumeInformer, daemonSetInformer,
		kubeClient, TestNamespace, TestNode1, TestServiceAccount)

	fakeRecorder := record.NewFakeRecorder(100)
	ic.eventRecorder = fakeRecorder

	ic.iStoreSynced = alwaysReady
	ic.vStoreSynced = alwaysReady
	ic.dsStoreSynced = alwaysReady

	ic.nowHandler = getTestNow
	ic.engineBinaryChecker = fakeEngineBinaryChecker
	ic.engineImageVersionUpdater = fakeEngineImageUpdater

	return ic
}

func getEngineImageControllerTestTemplate() *EngineImageControllerTestCase {
	tc := &EngineImageControllerTestCase{
		node:                newNode(TestNode1, TestNamespace, true, types.ConditionStatusTrue, ""),
		volume:              newVolume(TestVolumeName, 2),
		engine:              newEngine(TestEngineName, TestEngineImage, TestEngineManagerName, TestNode1, TestIP1, 0, true, types.InstanceStateRunning, types.InstanceStateRunning),
		upgradedEngineImage: newEngineImage(TestUpgradedEngineImage, types.EngineImageStateDeployed),

		defaultEngineImage: TestEngineImage,

		currentEngineImage: newEngineImage(TestEngineImage, types.EngineImageStateDeployed),
		currentDaemonSet:   newEngineImageDaemonSet(),
	}

	tc.volume.Status.CurrentNodeID = TestNode1
	tc.volume.Status.State = types.VolumeStateAttached
	tc.volume.Status.CurrentImage = TestEngineImage
	tc.currentEngineImage.Status.RefCount = 1

	return tc
}

func (tc *EngineImageControllerTestCase) copyCurrentToExpected() {
	if tc.currentEngineImage != nil {
		tc.expectedEngineImage = tc.currentEngineImage.DeepCopy()
	}
	if tc.currentDaemonSet != nil {
		tc.expectedDaemonSet = tc.currentDaemonSet.DeepCopy()
	}
}

func createEngineImageDaemonSetPod(name string, containerReadyStatus bool, nodeID string) *corev1.Pod {
	pod := newPod(
		&corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Ready: containerReadyStatus}}},
		name,
		TestNamespace,
		nodeID,
	)
	pod.Labels = types.GetEIDaemonSetLabelSelector(getTestEngineImageName())
	return pod
}

func generateEngineImageControllerTestCases() map[string]*EngineImageControllerTestCase {
	var tc *EngineImageControllerTestCase
	testCases := map[string]*EngineImageControllerTestCase{}

	// The TestNode2 is a non-existing node and is just used for node down test.
	tc = getEngineImageControllerTestTemplate()
	tc.currentEngineImage.Status.OwnerID = TestNode2
	tc.currentDaemonSetPod = createEngineImageDaemonSetPod(getTestEngineImageDaemonSetName()+TestPod1, true, TestNode1)
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.OwnerID = TestNode1
	tc.expectedEngineImage.Status.NodeDeploymentMap = map[string]bool{TestNode1: true}
	testCases["Engine image ownerID node is down"] = tc

	tc = getEngineImageControllerTestTemplate()
	tc.currentEngineImage.Status.State = types.EngineImageStateDeploying
	tc.currentDaemonSet = nil
	tc.copyCurrentToExpected()
	tc.expectedDaemonSet = newEngineImageDaemonSet()
	tc.expectedDaemonSet.Status.NumberAvailable = 0
	tc.expectedEngineImage.Status.Conditions[types.EngineImageConditionTypeReady] = types.Condition{
		Type:   types.EngineImageConditionTypeReady,
		Status: types.ConditionStatusFalse,
		Reason: types.EngineImageConditionTypeReadyReasonDaemonSet,
	}
	testCases["Engine image DaemonSet creation"] = tc

	// The DaemonSet is not ready hence the engine image will become state `deploying`
	tc = getEngineImageControllerTestTemplate()
	tc.currentDaemonSet.Status.NumberAvailable = 0
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.State = types.EngineImageStateDeploying
	tc.expectedEngineImage.Status.Conditions[types.EngineImageConditionTypeReady] = types.Condition{
		Type:   types.EngineImageConditionTypeReady,
		Status: types.ConditionStatusFalse,
		Reason: types.EngineImageConditionTypeReadyReasonDaemonSet,
	}
	tc.expectedEngineImage.Status.NodeDeploymentMap = map[string]bool{TestNode1: false}
	testCases["Engine Image DaemonSet pods are suddenly removed"] = tc

	// `ei.Status.refCount` should become 1 and `Status.NoRefSince` should be unset
	tc = getEngineImageControllerTestTemplate()
	tc.currentEngineImage.Status.RefCount = 0
	tc.currentEngineImage.Status.NoRefSince = getTestNow()
	tc.currentDaemonSetPod = createEngineImageDaemonSetPod(getTestEngineImageDaemonSetName()+TestPod1, true, TestNode1)
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.RefCount = 1
	tc.expectedEngineImage.Status.NoRefSince = ""
	tc.expectedEngineImage.Status.NodeDeploymentMap = map[string]bool{TestNode1: true}
	testCases["One volume starts to use the engine image"] = tc

	// No volume is using the current engine image.
	tc = getEngineImageControllerTestTemplate()
	tc.volume = nil
	tc.engine = nil
	tc.currentDaemonSetPod = createEngineImageDaemonSetPod(getTestEngineImageDaemonSetName()+TestPod1, true, TestNode1)
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.RefCount = 0
	tc.expectedEngineImage.Status.NoRefSince = getTestNow()
	tc.expectedEngineImage.Status.NodeDeploymentMap = map[string]bool{TestNode1: true}
	testCases["The default engine image won't be cleaned up even if there is no volume using it"] = tc

	tc = getEngineImageControllerTestTemplate()
	incompatibleVersion := types.EngineVersionDetails{
		Version:                 "ei.Spec.Image",
		GitCommit:               "unknown",
		BuildDate:               "unknown",
		CLIAPIVersion:           types.InvalidEngineVersion,
		CLIAPIMinVersion:        types.InvalidEngineVersion,
		ControllerAPIVersion:    types.InvalidEngineVersion,
		ControllerAPIMinVersion: types.InvalidEngineVersion,
		DataFormatVersion:       types.InvalidEngineVersion,
		DataFormatMinVersion:    types.InvalidEngineVersion,
	}
	tc.currentEngineImage.Status.EngineVersionDetails = incompatibleVersion
	tc.currentDaemonSetPod = createEngineImageDaemonSetPod(getTestEngineImageDaemonSetName()+TestPod1, true, TestNode1)
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.State = types.EngineImageStateIncompatible
	tc.expectedEngineImage.Status.Conditions[types.EngineImageConditionTypeReady] = types.Condition{
		Type:   types.EngineImageConditionTypeReady,
		Status: types.ConditionStatusFalse,
		Reason: types.EngineImageConditionTypeReadyReasonBinary,
	}
	tc.expectedEngineImage.Status.NodeDeploymentMap = map[string]bool{TestNode1: true}
	testCases["Incompatible engine image"] = tc

	return testCases
}

func (s *TestSuite) TestEngineImage(c *C) {
	testCases := generateEngineImageControllerTestCases()
	for name, tc := range testCases {
		var err error
		fmt.Printf("Testing engine image controller: %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		dsIndexer := kubeInformerFactory.Apps().V1().DaemonSets().Informer().GetIndexer()
		podIndexer := kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

		nodeIndexer := lhInformerFactory.Longhorn().V1beta1().Nodes().Informer().GetIndexer()
		settingIndexer := lhInformerFactory.Longhorn().V1beta1().Settings().Informer().GetIndexer()
		eiIndexer := lhInformerFactory.Longhorn().V1beta1().EngineImages().Informer().GetIndexer()
		vIndexer := lhInformerFactory.Longhorn().V1beta1().Volumes().Informer().GetIndexer()
		eIndexer := lhInformerFactory.Longhorn().V1beta1().Engines().Informer().GetIndexer()

		ic := newTestEngineImageController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient)

		setting, err := lhClient.LonghornV1beta1().Settings(TestNamespace).Create(newSetting(string(types.SettingNameDefaultEngineImage), tc.defaultEngineImage))
		c.Assert(err, IsNil)
		err = settingIndexer.Add(setting)
		c.Assert(err, IsNil)
		// For DaemonSet creation test
		setting, err = lhClient.LonghornV1beta1().Settings(TestNamespace).Create(newSetting(string(types.SettingNameTaintToleration), ""))
		c.Assert(err, IsNil)
		err = settingIndexer.Add(setting)
		c.Assert(err, IsNil)

		node, err := lhClient.LonghornV1beta1().Nodes(TestNamespace).Create(tc.node)
		c.Assert(err, IsNil)
		err = nodeIndexer.Add(node)
		c.Assert(err, IsNil)

		ei, err := lhClient.LonghornV1beta1().EngineImages(TestNamespace).Create(tc.currentEngineImage)
		c.Assert(err, IsNil)
		err = eiIndexer.Add(ei)
		c.Assert(err, IsNil)
		ei, err = lhClient.LonghornV1beta1().EngineImages(TestNamespace).Create(tc.upgradedEngineImage)
		c.Assert(err, IsNil)
		err = eiIndexer.Add(ei)
		c.Assert(err, IsNil)

		if tc.volume != nil {
			v, err := lhClient.LonghornV1beta1().Volumes(TestNamespace).Create(tc.volume)
			c.Assert(err, IsNil)
			err = vIndexer.Add(v)
			c.Assert(err, IsNil)
		}
		if tc.engine != nil {
			e, err := lhClient.LonghornV1beta1().Engines(TestNamespace).Create(tc.engine)
			c.Assert(err, IsNil)
			err = eIndexer.Add(e)
			c.Assert(err, IsNil)
		}
		if tc.currentDaemonSet != nil {
			ds, err := kubeClient.AppsV1().DaemonSets(TestNamespace).Create(tc.currentDaemonSet)
			c.Assert(err, IsNil)
			err = dsIndexer.Add(ds)
			c.Assert(err, IsNil)
		}
		if tc.currentDaemonSetPod != nil {
			p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(tc.currentDaemonSetPod)
			c.Assert(err, IsNil)
			err = podIndexer.Add(p)
			c.Assert(err, IsNil)
		}
		engineImageControllerKey := fmt.Sprintf("%s/%s", TestNamespace, getTestEngineImageName())
		err = ic.syncEngineImage(engineImageControllerKey)
		c.Assert(err, IsNil)

		ei, err = lhClient.LonghornV1beta1().EngineImages(TestNamespace).Get(getTestEngineImageName(), metav1.GetOptions{})
		if tc.expectedEngineImage == nil {
			c.Assert(datastore.ErrorIsNotFound(err), Equals, true)
		} else {
			c.Assert(err, IsNil)
			for k, v := range ei.Status.Conditions {
				v.LastTransitionTime = ""
				v.Message = ""
				ei.Status.Conditions[k] = v
			}
			c.Assert(ei.Status, DeepEquals, tc.expectedEngineImage.Status)
		}

		ds, err := kubeClient.AppsV1().DaemonSets(TestNamespace).Get(getTestEngineImageDaemonSetName(), metav1.GetOptions{})
		if tc.expectedDaemonSet == nil {
			c.Assert(datastore.ErrorIsNotFound(err), Equals, true)
		} else {
			c.Assert(err, IsNil)
			// For the DaemonSet created by the fake k8s client, the field `Status.DesiredNumberScheduled` won't be set automatically.
			ds.Status.DesiredNumberScheduled = 1
			c.Assert(ds.Status, DeepEquals, tc.expectedDaemonSet.Status)
		}
	}
}

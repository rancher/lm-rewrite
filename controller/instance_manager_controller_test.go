package controller

import (
	"fmt"

	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

type InstanceManagerTestCase struct {
	controllerID string
	nodeDown     bool
	nodeID       string

	currentPodStatus *v1.PodStatus
	currentOwnerID   string
	currentState     types.InstanceManagerState

	expectedPodCount int
	expectedStatus   types.InstanceManagerStatus
	expectedType     types.InstanceManagerType
}

func newTolerationSetting() *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNameTaintToleration),
		},
		Setting: types.Setting{
			Value: "",
		},
	}
}

func fakeInstanceManagerVersionUpdater(im *longhorn.InstanceManager) error {
	im.Status.APIMinVersion = engineapi.CurrentInstanceManagerAPIVersion
	im.Status.APIVersion = engineapi.CurrentInstanceManagerAPIVersion
	return nil
}

func newTestInstanceManagerController(lhInformerFactory lhinformerfactory.SharedInformerFactory,
	kubeInformerFactory informers.SharedInformerFactory, lhClient *lhfake.Clientset, kubeClient *fake.Clientset,
	controllerID string) *InstanceManagerController {

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
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()
	daemonSetInformer := kubeInformerFactory.Apps().V1().DaemonSets()
	deploymentInformer := kubeInformerFactory.Apps().V1().Deployments()
	persistentVolumeInformer := kubeInformerFactory.Core().V1().PersistentVolumes()
	persistentVolumeClaimInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()
	configMapInformer := kubeInformerFactory.Core().V1().ConfigMaps()
	secretInformer := kubeInformerFactory.Core().V1().Secrets()
	kubeNodeInformer := kubeInformerFactory.Core().V1().Nodes()
	priorityClassInformer := kubeInformerFactory.Scheduling().V1().PriorityClasses()
	csiDriverInformer := kubeInformerFactory.Storage().V1beta1().CSIDrivers()
	storageclassInformer := kubeInformerFactory.Storage().V1().StorageClasses()
	pdbInformer := kubeInformerFactory.Policy().V1beta1().PodDisruptionBudgets()
	serviceInformer := kubeInformerFactory.Core().V1().Services()

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
	imc := NewInstanceManagerController(logger,
		ds, scheme.Scheme, imInformer, podInformer, kubeNodeInformer, kubeClient, TestNamespace,
		controllerID, TestServiceAccount)
	fakeRecorder := record.NewFakeRecorder(100)
	imc.eventRecorder = fakeRecorder
	imc.imStoreSynced = alwaysReady
	imc.pStoreSynced = alwaysReady
	imc.versionUpdater = fakeInstanceManagerVersionUpdater

	return imc
}

func (s *TestSuite) TestSyncInstanceManager(c *C) {
	var err error

	testCases := map[string]InstanceManagerTestCase{
		"instance manager change ownership": {
			TestNode1, false, TestNode1,
			&v1.PodStatus{PodIP: TestIP1, Phase: v1.PodRunning},
			TestNode2, types.InstanceManagerStateUnknown, 1,
			types.InstanceManagerStatus{
				OwnerID:       TestNode1,
				CurrentState:  types.InstanceManagerStateRunning,
				IP:            TestIP1,
				APIMinVersion: 1,
				APIVersion:    1,
			},
			types.InstanceManagerTypeEngine,
		},
		"instance manager error then restart immediately": {
			TestNode1, false, TestNode1,
			&v1.PodStatus{PodIP: "", Phase: v1.PodFailed},
			TestNode1, types.InstanceManagerStateRunning, 1,
			types.InstanceManagerStatus{
				OwnerID:       TestNode1,
				CurrentState:  types.InstanceManagerStateStarting,
				APIMinVersion: 0,
				APIVersion:    0,
			},
			types.InstanceManagerTypeEngine,
		},
		"instance manager node down": {
			TestNode2, true, TestNode1,
			&v1.PodStatus{PodIP: TestIP1, Phase: v1.PodRunning},
			TestNode1, types.InstanceManagerStateRunning, 0,
			types.InstanceManagerStatus{
				OwnerID:       TestNode2,
				CurrentState:  types.InstanceManagerStateUnknown,
				APIMinVersion: 0,
				APIVersion:    0,
			},
			types.InstanceManagerTypeEngine,
		},
		"instance manager restarting after error": {
			TestNode1, false, TestNode1,
			&v1.PodStatus{PodIP: TestIP1, Phase: v1.PodRunning},
			TestNode1, types.InstanceManagerStateError, 1,
			types.InstanceManagerStatus{
				OwnerID:       TestNode1,
				CurrentState:  types.InstanceManagerStateStarting,
				APIMinVersion: 0,
				APIVersion:    0,
			},
			types.InstanceManagerTypeEngine,
		},
		"instance manager running": {
			TestNode1, false, TestNode1,
			&v1.PodStatus{PodIP: TestIP1, Phase: v1.PodRunning},
			TestNode1, types.InstanceManagerStateStarting, 1,
			types.InstanceManagerStatus{
				OwnerID:       TestNode1,
				CurrentState:  types.InstanceManagerStateRunning,
				IP:            TestIP1,
				APIMinVersion: 1,
				APIVersion:    1,
			},
			types.InstanceManagerTypeEngine,
		},
		"instance manager starting engine": {
			TestNode1, false, TestNode1,
			nil,
			TestNode1, types.InstanceManagerStateStopped, 1,
			types.InstanceManagerStatus{
				OwnerID:       TestNode1,
				CurrentState:  types.InstanceManagerStateStarting,
				APIMinVersion: 0,
				APIVersion:    0,
			},
			types.InstanceManagerTypeEngine,
		},
		"instance manager starting replica": {
			TestNode1, false, TestNode1,
			nil,
			TestNode1, types.InstanceManagerStateStopped, 1,
			types.InstanceManagerStatus{
				OwnerID:       TestNode1,
				CurrentState:  types.InstanceManagerStateStarting,
				APIMinVersion: 0,
				APIVersion:    0,
			},
			types.InstanceManagerTypeReplica,
		},
		"instance manager sync IP": {
			TestNode1, false, TestNode1,
			&v1.PodStatus{PodIP: TestIP2, Phase: v1.PodRunning},
			TestNode1, types.InstanceManagerStateRunning, 1,
			types.InstanceManagerStatus{
				OwnerID:       TestNode1,
				CurrentState:  types.InstanceManagerStateRunning,
				IP:            TestIP2,
				APIMinVersion: 1,
				APIVersion:    1,
			},
			types.InstanceManagerTypeReplica,
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())
		pIndexer := kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
		kubeNodeIndexer := kubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())
		imIndexer := lhInformerFactory.Longhorn().V1beta1().InstanceManagers().Informer().GetIndexer()
		sIndexer := lhInformerFactory.Longhorn().V1beta1().Settings().Informer().GetIndexer()
		lhNodeIndexer := lhInformerFactory.Longhorn().V1beta1().Nodes().Informer().GetIndexer()

		imc := newTestInstanceManagerController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient,
			tc.controllerID)

		// Controller logic depends on the existence of DefaultInstanceManagerImage Setting and Toleration Setting.
		tolerationSetting := newTolerationSetting()
		tolerationSetting, err = lhClient.LonghornV1beta1().Settings(TestNamespace).Create(tolerationSetting)
		c.Assert(err, IsNil)
		err = sIndexer.Add(tolerationSetting)
		c.Assert(err, IsNil)
		imImageSetting := newDefaultInstanceManagerImageSetting()
		imImageSetting, err = lhClient.LonghornV1beta1().Settings(TestNamespace).Create(imImageSetting)
		c.Assert(err, IsNil)
		err = sIndexer.Add(imImageSetting)
		c.Assert(err, IsNil)

		// Create Nodes for test. Conditionally add the first Node.
		if !tc.nodeDown {
			kubeNode1 := newKubernetesNode(TestNode1, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionTrue)
			err = kubeNodeIndexer.Add(kubeNode1)
			c.Assert(err, IsNil)
			_, err = kubeClient.CoreV1().Nodes().Create(kubeNode1)
			c.Assert(err, IsNil)

			lhNode1 := newNode(TestNode1, TestNamespace, true, types.ConditionStatusTrue, "")
			err = lhNodeIndexer.Add(lhNode1)
			c.Assert(err, IsNil)
			_, err = lhClient.LonghornV1beta1().Nodes(lhNode1.Namespace).Create(lhNode1)
			c.Assert(err, IsNil)
		}

		kubeNode2 := newKubernetesNode(TestNode2, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionTrue)
		err = kubeNodeIndexer.Add(kubeNode2)
		c.Assert(err, IsNil)
		_, err = kubeClient.CoreV1().Nodes().Create(kubeNode2)

		lhNode2 := newNode(TestNode2, TestNamespace, true, types.ConditionStatusTrue, "")
		err = lhNodeIndexer.Add(lhNode2)
		c.Assert(err, IsNil)
		_, err = lhClient.LonghornV1beta1().Nodes(lhNode2.Namespace).Create(lhNode2)
		c.Assert(err, IsNil)

		currentIP := ""
		if tc.currentState == types.InstanceManagerStateRunning || tc.currentState == types.InstanceManagerStateStarting {
			currentIP = TestIP1
		}
		im := newInstanceManager(TestInstanceManagerName1, tc.expectedType, tc.currentState, tc.currentOwnerID, tc.nodeID, currentIP, nil, false)
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
		_, err = lhClient.LonghornV1beta1().InstanceManagers(im.Namespace).Create(im)
		c.Assert(err, IsNil)

		if tc.currentPodStatus != nil {
			pod := newPod(tc.currentPodStatus, im.Name, im.Namespace, im.Spec.NodeID)
			err = pIndexer.Add(pod)
			c.Assert(err, IsNil)
			_, err = kubeClient.CoreV1().Pods(im.Namespace).Create(pod)
			c.Assert(err, IsNil)
		}

		err = imc.syncInstanceManager(getKey(im, c))
		c.Assert(err, IsNil)
		podList, err := kubeClient.CoreV1().Pods(im.Namespace).List(metav1.ListOptions{})
		c.Assert(err, IsNil)
		c.Assert(podList.Items, HasLen, tc.expectedPodCount)

		// Check the Pod that was created by the Instance Manager.
		if tc.currentPodStatus == nil {
			pod, err := kubeClient.CoreV1().Pods(im.Namespace).Get(im.Name, metav1.GetOptions{})
			c.Assert(err, IsNil)
			switch im.Spec.Type {
			case types.InstanceManagerTypeEngine:
				c.Assert(pod.Spec.Containers[0].Name, Equals, "engine-manager")
			case types.InstanceManagerTypeReplica:
				c.Assert(pod.Spec.Containers[0].Name, Equals, "replica-manager")
			}
		}

		// Skip checking imc.instanceManagerMonitorMap since the monitor doesn't work in the unit test.

		updatedIM, err := lhClient.LonghornV1beta1().InstanceManagers(im.Namespace).Get(im.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
		c.Assert(updatedIM.Status, DeepEquals, tc.expectedStatus)
	}
}

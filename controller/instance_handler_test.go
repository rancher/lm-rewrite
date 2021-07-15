package controller

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

const (
	NonExistingInstance = "nil-instance"
	ExistingInstance    = "existing-instance"
)

type MockInstanceManagerHandler struct{}

func (imh *MockInstanceManagerHandler) GetInstance(obj interface{}) (*types.InstanceProcess, error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	name := metadata.GetName()
	if strings.Contains(name, NonExistingInstance) {
		return nil, fmt.Errorf("cannot find")
	}
	return &types.InstanceProcess{}, nil
}

func (imh *MockInstanceManagerHandler) CreateInstance(obj interface{}) (*types.InstanceProcess, error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	name := metadata.GetName()
	if strings.Contains(name, NonExistingInstance) {
		return &types.InstanceProcess{}, nil
	}
	return nil, fmt.Errorf("already exists")
}

func (imh *MockInstanceManagerHandler) DeleteInstance(obj interface{}) error {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	name := metadata.GetName()
	if strings.Contains(name, NonExistingInstance) {
		return fmt.Errorf("cannot find")
	}
	return nil
}

func (imh *MockInstanceManagerHandler) LogInstance(obj interface{}) (*imapi.LogStream, error) {
	return nil, fmt.Errorf("LogInstance is not mocked")
}

func newEngine(name, currentImage, imName, nodeName, ip string, port int, started bool, currentState, desireState types.InstanceState) *longhorn.Engine {
	return &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
			Labels:    types.GetVolumeLabels(TestVolumeName),
		},
		Spec: types.EngineSpec{
			InstanceSpec: types.InstanceSpec{
				VolumeName:  TestVolumeName,
				VolumeSize:  TestVolumeSize,
				DesireState: desireState,
				NodeID:      nodeName,
				EngineImage: TestEngineImage,
			},
		},
		Status: types.EngineStatus{
			InstanceStatus: types.InstanceStatus{
				OwnerID:             TestOwnerID1,
				CurrentState:        currentState,
				CurrentImage:        currentImage,
				InstanceManagerName: imName,
				IP:                  ip,
				Port:                port,
				Started:             started,
			},
		},
	}
}

func (s *TestSuite) TestReconcileInstanceState(c *C) {
	testCases := map[string]struct {
		imType types.InstanceManagerType
		//instance manager setup
		instanceManager *longhorn.InstanceManager

		obj runtime.Object

		//status expectation
		expectedObj runtime.Object
		errorOut    bool
	}{
		// 1. keep stopped
		"engine keeps stopped": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopped),
			false,
		},
		// 2. desire state becomes running
		"engine desire state becomes running": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
			newEngine(NonExistingInstance, "", "", TestNode1, "", 0, false, types.InstanceStateStopped, types.InstanceStateRunning),
			newEngine(NonExistingInstance, "", "", TestNode1, "", 0, false, types.InstanceStateStopped, types.InstanceStateRunning),
			false,
		},
		// 3.1.1. become starting
		"engine becomes starting": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: {
						Spec: types.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateStarting,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, "", "", TestNode1, "", 0, false, types.InstanceStateStopped, types.InstanceStateRunning),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, TestNode1, "", 0, false, types.InstanceStateStarting, types.InstanceStateRunning),
			false,
		},
		// 3.1.3. become running from starting
		"engine becomes running from starting state": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: {
						Spec: types.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, TestNode1, "", 0, false, types.InstanceStateStarting, types.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateRunning),
			false,
		},
		// 3.2. become running from stopped
		"engine becomes running from stopped state": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: {
						Spec: types.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, "", "", TestNode1, "", 0, false, types.InstanceStateStopped, types.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateRunning),
			false,
		},
		// 4. keep running
		"engine keeps running": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: {
						Spec: types.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateRunning),
			false,
		},
		// 5. desire state becomes stopped
		"engine desire state becomes stopped": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: {
						Spec: types.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, "", TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateStopped),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, "", TestIP1, TestPort1, false, types.InstanceStateRunning, types.InstanceStateStopped),
			false,
		},
		// 6. wait for update
		"stopping engine waits for im update": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: {
						Spec: types.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, "", TestIP1, TestPort1, false, types.InstanceStateRunning, types.InstanceStateStopped),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, "", TestIP1, TestPort1, false, types.InstanceStateRunning, types.InstanceStateStopped),
			false,
		},
		// 7.1.1. become stopping
		"engine becomes stopping": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: {
						Spec: types.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateStopping,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, "", TestIP1, TestPort1, false, types.InstanceStateRunning, types.InstanceStateStopped),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, "", "", 0, false, types.InstanceStateStopping, types.InstanceStateStopped),
			false,
		},
		// 7.1.2. still stopping
		"engine is still stopping": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: {
						Spec: types.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateStopping,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, "", "", 0, false, types.InstanceStateStopping, types.InstanceStateStopped),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, "", "", 0, false, types.InstanceStateStopping, types.InstanceStateStopped),
			false,
		},
		// 7.1.3. become stopped from stopping
		"engine becomes stopped from stopping state": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
			newEngine(NonExistingInstance, "", TestInstanceManagerName1, "", "", 0, false, types.InstanceStateStopping, types.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopped),
			false,
		},
		// 7.2. become stopped from running
		"engine becomes stopped from running state": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName1, "", TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopped),
			false,
		},

		// corner case1: invalid desireState
		"engine gets invalid desire state": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopping),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopping),
			true,
		},
		// corner case2: the instance currentState is running but the related instance manager is being deleting
		"engine keeps running but instance manager is being deleting": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: {
						Spec: types.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, true),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateRunning),
			newEngine(NonExistingInstance, "", TestInstanceManagerName1, TestNode1, "", 0, true, types.InstanceStateError, types.InstanceStateRunning),
			false,
		},
		// corner case3: the instance is stopped and the related instance manager is being deleting
		"engine keeps stopped and instance manager is being deleting": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, true),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopped),
			false,
		},
		// corner case4: the instance currentState is running but the related instance manager is starting
		"engine keeps running but instance manager somehow is starting": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateStarting, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateRunning),
			newEngine(NonExistingInstance, "", TestInstanceManagerName1, TestNode1, "", 0, true, types.InstanceStateError, types.InstanceStateRunning),
			false,
		},
		// corner case5: the node is down
		"engine node is down": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateUnknown, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{
				ExistingInstance: {
					Spec: types.InstanceProcessSpec{
						Name: ExistingInstance,
					},
					Status: types.InstanceProcessStatus{
						State:     types.InstanceStateRunning,
						PortStart: TestPort1,
					},
				},
			}, false),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, "", 0, true, types.InstanceStateUnknown, types.InstanceStateRunning),
			false,
		},
		// corner case6: engine node is deleted
		"engine keeps running but the node is deleted": {
			types.InstanceManagerTypeEngine,
			nil,
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode2, TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateRunning),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode2, "", 0, true, types.InstanceStateUnknown, types.InstanceStateRunning),
			false,
		},
		// corner case7
		"engine desire state becomes stopped after the node is deleted": {
			types.InstanceManagerTypeEngine,
			nil,
			newEngine(NonExistingInstance, "", TestInstanceManagerName1, "", "", 0, true, types.InstanceStateUnknown, types.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopped),
			false,
		},
	}
	for name, tc := range testCases {
		fmt.Printf("testing instance handler: %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		eiIndexer := lhInformerFactory.Longhorn().V1beta1().EngineImages().Informer().GetIndexer()
		sIndexer := lhInformerFactory.Longhorn().V1beta1().Settings().Informer().GetIndexer()

		h := newTestInstanceHandler(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient)

		ei, err := lhClient.LonghornV1beta1().EngineImages(TestNamespace).Create(newEngineImage(TestEngineImage, types.EngineImageStateDeployed))
		c.Assert(err, IsNil)
		err = eiIndexer.Add(ei)
		c.Assert(err, IsNil)

		imImageSetting := newDefaultInstanceManagerImageSetting()
		imImageSetting, err = lhClient.LonghornV1beta1().Settings(TestNamespace).Create(imImageSetting)
		c.Assert(err, IsNil)
		err = sIndexer.Add(imImageSetting)
		c.Assert(err, IsNil)

		if tc.instanceManager != nil {
			im, err := lhClient.LonghornV1beta1().InstanceManagers(TestNamespace).Create(tc.instanceManager)
			c.Assert(err, IsNil)
			imIndexer := lhInformerFactory.Longhorn().V1beta1().InstanceManagers().Informer().GetIndexer()
			err = imIndexer.Add(im)
			c.Assert(err, IsNil)
		}

		node, err := lhClient.LonghornV1beta1().Nodes(TestNamespace).Create(newNode(TestNode1, TestNamespace, true, types.ConditionStatusTrue, ""))
		c.Assert(err, IsNil)
		nodeIndexer := lhInformerFactory.Longhorn().V1beta1().Nodes().Informer().GetIndexer()
		err = nodeIndexer.Add(node)
		c.Assert(err, IsNil)

		var spec *types.InstanceSpec
		var status *types.InstanceStatus
		if tc.imType == types.InstanceManagerTypeEngine {
			e, ok := tc.obj.(*longhorn.Engine)
			c.Assert(ok, Equals, true)
			spec = &e.Spec.InstanceSpec
			status = &e.Status.InstanceStatus
		} else {
			r, ok := tc.obj.(*longhorn.Replica)
			c.Assert(ok, Equals, true)
			spec = &r.Spec.InstanceSpec
			status = &r.Status.InstanceStatus
		}
		err = h.ReconcileInstanceState(tc.obj, spec, status)
		if tc.errorOut {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(tc.obj, DeepEquals, tc.expectedObj)
		}
	}
}

func newTestInstanceHandler(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset) *InstanceHandler {
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
	fakeRecorder := record.NewFakeRecorder(100)

	return NewInstanceHandler(ds, &MockInstanceManagerHandler{}, fakeRecorder)
}

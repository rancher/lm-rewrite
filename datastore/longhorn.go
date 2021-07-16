package datastore

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

const (
	// NameMaximumLength restricted the length due to Kubernetes name limitation
	NameMaximumLength = 40
)

var (
	longhornFinalizerKey = longhorn.SchemeGroupVersion.Group

	// VerificationRetryInterval is the wait time for each verification retries
	VerificationRetryInterval = 100 * time.Millisecond
	// VerificationRetryCounts is the number of times to retry for verification
	VerificationRetryCounts = 20
)

// InitSettings creates all Settings in SettingNameList if not already exist
func (s *DataStore) InitSettings() error {
	for _, sName := range types.SettingNameList {
		definition, ok := types.SettingDefinitions[sName]
		if !ok {
			return fmt.Errorf("BUG: setting %v is not defined", sName)
		}
		if _, err := s.sLister.Settings(s.namespace).Get(string(sName)); err != nil {
			if ErrorIsNotFound(err) {
				setting := &longhorn.Setting{
					ObjectMeta: metav1.ObjectMeta{
						Name: string(sName),
					},
					Setting: types.Setting{
						Value: definition.Default,
					},
				}
				if _, err := s.CreateSetting(setting); err != nil && !apierrors.IsAlreadyExists(err) {
					return err
				}
			} else {
				return err
			}
		}
	}
	return nil
}

// CreateSetting create a Longhorn Settings resource for the given setting and
// namespace
func (s *DataStore) CreateSetting(setting *longhorn.Setting) (*longhorn.Setting, error) {
	// GetSetting automatically create default entry, so no need to double check
	return s.lhClient.LonghornV1beta1().Settings(s.namespace).Create(setting)
}

// UpdateSetting updates the given Longhorn Settings and verifies update
func (s *DataStore) UpdateSetting(setting *longhorn.Setting) (*longhorn.Setting, error) {
	obj, err := s.lhClient.LonghornV1beta1().Settings(s.namespace).Update(setting)
	if err != nil {
		return nil, err
	}
	verifyUpdate(setting.Name, obj, func(name string) (runtime.Object, error) {
		return s.getSettingRO(name)
	})
	return obj, nil
}

// ValidateSetting checks the given setting value types and condition
func (s *DataStore) ValidateSetting(name, value string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to set settings with invalid %v", name)
	}()
	sName := types.SettingName(name)

	if err := types.ValidateInitSetting(name, value); err != nil {
		return err
	}

	switch sName {
	case types.SettingNameBackupTarget:
		vs, err := s.ListStandbyVolumesRO()
		if err != nil {
			return errors.Wrapf(err, "failed to list standby volume when modifying BackupTarget")
		}
		if len(vs) != 0 {
			standbyVolumeNames := make([]string, len(vs))
			for k := range vs {
				standbyVolumeNames = append(standbyVolumeNames, k)
			}
			return fmt.Errorf("cannot modify BackupTarget since there are existing standby volumes: %v", standbyVolumeNames)
		}
	case types.SettingNameTaintToleration:
		list, err := s.ListVolumesRO()
		if err != nil {
			return errors.Wrapf(err, "failed to list volumes before modifying toleration setting")
		}
		for _, v := range list {
			if v.Status.State != types.VolumeStateDetached {
				return fmt.Errorf("cannot modify toleration setting before all volumes are detached")
			}
		}
	case types.SettingNameSystemManagedComponentsNodeSelector:
		list, err := s.ListVolumesRO()
		if err != nil {
			return errors.Wrapf(err, "failed to list volumes before modifying node selector for managed components setting")
		}
		for _, v := range list {
			if v.Status.State != types.VolumeStateDetached {
				return fmt.Errorf("cannot modify node selector for managed components setting before all volumes are detached")
			}
		}
	case types.SettingNamePriorityClass:
		if value != "" {
			if _, err := s.GetPriorityClass(value); err != nil {
				return errors.Wrapf(err, "failed to get priority class %v before modifying priority class setting", value)
			}
		}
		list, err := s.ListVolumesRO()
		if err != nil {
			return errors.Wrapf(err, "failed to list volumes before modifying priority class setting")
		}
		for _, v := range list {
			if v.Status.State != types.VolumeStateDetached {
				return fmt.Errorf("cannot modify priority class setting before all volumes are detached")
			}
		}
	case types.SettingNameGuaranteedEngineManagerCPU:
		fallthrough
	case types.SettingNameGuaranteedReplicaManagerCPU:
		guaranteedEngineManagerCPU, err := s.GetSetting(types.SettingNameGuaranteedEngineManagerCPU)
		if err != nil {
			return err
		}
		if sName == types.SettingNameGuaranteedEngineManagerCPU {
			guaranteedEngineManagerCPU.Value = value
		}
		guaranteedReplicaManagerCPU, err := s.GetSetting(types.SettingNameGuaranteedReplicaManagerCPU)
		if err != nil {
			return err
		}
		if sName == types.SettingNameGuaranteedReplicaManagerCPU {
			guaranteedReplicaManagerCPU.Value = value
		}
		if err := types.ValidateCPUReservationValues(guaranteedEngineManagerCPU.Value, guaranteedReplicaManagerCPU.Value); err != nil {
			return err
		}
	}
	return nil
}

func (s *DataStore) getSettingRO(name string) (*longhorn.Setting, error) {
	return s.sLister.Settings(s.namespace).Get(name)
}

// GetSetting will automatically fill the non-existing setting if it's a valid
// setting name.
// The function will not return nil for *longhorn.Setting when error is nil
func (s *DataStore) GetSetting(sName types.SettingName) (*longhorn.Setting, error) {
	definition, ok := types.SettingDefinitions[sName]
	if !ok {
		return nil, fmt.Errorf("setting %v is not supported", sName)
	}
	resultRO, err := s.getSettingRO(string(sName))
	if err != nil {
		if !ErrorIsNotFound(err) {
			return nil, err
		}
		resultRO = &longhorn.Setting{
			ObjectMeta: metav1.ObjectMeta{
				Name: string(sName),
			},
			Setting: types.Setting{
				Value: definition.Default,
			},
		}
	}
	return resultRO.DeepCopy(), nil
}

// GetSettingValueExisted returns the value of the given setting name.
// Returns error if the setting does not exist or value is empty
func (s *DataStore) GetSettingValueExisted(sName types.SettingName) (string, error) {
	setting, err := s.GetSetting(sName)
	if err != nil {
		return "", err
	}
	if setting.Value == "" {
		return "", fmt.Errorf("setting %v is empty", sName)
	}
	return setting.Value, nil
}

// ListSettings lists all Settings in the namespace, and fill with default
// values of any missing entry
func (s *DataStore) ListSettings() (map[types.SettingName]*longhorn.Setting, error) {
	itemMap := make(map[types.SettingName]*longhorn.Setting)

	list, err := s.sLister.Settings(s.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	for _, itemRO := range list {
		// Cannot use cached object from lister
		settingField := types.SettingName(itemRO.Name)
		// Ignore the items that we don't recongize
		if _, ok := types.SettingDefinitions[settingField]; ok {
			itemMap[settingField] = itemRO.DeepCopy()
		}
	}
	// fill up the missing entries
	for sName, definition := range types.SettingDefinitions {
		if _, ok := itemMap[sName]; !ok {
			itemMap[sName] = &longhorn.Setting{
				ObjectMeta: metav1.ObjectMeta{
					Name: string(sName),
				},
				Setting: types.Setting{
					Value: definition.Default,
				},
			}
		}
	}
	return itemMap, nil
}

// GetCredentialFromSecret gets the Secret of the given name and namespace
// Returns a new credential object or error
func (s *DataStore) GetCredentialFromSecret(secretName string) (map[string]string, error) {
	secret, err := s.kubeClient.CoreV1().Secrets(s.namespace).Get(secretName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	credentialSecret := make(map[string]string)
	if secret.Data != nil {
		credentialSecret[types.AWSIAMRoleArn] = string(secret.Data[types.AWSIAMRoleArn])
		credentialSecret[types.AWSAccessKey] = string(secret.Data[types.AWSAccessKey])
		credentialSecret[types.AWSSecretKey] = string(secret.Data[types.AWSSecretKey])
		credentialSecret[types.AWSEndPoint] = string(secret.Data[types.AWSEndPoint])
		credentialSecret[types.AWSCert] = string(secret.Data[types.AWSCert])
		credentialSecret[types.HTTPSProxy] = string(secret.Data[types.HTTPSProxy])
		credentialSecret[types.HTTPProxy] = string(secret.Data[types.HTTPProxy])
		credentialSecret[types.NOProxy] = string(secret.Data[types.NOProxy])
		credentialSecret[types.VirtualHostedStyle] = string(secret.Data[types.VirtualHostedStyle])
	}
	return credentialSecret, nil
}

func checkVolume(v *longhorn.Volume) error {
	size, err := util.ConvertSize(v.Spec.Size)
	if err != nil {
		return err
	}
	if v.Name == "" || size == 0 || v.Spec.NumberOfReplicas == 0 {
		return fmt.Errorf("BUG: missing required field %+v", v)
	}
	errs := validation.IsDNS1123Label(v.Name)
	if len(errs) != 0 {
		return fmt.Errorf("Invalid volume name: %+v", errs)
	}
	if len(v.Name) > NameMaximumLength {
		return fmt.Errorf("Volume name is too long %v, must be less than %v characters",
			v.Name, NameMaximumLength)
	}
	return nil
}

func tagVolumeLabel(volumeName string, obj runtime.Object) error {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	labels := metadata.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	for k, v := range types.GetVolumeLabels(volumeName) {
		labels[k] = v
	}
	metadata.SetLabels(labels)
	return nil
}

func fixupMetadata(volumeName string, obj runtime.Object) error {
	if err := tagVolumeLabel(volumeName, obj); err != nil {
		return err
	}
	if err := util.AddFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	return nil
}

func getVolumeSelector(volumeName string) (labels.Selector, error) {
	return metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: types.GetVolumeLabels(volumeName),
	})
}

// GetOwnerReferencesForVolume returns a list contains single OwnerReference for the
// given volume UID and name
func GetOwnerReferencesForVolume(v *longhorn.Volume) []metav1.OwnerReference {
	return []metav1.OwnerReference{
		{
			APIVersion: longhorn.SchemeGroupVersion.String(),
			Kind:       types.LonghornKindVolume,
			UID:        v.UID,
			Name:       v.Name,
		},
	}
}

// CreateVolume creates a Longhorn Volume resource and verifies creation
func (s *DataStore) CreateVolume(v *longhorn.Volume) (*longhorn.Volume, error) {
	if err := checkVolume(v); err != nil {
		return nil, err
	}
	if err := fixupMetadata(v.Name, v); err != nil {
		return nil, err
	}
	ret, err := s.lhClient.LonghornV1beta1().Volumes(s.namespace).Create(v)
	if err != nil {
		return nil, err
	}
	if SkipListerCheck {
		return ret, nil
	}

	obj, err := verifyCreation(v.Name, "volume", func(name string) (runtime.Object, error) {
		return s.getVolumeRO(name)
	})
	if err != nil {
		return nil, err
	}
	ret, ok := obj.(*longhorn.Volume)
	if !ok {
		return nil, fmt.Errorf("BUG: datastore: verifyCreation returned wrong type for volume")
	}

	return ret.DeepCopy(), nil
}

// UpdateVolume updates Longhorn Volume and verifies update
func (s *DataStore) UpdateVolume(v *longhorn.Volume) (*longhorn.Volume, error) {
	if err := checkVolume(v); err != nil {
		return nil, err
	}
	if err := fixupMetadata(v.Name, v); err != nil {
		return nil, err
	}

	obj, err := s.lhClient.LonghornV1beta1().Volumes(s.namespace).Update(v)
	if err != nil {
		return nil, err
	}
	verifyUpdate(v.Name, obj, func(name string) (runtime.Object, error) {
		return s.getVolumeRO(name)
	})
	return obj, nil
}

// UpdateVolumeStatus updates Longhorn Volume status and verifies update
func (s *DataStore) UpdateVolumeStatus(v *longhorn.Volume) (*longhorn.Volume, error) {
	obj, err := s.lhClient.LonghornV1beta1().Volumes(s.namespace).UpdateStatus(v)
	if err != nil {
		return nil, err
	}
	verifyUpdate(v.Name, obj, func(name string) (runtime.Object, error) {
		return s.getVolumeRO(name)
	})
	return obj, nil
}

// DeleteVolume won't result in immediately deletion since finalizer was set by
// default
func (s *DataStore) DeleteVolume(name string) error {
	return s.lhClient.LonghornV1beta1().Volumes(s.namespace).Delete(name, &metav1.DeleteOptions{})
}

// RemoveFinalizerForVolume will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForVolume(obj *longhorn.Volume) error {
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err := s.lhClient.LonghornV1beta1().Volumes(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for volume %v", obj.Name)
	}
	return nil
}

func (s *DataStore) getVolumeRO(name string) (*longhorn.Volume, error) {
	return s.vLister.Volumes(s.namespace).Get(name)
}

// GetVolume returns a new volume object for the given namespace and name
func (s *DataStore) GetVolume(name string) (*longhorn.Volume, error) {
	resultRO, err := s.vLister.Volumes(s.namespace).Get(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

// ListVolumesRO returns a list of all Volumes for the given namespace
func (s *DataStore) ListVolumesRO() ([]*longhorn.Volume, error) {
	return s.vLister.Volumes(s.namespace).List(labels.Everything())
}

// ListVolumes returns an object contains all Volume
func (s *DataStore) ListVolumes() (map[string]*longhorn.Volume, error) {
	itemMap := make(map[string]*longhorn.Volume)

	list, err := s.ListVolumesRO()
	if err != nil {
		return nil, err
	}

	for _, itemRO := range list {
		// Cannot use cached object from lister
		itemMap[itemRO.Name] = itemRO.DeepCopy()
	}
	return itemMap, nil
}

// ListStandbyVolumesRO returns a single object contains all standby Volumes
func (s *DataStore) ListStandbyVolumesRO() (map[string]*longhorn.Volume, error) {
	itemMap := make(map[string]*longhorn.Volume)

	list, err := s.ListVolumesRO()
	if err != nil {
		return nil, err
	}

	for _, itemRO := range list {
		if itemRO.Spec.Standby {
			itemMap[itemRO.Name] = itemRO
		}
	}
	return itemMap, nil
}

func checkEngine(engine *longhorn.Engine) error {
	if engine.Name == "" || engine.Spec.VolumeName == "" {
		return fmt.Errorf("BUG: missing required field %+v", engine)
	}
	return nil
}

// CreateEngine creates a Longhorn Engine resource and verifies creation
func (s *DataStore) CreateEngine(e *longhorn.Engine) (*longhorn.Engine, error) {
	if err := checkEngine(e); err != nil {
		return nil, err
	}
	if err := fixupMetadata(e.Spec.VolumeName, e); err != nil {
		return nil, err
	}
	if err := tagNodeLabel(e.Spec.NodeID, e); err != nil {
		return nil, err
	}

	ret, err := s.lhClient.LonghornV1beta1().Engines(s.namespace).Create(e)
	if err != nil {
		return nil, err
	}
	if SkipListerCheck {
		return ret, nil
	}

	obj, err := verifyCreation(e.Name, "engine", func(name string) (runtime.Object, error) {
		return s.getEngineRO(name)
	})
	if err != nil {
		return nil, err
	}
	ret, ok := obj.(*longhorn.Engine)
	if !ok {
		return nil, fmt.Errorf("BUG: datastore: verifyCreation returned wrong type for engine")
	}

	return ret.DeepCopy(), nil
}

// UpdateEngine updates Longhorn Engine and verifies update
func (s *DataStore) UpdateEngine(e *longhorn.Engine) (*longhorn.Engine, error) {
	if err := checkEngine(e); err != nil {
		return nil, err
	}
	if err := fixupMetadata(e.Spec.VolumeName, e); err != nil {
		return nil, err
	}
	if err := tagNodeLabel(e.Spec.NodeID, e); err != nil {
		return nil, err
	}

	obj, err := s.lhClient.LonghornV1beta1().Engines(s.namespace).Update(e)
	if err != nil {
		return nil, err
	}
	verifyUpdate(e.Name, obj, func(name string) (runtime.Object, error) {
		return s.getEngineRO(name)
	})
	return obj, nil
}

// UpdateEngineStatus updates Longhorn Engine status and verifies update
func (s *DataStore) UpdateEngineStatus(e *longhorn.Engine) (*longhorn.Engine, error) {
	obj, err := s.lhClient.LonghornV1beta1().Engines(s.namespace).UpdateStatus(e)
	if err != nil {
		return nil, err
	}
	verifyUpdate(e.Name, obj, func(name string) (runtime.Object, error) {
		return s.getEngineRO(name)
	})
	return obj, nil
}

// DeleteEngine won't result in immediately deletion since finalizer was set by
// default
func (s *DataStore) DeleteEngine(name string) error {
	return s.lhClient.LonghornV1beta1().Engines(s.namespace).Delete(name, &metav1.DeleteOptions{})
}

// RemoveFinalizerForEngine will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForEngine(obj *longhorn.Engine) error {
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err := s.lhClient.LonghornV1beta1().Engines(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for engine %v", obj.Name)
	}
	return nil
}

func (s *DataStore) getEngineRO(name string) (*longhorn.Engine, error) {
	return s.eLister.Engines(s.namespace).Get(name)
}

func (s *DataStore) getEngine(name string) (*longhorn.Engine, error) {
	resultRO, err := s.getEngineRO(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

// GetEngine returns the Engine for the given name and namespace
func (s *DataStore) GetEngine(name string) (*longhorn.Engine, error) {
	return s.eLister.Engines(s.namespace).Get(name)
}

func (s *DataStore) listEngines(selector labels.Selector) (map[string]*longhorn.Engine, error) {
	list, err := s.eLister.Engines(s.namespace).List(selector)
	if err != nil {
		return nil, err
	}
	engines := map[string]*longhorn.Engine{}
	for _, e := range list {
		// Cannot use cached object from lister
		engines[e.Name] = e.DeepCopy()
	}
	return engines, nil
}

// ListEngines returns an object contains all Engine for the given namespace
func (s *DataStore) ListEngines() (map[string]*longhorn.Engine, error) {
	return s.listEngines(labels.Everything())
}

// ListEnginesRO returns a list of all Engine for the given namespace
func (s *DataStore) ListEnginesRO() ([]*longhorn.Engine, error) {
	return s.eLister.Engines(s.namespace).List(labels.Everything())
}

// ListVolumeEngines returns an object contains all Engines with the given
// LonghornLabelVolume name and namespace
func (s *DataStore) ListVolumeEngines(volumeName string) (map[string]*longhorn.Engine, error) {
	selector, err := getVolumeSelector(volumeName)
	if err != nil {
		return nil, err
	}
	return s.listEngines(selector)
}

func checkReplica(r *longhorn.Replica) error {
	if r.Name == "" || r.Spec.VolumeName == "" {
		return fmt.Errorf("BUG: missing required field %+v", r)
	}
	if (r.Status.CurrentState == types.InstanceStateRunning) != (r.Status.IP != "") {
		return fmt.Errorf("BUG: instance state and IP wasn't in sync %+v", r)
	}
	return nil
}

// CreateReplica creates a Longhorn Replica resource and verifies creation
func (s *DataStore) CreateReplica(r *longhorn.Replica) (*longhorn.Replica, error) {
	if err := checkReplica(r); err != nil {
		return nil, err
	}
	if err := fixupMetadata(r.Spec.VolumeName, r); err != nil {
		return nil, err
	}
	if err := tagNodeLabel(r.Spec.NodeID, r); err != nil {
		return nil, err
	}
	if err := tagDiskUUIDLabel(r.Spec.DiskID, r); err != nil {
		return nil, err
	}
	if err := tagBackingImageLabel(r.Spec.BackingImage, r); err != nil {
		return nil, err
	}

	ret, err := s.lhClient.LonghornV1beta1().Replicas(s.namespace).Create(r)
	if err != nil {
		return nil, err
	}
	if SkipListerCheck {
		return ret, nil
	}

	obj, err := verifyCreation(r.Name, "replica", func(name string) (runtime.Object, error) {
		return s.getReplicaRO(name)
	})
	if err != nil {
		return nil, err
	}
	ret, ok := obj.(*longhorn.Replica)
	if !ok {
		return nil, fmt.Errorf("BUG: datastore: verifyCreation returned wrong type for replica")
	}

	return ret.DeepCopy(), nil
}

// UpdateReplica updates Replica and verifies update
func (s *DataStore) UpdateReplica(r *longhorn.Replica) (*longhorn.Replica, error) {
	if err := checkReplica(r); err != nil {
		return nil, err
	}
	if err := fixupMetadata(r.Spec.VolumeName, r); err != nil {
		return nil, err
	}
	if err := tagNodeLabel(r.Spec.NodeID, r); err != nil {
		return nil, err
	}
	if err := tagDiskUUIDLabel(r.Spec.DiskID, r); err != nil {
		return nil, err
	}
	if err := tagBackingImageLabel(r.Spec.BackingImage, r); err != nil {
		return nil, err
	}

	obj, err := s.lhClient.LonghornV1beta1().Replicas(s.namespace).Update(r)
	if err != nil {
		return nil, err
	}
	verifyUpdate(r.Name, obj, func(name string) (runtime.Object, error) {
		return s.getReplicaRO(name)
	})
	return obj, nil
}

// UpdateReplicaStatus updates Replica status and verifies update
func (s *DataStore) UpdateReplicaStatus(r *longhorn.Replica) (*longhorn.Replica, error) {
	if err := checkReplica(r); err != nil {
		return nil, err
	}

	obj, err := s.lhClient.LonghornV1beta1().Replicas(s.namespace).UpdateStatus(r)
	if err != nil {
		return nil, err
	}
	verifyUpdate(r.Name, obj, func(name string) (runtime.Object, error) {
		return s.getReplicaRO(name)
	})
	return obj, nil
}

// DeleteReplica won't result in immediately deletion since finalizer was set
// by default
func (s *DataStore) DeleteReplica(name string) error {
	return s.lhClient.LonghornV1beta1().Replicas(s.namespace).Delete(name, &metav1.DeleteOptions{})
}

// RemoveFinalizerForReplica will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForReplica(obj *longhorn.Replica) error {
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err := s.lhClient.LonghornV1beta1().Replicas(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for replica %v", obj.Name)
	}
	return nil
}

// GetReplica gets Replica for the given name and namespace and returns
// a new Replica object
func (s *DataStore) GetReplica(name string) (*longhorn.Replica, error) {
	result, err := s.getReplica(name)
	if err != nil {
		return nil, err
	}
	return s.fixupReplica(result)
}

func (s *DataStore) getReplicaRO(name string) (*longhorn.Replica, error) {
	return s.rLister.Replicas(s.namespace).Get(name)
}

func (s *DataStore) getReplica(name string) (*longhorn.Replica, error) {
	resultRO, err := s.rLister.Replicas(s.namespace).Get(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

func (s *DataStore) listReplicas(selector labels.Selector) (map[string]*longhorn.Replica, error) {
	list, err := s.rLister.Replicas(s.namespace).List(selector)
	if err != nil {
		return nil, err
	}

	itemMap := map[string]*longhorn.Replica{}
	for _, itemRO := range list {
		// Cannot use cached object from lister
		itemMap[itemRO.Name], err = s.fixupReplica(itemRO.DeepCopy())
		if err != nil {
			return nil, err
		}
	}
	return itemMap, nil
}

// ListReplicas returns an object contains all Replicas for the given namespace
func (s *DataStore) ListReplicas() (map[string]*longhorn.Replica, error) {
	return s.listReplicas(labels.Everything())
}

// ListVolumeReplicas returns an object contains all Replica with the given
// LonghornLabelVolume name and namespace
func (s *DataStore) ListVolumeReplicas(volumeName string) (map[string]*longhorn.Replica, error) {
	selector, err := getVolumeSelector(volumeName)
	if err != nil {
		return nil, err
	}
	return s.listReplicas(selector)
}

func (s *DataStore) fixupReplica(replica *longhorn.Replica) (*longhorn.Replica, error) {
	return replica, nil
}

// ReplicaAddressToReplicaName will directly return the address if the format
// is invalid or the replica is not found.
func ReplicaAddressToReplicaName(address string, rs []*longhorn.Replica) string {
	addressComponents := strings.Split(strings.TrimPrefix(address, "tcp://"), ":")
	// The address format should be `<IP>:<Port>` after removing the prefix "tcp://".
	if len(addressComponents) != 2 {
		return address
	}
	for _, r := range rs {
		if addressComponents[0] == r.Status.IP && addressComponents[1] == strconv.Itoa(r.Status.Port) {
			return r.Name
		}
	}
	// Cannot find matching replica by the address, replica may be removed already. Use address instead.
	return address
}

// GetOwnerReferencesForEngineImage returns OwnerReference for the given
// Longhorn EngineImage name and UID
func GetOwnerReferencesForEngineImage(ei *longhorn.EngineImage) []metav1.OwnerReference {
	blockOwnerDeletion := true
	return []metav1.OwnerReference{
		{
			APIVersion:         longhorn.SchemeGroupVersion.String(),
			Kind:               types.LonghornKindEngineImage,
			Name:               ei.Name,
			UID:                ei.UID,
			BlockOwnerDeletion: &blockOwnerDeletion,
		},
	}
}

// CreateEngineImage creates a Longhorn EngineImage resource and verifies
// creation
func (s *DataStore) CreateEngineImage(img *longhorn.EngineImage) (*longhorn.EngineImage, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, img); err != nil {
		return nil, err
	}
	ret, err := s.lhClient.LonghornV1beta1().EngineImages(s.namespace).Create(img)
	if err != nil {
		return nil, err
	}
	if SkipListerCheck {
		return ret, nil
	}

	obj, err := verifyCreation(img.Name, "engine image", func(name string) (runtime.Object, error) {
		return s.getEngineImageRO(name)
	})
	if err != nil {
		return nil, err
	}
	ret, ok := obj.(*longhorn.EngineImage)
	if !ok {
		return nil, fmt.Errorf("BUG: datastore: verifyCreation returned wrong type for engine image")
	}

	return ret.DeepCopy(), nil
}

// UpdateEngineImage updates Longhorn EngineImage and verifies update
func (s *DataStore) UpdateEngineImage(img *longhorn.EngineImage) (*longhorn.EngineImage, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, img); err != nil {
		return nil, err
	}

	obj, err := s.lhClient.LonghornV1beta1().EngineImages(s.namespace).Update(img)
	if err != nil {
		return nil, err
	}
	verifyUpdate(img.Name, obj, func(name string) (runtime.Object, error) {
		return s.getEngineImageRO(name)
	})
	return obj, nil
}

// UpdateEngineImageStatus updates Longhorn EngineImage resource status and
// verifies update
func (s *DataStore) UpdateEngineImageStatus(img *longhorn.EngineImage) (*longhorn.EngineImage, error) {
	obj, err := s.lhClient.LonghornV1beta1().EngineImages(s.namespace).UpdateStatus(img)
	if err != nil {
		return nil, err
	}
	verifyUpdate(img.Name, obj, func(name string) (runtime.Object, error) {
		return s.getEngineImageRO(name)
	})
	return obj, nil
}

// DeleteEngineImage won't result in immediately deletion since finalizer was
// set by default
func (s *DataStore) DeleteEngineImage(name string) error {
	propagation := metav1.DeletePropagationForeground
	return s.lhClient.LonghornV1beta1().EngineImages(s.namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &propagation})
}

// RemoveFinalizerForEngineImage will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForEngineImage(obj *longhorn.EngineImage) error {
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err := s.lhClient.LonghornV1beta1().EngineImages(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for engine image %v", obj.Name)
	}
	return nil
}

func (s *DataStore) getEngineImageRO(name string) (*longhorn.EngineImage, error) {
	return s.iLister.EngineImages(s.namespace).Get(name)
}

func (s *DataStore) getEngineImage(name string) (*longhorn.EngineImage, error) {
	resultRO, err := s.getEngineImageRO(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

// GetEngineImage returns a new EngineImage object for the given name and
// namespace
func (s *DataStore) GetEngineImage(name string) (*longhorn.EngineImage, error) {
	result, err := s.getEngineImage(name)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ListEngineImages returns object includes all EngineImage in namespace
func (s *DataStore) ListEngineImages() (map[string]*longhorn.EngineImage, error) {
	itemMap := map[string]*longhorn.EngineImage{}

	list, err := s.iLister.EngineImages(s.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	for _, itemRO := range list {
		// Cannot use cached object from lister
		itemMap[itemRO.Name] = itemRO.DeepCopy()
	}
	return itemMap, nil
}

// CheckEngineImageReadiness return true if the engine IMAGE is deployed on all nodes in the NODES list
func (s *DataStore) CheckEngineImageReadiness(image string, nodes ...string) (isReady bool, err error) {
	if len(nodes) == 0 || (len(nodes) == 1 && nodes[0] == "") {
		return false, nil
	}
	nodesHaveEngineImage, err := s.ListNodesWithEngineImage(image)
	if err != nil {
		return false, errors.Wrapf(err, "failed CheckEngineImageReadiness for nodes %v", nodes)
	}
	undeployedNodes := []string{}
	for _, node := range nodes {
		if _, ok := nodesHaveEngineImage[node]; !ok {
			undeployedNodes = append(undeployedNodes, node)
		}
	}
	if len(undeployedNodes) > 0 {
		logrus.Debugf("CheckEngineImageReadiness: nodes %v don't have the engine image %v", undeployedNodes, image)
		return false, nil
	}
	return true, nil
}

// CheckEngineImageReadyOnAtLeastOneVolumeReplica checks if the IMAGE is deployed on the NODEID and on at least one of the the volume's replicas
func (s *DataStore) CheckEngineImageReadyOnAtLeastOneVolumeReplica(image, volumeName, nodeID string) (bool, error) {
	replicas, err := s.ListVolumeReplicas(volumeName)
	if err != nil {
		return false, fmt.Errorf("cannot get replicas for volume %v: %w", volumeName, err)
	}

	isReady, err := s.CheckEngineImageReadiness(image, nodeID)
	if nodeID != "" && !isReady {
		return isReady, err
	}
	for _, r := range replicas {
		isReady, err := s.CheckEngineImageReadiness(image, r.Spec.NodeID)
		if err != nil || isReady {
			return isReady, err
		}
	}
	return false, nil
}

// CheckEngineImageReadyOnAllVolumeReplicas checks if the IMAGE is deployed on the NODEID as well as all the volume's replicas
func (s *DataStore) CheckEngineImageReadyOnAllVolumeReplicas(image, volumeName, nodeID string) (bool, error) {
	replicas, err := s.ListVolumeReplicas(volumeName)
	if err != nil {
		return false, fmt.Errorf("cannot get replicas for volume %v: %w", volumeName, err)
	}
	nodes := []string{}
	if nodeID != "" {
		nodes = append(nodes, nodeID)
	}
	for _, r := range replicas {
		if r.Spec.NodeID != "" {
			nodes = append(nodes, r.Spec.NodeID)
		}
	}
	return s.CheckEngineImageReadiness(image, nodes...)
}

// CreateBackingImage creates a Longhorn BackingImage resource and verifies
// creation
func (s *DataStore) CreateBackingImage(backingImage *longhorn.BackingImage) (*longhorn.BackingImage, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, backingImage); err != nil {
		return nil, err
	}
	ret, err := s.lhClient.LonghornV1beta1().BackingImages(s.namespace).Create(backingImage)
	if err != nil {
		return nil, err
	}
	if SkipListerCheck {
		return ret, nil
	}

	obj, err := verifyCreation(backingImage.Name, "backing image", func(name string) (runtime.Object, error) {
		return s.getBackingImageRO(name)
	})
	if err != nil {
		return nil, err
	}
	ret, ok := obj.(*longhorn.BackingImage)
	if !ok {
		return nil, fmt.Errorf("BUG: datastore: verifyCreation returned wrong type for backing image")
	}

	return ret.DeepCopy(), nil
}

// UpdateBackingImage updates Longhorn BackingImage and verifies update
func (s *DataStore) UpdateBackingImage(backingImage *longhorn.BackingImage) (*longhorn.BackingImage, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, backingImage); err != nil {
		return nil, err
	}

	obj, err := s.lhClient.LonghornV1beta1().BackingImages(s.namespace).Update(backingImage)
	if err != nil {
		return nil, err
	}
	verifyUpdate(backingImage.Name, obj, func(name string) (runtime.Object, error) {
		return s.getBackingImageRO(name)
	})
	return obj, nil
}

// UpdateBackingImageStatus updates Longhorn BackingImage resource status and
// verifies update
func (s *DataStore) UpdateBackingImageStatus(backingImage *longhorn.BackingImage) (*longhorn.BackingImage, error) {
	obj, err := s.lhClient.LonghornV1beta1().BackingImages(s.namespace).UpdateStatus(backingImage)
	if err != nil {
		return nil, err
	}
	verifyUpdate(backingImage.Name, obj, func(name string) (runtime.Object, error) {
		return s.getBackingImageRO(name)
	})
	return obj, nil
}

// DeleteBackingImage won't result in immediately deletion since finalizer was
// set by default
func (s *DataStore) DeleteBackingImage(name string) error {
	propagation := metav1.DeletePropagationForeground
	return s.lhClient.LonghornV1beta1().BackingImages(s.namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &propagation})
}

// RemoveFinalizerForBackingImage will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForBackingImage(obj *longhorn.BackingImage) error {
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err := s.lhClient.LonghornV1beta1().BackingImages(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for backing image %v", obj.Name)
	}
	return nil
}

func (s *DataStore) getBackingImageRO(name string) (*longhorn.BackingImage, error) {
	return s.biLister.BackingImages(s.namespace).Get(name)
}

// GetBackingImage returns a new BackingImage object for the given name and
// namespace
func (s *DataStore) GetBackingImage(name string) (*longhorn.BackingImage, error) {
	resultRO, err := s.getBackingImageRO(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

// ListBackingImages returns object includes all BackingImage in namespace
func (s *DataStore) ListBackingImages() (map[string]*longhorn.BackingImage, error) {
	itemMap := map[string]*longhorn.BackingImage{}

	list, err := s.biLister.BackingImages(s.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	for _, itemRO := range list {
		// Cannot use cached object from lister
		itemMap[itemRO.Name] = itemRO.DeepCopy()
	}
	return itemMap, nil
}

// GetOwnerReferencesForBackingImage returns OwnerReference for the given
// backing image name and UID
func GetOwnerReferencesForBackingImage(backingImage *longhorn.BackingImage) []metav1.OwnerReference {
	controller := true
	blockOwnerDeletion := true
	return []metav1.OwnerReference{
		{
			APIVersion:         longhorn.SchemeGroupVersion.String(),
			Kind:               types.LonghornKindBackingImage,
			Name:               backingImage.Name,
			UID:                backingImage.UID,
			Controller:         &controller,
			BlockOwnerDeletion: &blockOwnerDeletion,
		},
	}
}

// CreateBackingImageManager creates a Longhorn BackingImageManager resource and verifies
// creation
func (s *DataStore) CreateBackingImageManager(backingImageManager *longhorn.BackingImageManager) (*longhorn.BackingImageManager, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, backingImageManager); err != nil {
		return nil, err
	}
	if err := tagLonghornNodeLabel(backingImageManager.Spec.NodeID, backingImageManager); err != nil {
		return nil, err
	}
	if err := tagLonghornDiskUUIDLabel(backingImageManager.Spec.DiskUUID, backingImageManager); err != nil {
		return nil, err
	}

	ret, err := s.lhClient.LonghornV1beta1().BackingImageManagers(s.namespace).Create(backingImageManager)
	if err != nil {
		return nil, err
	}
	if SkipListerCheck {
		return ret, nil
	}

	obj, err := verifyCreation(backingImageManager.Name, "backing image manager", func(name string) (runtime.Object, error) {
		return s.getBackingImageManagerRO(name)
	})
	if err != nil {
		return nil, err
	}
	ret, ok := obj.(*longhorn.BackingImageManager)
	if !ok {
		return nil, fmt.Errorf("BUG: datastore: verifyCreation returned wrong type for backing image manager")
	}

	return ret.DeepCopy(), nil
}

// UpdateBackingImageManager updates Longhorn BackingImageManager and verifies update
func (s *DataStore) UpdateBackingImageManager(backingImageManager *longhorn.BackingImageManager) (*longhorn.BackingImageManager, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, backingImageManager); err != nil {
		return nil, err
	}
	if err := tagLonghornNodeLabel(backingImageManager.Spec.NodeID, backingImageManager); err != nil {
		return nil, err
	}
	if err := tagLonghornDiskUUIDLabel(backingImageManager.Spec.DiskUUID, backingImageManager); err != nil {
		return nil, err
	}

	obj, err := s.lhClient.LonghornV1beta1().BackingImageManagers(s.namespace).Update(backingImageManager)
	if err != nil {
		return nil, err
	}
	verifyUpdate(backingImageManager.Name, obj, func(name string) (runtime.Object, error) {
		return s.getBackingImageManagerRO(name)
	})
	return obj, nil
}

// UpdateBackingImageManagerStatus updates Longhorn BackingImageManager resource status and
// verifies update
func (s *DataStore) UpdateBackingImageManagerStatus(backingImageManager *longhorn.BackingImageManager) (*longhorn.BackingImageManager, error) {
	obj, err := s.lhClient.LonghornV1beta1().BackingImageManagers(s.namespace).UpdateStatus(backingImageManager)
	if err != nil {
		return nil, err
	}
	verifyUpdate(backingImageManager.Name, obj, func(name string) (runtime.Object, error) {
		return s.getBackingImageManagerRO(name)
	})
	return obj, nil
}

// DeleteBackingImageManager won't result in immediately deletion since finalizer was
// set by default
func (s *DataStore) DeleteBackingImageManager(name string) error {
	propagation := metav1.DeletePropagationForeground
	return s.lhClient.LonghornV1beta1().BackingImageManagers(s.namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &propagation})
}

// RemoveFinalizerForBackingImageManager will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForBackingImageManager(obj *longhorn.BackingImageManager) error {
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err := s.lhClient.LonghornV1beta1().BackingImageManagers(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for backing image manager %v", obj.Name)
	}
	return nil
}

func (s *DataStore) getBackingImageManagerRO(name string) (*longhorn.BackingImageManager, error) {
	return s.bimLister.BackingImageManagers(s.namespace).Get(name)
}

// GetBackingImageManager returns a new BackingImageManager object for the given name and
// namespace
func (s *DataStore) GetBackingImageManager(name string) (*longhorn.BackingImageManager, error) {
	resultRO, err := s.getBackingImageManagerRO(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

func (s *DataStore) listBackingImageManagers(selector labels.Selector) (map[string]*longhorn.BackingImageManager, error) {
	itemMap := map[string]*longhorn.BackingImageManager{}

	list, err := s.bimLister.BackingImageManagers(s.namespace).List(selector)
	if err != nil {
		return nil, err
	}

	for _, itemRO := range list {
		itemMap[itemRO.Name] = itemRO.DeepCopy()
	}
	return itemMap, nil
}

// ListBackingImageManagers returns object includes all BackingImageManager in namespace
func (s *DataStore) ListBackingImageManagers() (map[string]*longhorn.BackingImageManager, error) {
	return s.listBackingImageManagers(labels.Everything())
}

// ListBackingImageManagersByNode gets a list of BackingImageManager
// on a specific node with the given namespace.
func (s *DataStore) ListBackingImageManagersByNode(nodeName string) (map[string]*longhorn.BackingImageManager, error) {
	nodeSelector, err := getNodeSelector(nodeName)
	if err != nil {
		return nil, err
	}
	return s.listBackingImageManagers(nodeSelector)
}

// ListBackingImageManagersByDiskUUID gets a list of BackingImageManager
// in a specific disk with the given namespace.
func (s *DataStore) ListBackingImageManagersByDiskUUID(diskUUID string) (map[string]*longhorn.BackingImageManager, error) {
	diskSelector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{
			types.GetLonghornLabelKey(types.LonghornLabelDiskUUID): diskUUID,
		},
	})
	if err != nil {
		return nil, err
	}
	return s.listBackingImageManagers(diskSelector)
}

// ListDefaultBackingImageManagers gets a list of BackingImageManager
// using default image with the given namespace.
func (s *DataStore) ListDefaultBackingImageManagers() (map[string]*longhorn.BackingImageManager, error) {
	defaultImage, err := s.GetSettingValueExisted(types.SettingNameDefaultBackingImageManagerImage)
	if err != nil {
		return nil, err
	}

	itemMap := map[string]*longhorn.BackingImageManager{}
	list, err := s.bimLister.BackingImageManagers(s.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}
	for _, itemRO := range list {
		if itemRO.Spec.Image != defaultImage {
			continue
		}
		// Cannot use cached object from lister
		itemMap[itemRO.Name] = itemRO.DeepCopy()
	}
	return itemMap, nil
}

// GetOwnerReferencesForBackingImageManager returns OwnerReference for the given
// // backing image manager name and UID
func GetOwnerReferencesForBackingImageManager(backingImageManager *longhorn.BackingImageManager) []metav1.OwnerReference {
	controller := true
	blockOwnerDeletion := true
	return []metav1.OwnerReference{
		{
			APIVersion: longhorn.SchemeGroupVersion.String(),
			Kind:       types.LonghornKindBackingImageManager,
			Name:       backingImageManager.Name,
			UID:        backingImageManager.UID,
			// This field is needed so that `kubectl drain` can work without --force flag
			// See https://github.com/longhorn/longhorn/issues/1286#issuecomment-623283028 for more details
			Controller:         &controller,
			BlockOwnerDeletion: &blockOwnerDeletion,
		},
	}
}

// CreateBackingImageDataSource creates a Longhorn BackingImageDataSource resource and verifies
// creation
func (s *DataStore) CreateBackingImageDataSource(backingImageDataSource *longhorn.BackingImageDataSource) (*longhorn.BackingImageDataSource, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, backingImageDataSource); err != nil {
		return nil, err
	}
	ret, err := s.lhClient.LonghornV1beta1().BackingImageDataSources(s.namespace).Create(backingImageDataSource)
	if err != nil {
		return nil, err
	}
	if SkipListerCheck {
		return ret, nil
	}

	obj, err := verifyCreation(backingImageDataSource.Name, "backing image data source", func(name string) (runtime.Object, error) {
		return s.getBackingImageDataSourceRO(name)
	})
	if err != nil {
		return nil, err
	}
	ret, ok := obj.(*longhorn.BackingImageDataSource)
	if !ok {
		return nil, fmt.Errorf("BUG: datastore: verifyCreation returned wrong type for backing image data source")
	}

	return ret.DeepCopy(), nil
}

// UpdateBackingImageDataSource updates Longhorn BackingImageDataSource and verifies update
func (s *DataStore) UpdateBackingImageDataSource(backingImageDataSource *longhorn.BackingImageDataSource) (*longhorn.BackingImageDataSource, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, backingImageDataSource); err != nil {
		return nil, err
	}

	obj, err := s.lhClient.LonghornV1beta1().BackingImageDataSources(s.namespace).Update(backingImageDataSource)
	if err != nil {
		return nil, err
	}
	verifyUpdate(backingImageDataSource.Name, obj, func(name string) (runtime.Object, error) {
		return s.getBackingImageDataSourceRO(name)
	})
	return obj, nil
}

// UpdateBackingImageDataSourceStatus updates Longhorn BackingImageDataSource resource status and
// verifies update
func (s *DataStore) UpdateBackingImageDataSourceStatus(backingImageDataSource *longhorn.BackingImageDataSource) (*longhorn.BackingImageDataSource, error) {
	obj, err := s.lhClient.LonghornV1beta1().BackingImageDataSources(s.namespace).UpdateStatus(backingImageDataSource)
	if err != nil {
		return nil, err
	}
	verifyUpdate(backingImageDataSource.Name, obj, func(name string) (runtime.Object, error) {
		return s.getBackingImageDataSourceRO(name)
	})
	return obj, nil
}

// DeleteBackingImageDataSource won't result in immediately deletion since finalizer was
// set by default
func (s *DataStore) DeleteBackingImageDataSource(name string) error {
	propagation := metav1.DeletePropagationForeground
	return s.lhClient.LonghornV1beta1().BackingImageDataSources(s.namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &propagation})
}

// RemoveFinalizerForBackingImageDataSource will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForBackingImageDataSource(obj *longhorn.BackingImageDataSource) error {
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err := s.lhClient.LonghornV1beta1().BackingImageDataSources(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for backing image data source %v", obj.Name)
	}
	return nil
}

func (s *DataStore) getBackingImageDataSourceRO(name string) (*longhorn.BackingImageDataSource, error) {
	return s.bidsLister.BackingImageDataSources(s.namespace).Get(name)
}

// GetBackingImageDataSource returns a new BackingImageDataSource object for the given name and
// namespace
func (s *DataStore) GetBackingImageDataSource(name string) (*longhorn.BackingImageDataSource, error) {
	resultRO, err := s.getBackingImageDataSourceRO(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

// ListBackingImageDataSources returns object includes all BackingImageDataSource in namespace
func (s *DataStore) ListBackingImageDataSources() (map[string]*longhorn.BackingImageDataSource, error) {
	itemMap := map[string]*longhorn.BackingImageDataSource{}

	list, err := s.bidsLister.BackingImageDataSources(s.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	for _, itemRO := range list {
		// Cannot use cached object from lister
		itemMap[itemRO.Name] = itemRO.DeepCopy()
	}
	return itemMap, nil
}

// ListBackingImageDataSourcesByNode returns object includes all BackingImageDataSource in namespace
func (s *DataStore) ListBackingImageDataSourcesByNode(nodeName string) (map[string]*longhorn.BackingImageDataSource, error) {
	nodeSelector, err := getNodeSelector(nodeName)
	if err != nil {
		return nil, err
	}

	itemMap := map[string]*longhorn.BackingImageDataSource{}
	list, err := s.bidsLister.BackingImageDataSources(s.namespace).List(nodeSelector)
	if err != nil {
		return nil, err
	}

	for _, itemRO := range list {
		// Cannot use cached object from lister
		itemMap[itemRO.Name] = itemRO.DeepCopy()
	}
	return itemMap, nil
}

// GetOwnerReferencesForBackingImageDataSource returns OwnerReference for the given
// backing image data source name and UID
func GetOwnerReferencesForBackingImageDataSource(backingImageDataSource *longhorn.BackingImageDataSource) []metav1.OwnerReference {
	controller := true
	blockOwnerDeletion := true
	return []metav1.OwnerReference{
		{
			APIVersion:         longhorn.SchemeGroupVersion.String(),
			Kind:               types.LonghornKindBackingImageDataSource,
			Name:               backingImageDataSource.Name,
			UID:                backingImageDataSource.UID,
			Controller:         &controller,
			BlockOwnerDeletion: &blockOwnerDeletion,
		},
	}
}

// CreateNode creates a Longhorn Node resource and verifies creation
func (s *DataStore) CreateNode(node *longhorn.Node) (*longhorn.Node, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, node); err != nil {
		return nil, err
	}
	ret, err := s.lhClient.LonghornV1beta1().Nodes(s.namespace).Create(node)
	if err != nil {
		return nil, err
	}
	if SkipListerCheck {
		return ret, nil
	}

	obj, err := verifyCreation(node.Name, "node", func(name string) (runtime.Object, error) {
		return s.GetNodeRO(name)
	})
	if err != nil {
		return nil, err
	}
	ret, ok := obj.(*longhorn.Node)
	if !ok {
		return nil, fmt.Errorf("BUG: datastore: verifyCreation returned wrong type for node")
	}

	return ret.DeepCopy(), nil
}

// CreateDefaultNode will create the default Disk at the value of the
// DefaultDataPath Setting only if Create Default Disk on Labeled Nodes has
// been disabled.
func (s *DataStore) CreateDefaultNode(name string) (*longhorn.Node, error) {
	requireLabel, err := s.GetSettingAsBool(types.SettingNameCreateDefaultDiskLabeledNodes)
	if err != nil {
		return nil, err
	}
	node := &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: types.NodeSpec{
			Name:                     name,
			AllowScheduling:          true,
			EvictionRequested:        false,
			Tags:                     []string{},
			EngineManagerCPURequest:  0,
			ReplicaManagerCPURequest: 0,
		},
	}

	// For newly added node, the customized default disks will be applied only if the setting is enabled.
	if !requireLabel {
		// Note: this part wasn't moved to the controller is because
		// this will be done only once.
		// If user remove all the disks on the node, the default disk
		// will not be recreated automatically
		dataPath, err := s.GetSettingValueExisted(types.SettingNameDefaultDataPath)
		if err != nil {
			return nil, err
		}
		disks, err := types.CreateDefaultDisk(dataPath)
		if err != nil {
			return nil, err
		}
		node.Spec.Disks = disks
	}

	return s.CreateNode(node)
}

func (s *DataStore) GetNodeRO(name string) (*longhorn.Node, error) {
	return s.nLister.Nodes(s.namespace).Get(name)
}

// GetNode gets Longhorn Node for the given name and namespace
// Returns a new Node object
func (s *DataStore) GetNode(name string) (*longhorn.Node, error) {
	resultRO, err := s.GetNodeRO(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

// GetReadyDiskNode find the corresponding ready Longhorn Node for a given disk
// Returns a Node object and the disk name
func (s *DataStore) GetReadyDiskNode(diskUUID string) (*longhorn.Node, string, error) {
	nodes, err := s.ListReadyNodes()
	if err != nil {
		return nil, "", err
	}
	for _, node := range nodes {
		for diskName, diskStatus := range node.Status.DiskStatus {
			if diskStatus.DiskUUID == diskUUID {
				if types.GetCondition(diskStatus.Conditions, types.DiskConditionTypeReady).Status == types.ConditionStatusTrue {
					if _, exists := node.Spec.Disks[diskName]; exists {
						return node, diskName, nil
					}
				}
			}
		}
	}

	return nil, "", fmt.Errorf("cannot find the corresponding ready node and disk with disk UUID %v", diskUUID)
}

// UpdateNode updates Longhorn Node resource and verifies update
func (s *DataStore) UpdateNode(node *longhorn.Node) (*longhorn.Node, error) {
	obj, err := s.lhClient.LonghornV1beta1().Nodes(s.namespace).Update(node)
	if err != nil {
		return nil, err
	}
	verifyUpdate(node.Name, obj, func(name string) (runtime.Object, error) {
		return s.GetNodeRO(name)
	})
	return obj, nil
}

// UpdateNodeStatus updates Longhorn Node status and verifies update
func (s *DataStore) UpdateNodeStatus(node *longhorn.Node) (*longhorn.Node, error) {
	obj, err := s.lhClient.LonghornV1beta1().Nodes(s.namespace).UpdateStatus(node)
	if err != nil {
		return nil, err
	}
	verifyUpdate(node.Name, obj, func(name string) (runtime.Object, error) {
		return s.GetNodeRO(name)
	})
	return obj, nil
}

// ListNodes returns an object contains all Node for the namespace
func (s *DataStore) ListNodes() (map[string]*longhorn.Node, error) {
	itemMap := make(map[string]*longhorn.Node)

	nodeList, err := s.ListNodesRO()
	if err != nil {
		return nil, err
	}

	for _, node := range nodeList {
		// Cannot use cached object from lister
		itemMap[node.Name] = node.DeepCopy()
	}
	return itemMap, nil
}

// ListNodesRO returns a list of all Nodes for the given namespace,
// the list contains direct references to the internal cache objects and should not be mutated.
// Consider using this function when you can guarantee read only access and don't want the overhead of deep copies
func (s *DataStore) ListNodesRO() ([]*longhorn.Node, error) {
	return s.nLister.Nodes(s.namespace).List(labels.Everything())
}

func (s *DataStore) ListNodesWithEngineImage(image string) (map[string]*longhorn.Node, error) {
	nodes, err := s.ListNodes()
	if err != nil {
		return nil, err
	}
	eiName := types.GetEngineImageChecksumName(image)
	ei, err := s.GetEngineImage(eiName)
	if err != nil {
		return nil, fmt.Errorf("unable to get engine image %v: %v", image, err)
	}
	if ei.Status.State != types.EngineImageStateDeployed && ei.Status.State != types.EngineImageStateDeploying {
		return map[string]*longhorn.Node{}, nil
	}

	for nodeID := range nodes {
		if !ei.Status.NodeDeploymentMap[nodeID] {
			delete(nodes, nodeID)
		}
	}
	return nodes, nil
}

// filterNodes returns only the nodes where the passed predicate returns true
func filterNodes(nodes map[string]*longhorn.Node, predicate func(node *longhorn.Node) bool) map[string]*longhorn.Node {
	filtered := make(map[string]*longhorn.Node)
	for nodeID, node := range nodes {
		if predicate(node) {
			filtered[nodeID] = node
		}
	}
	return filtered
}

// filterReadyNodes returns only the nodes that are ready
func filterReadyNodes(nodes map[string]*longhorn.Node) map[string]*longhorn.Node {
	return filterNodes(nodes, func(node *longhorn.Node) bool {
		nodeReadyCondition := types.GetCondition(node.Status.Conditions, types.NodeConditionTypeReady)
		return nodeReadyCondition.Status == types.ConditionStatusTrue
	})
}

// filterSchedulableNodes returns only the nodes that are ready
func filterSchedulableNodes(nodes map[string]*longhorn.Node) map[string]*longhorn.Node {
	return filterNodes(nodes, func(node *longhorn.Node) bool {
		nodeSchedulableCondition := types.GetCondition(node.Status.Conditions, types.NodeConditionTypeSchedulable)
		return nodeSchedulableCondition.Status == types.ConditionStatusTrue
	})
}

func (s *DataStore) ListReadyNodes() (map[string]*longhorn.Node, error) {
	nodes, err := s.ListNodes()
	if err != nil {
		return nil, err
	}
	readyNodes := filterReadyNodes(nodes)
	return readyNodes, nil
}

func (s *DataStore) ListReadyAndSchedulableNodes() (map[string]*longhorn.Node, error) {
	nodes, err := s.ListNodes()
	if err != nil {
		return nil, err
	}
	return filterSchedulableNodes(filterReadyNodes(nodes)), nil
}

// ListReadyNodesWithEngineImage returns list of ready nodes that have the corresponding engine image deployed
func (s *DataStore) ListReadyNodesWithEngineImage(image string) (map[string]*longhorn.Node, error) {
	nodes, err := s.ListNodesWithEngineImage(image)
	if err != nil {
		return nil, err
	}
	readyNodes := filterReadyNodes(nodes)
	return readyNodes, nil
}

// ListKubeNodesRO returns a list of all Kubernetes Nodes for the given namespace,
// the list contains direct references to the internal cache objects and should not be mutated.
// Consider using this function when you can guarantee read only access and don't want the overhead of deep copies
func (s *DataStore) ListKubeNodesRO() ([]*corev1.Node, error) {
	return s.knLister.List(labels.Everything())
}

// ListPodsRO returns a list of all Pods for the given namespace,
// the list contains direct references to the internal cache objects and should not be mutated.
// Consider using this function when you can guarantee read only access and don't want the overhead of deep copies
func (s *DataStore) ListPodsRO() ([]*corev1.Pod, error) {
	return s.pLister.Pods(s.namespace).List(labels.Everything())
}

// GetRandomReadyNode gets a list of all Node in the given namespace and
// returns the first Node marked with condition ready and allow scheduling
func (s *DataStore) GetRandomReadyNode() (*longhorn.Node, error) {
	logrus.Debugf("Prepare to find a random ready node")
	nodesRO, err := s.ListNodesRO()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get random ready node")
	}

	for _, node := range nodesRO {
		readyCondition := types.GetCondition(node.Status.Conditions, types.NodeConditionTypeReady)
		if readyCondition.Status == types.ConditionStatusTrue && node.Spec.AllowScheduling == true {
			return node.DeepCopy(), nil
		}
	}

	return nil, fmt.Errorf("unable to get a ready node")
}

// RemoveFinalizerForNode will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForNode(obj *longhorn.Node) error {
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err := s.lhClient.LonghornV1beta1().Nodes(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for node %v", obj.Name)
	}
	return nil
}

func (s *DataStore) IsNodeDownOrDeletedOrMissingManager(name string) (bool, error) {
	if name == "" {
		return false, errors.New("no node name provided to IsNodeDownOrDeletedOrMissingManager")
	}
	node, err := s.GetNodeRO(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	cond := types.GetCondition(node.Status.Conditions, types.NodeConditionTypeReady)
	if cond.Status == types.ConditionStatusFalse &&
		(cond.Reason == string(types.NodeConditionReasonKubernetesNodeGone) ||
			cond.Reason == string(types.NodeConditionReasonKubernetesNodeNotReady) ||
			cond.Reason == string(types.NodeConditionReasonManagerPodMissing)) {
		return true, nil
	}
	return false, nil
}

// IsNodeDownOrDeleted gets Node for the given name and namespace and checks
// if the Node condition is gone or not ready
func (s *DataStore) IsNodeDownOrDeleted(name string) (bool, error) {
	if name == "" {
		return false, errors.New("no node name provided to check node down or deleted")
	}
	node, err := s.GetNodeRO(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	cond := types.GetCondition(node.Status.Conditions, types.NodeConditionTypeReady)
	if cond.Status == types.ConditionStatusFalse &&
		(cond.Reason == string(types.NodeConditionReasonKubernetesNodeGone) ||
			cond.Reason == string(types.NodeConditionReasonKubernetesNodeNotReady)) {
		return true, nil
	}
	return false, nil
}

func (s *DataStore) IsNodeSchedulable(name string) bool {
	node, err := s.GetNodeRO(name)
	if err != nil {
		return false
	}
	nodeSchedulableCondition := types.GetCondition(node.Status.Conditions, types.NodeConditionTypeSchedulable)
	return nodeSchedulableCondition.Status == types.ConditionStatusTrue
}

func getNodeSelector(nodeName string) (labels.Selector, error) {
	return metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{
			types.LonghornNodeKey: nodeName,
		},
	})
}

// ListReplicasByNode gets a map of Replicas on the node Name for the given namespace.
func (s *DataStore) ListReplicasByNode(name string) (map[string]*longhorn.Replica, error) {
	nodeSelector, err := getNodeSelector(name)
	if err != nil {
		return nil, err
	}
	return s.listReplicas(nodeSelector)
}

// ListReplicasByDiskUUID gets a list of Replicas on a specific disk the given namespace.
func (s *DataStore) ListReplicasByDiskUUID(uuid string) (map[string]*longhorn.Replica, error) {
	diskSelector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{
			types.LonghornDiskUUIDKey: uuid,
		},
	})
	if err != nil {
		return nil, err
	}
	return s.listReplicas(diskSelector)
}

func getBackingImageSelector(backingImageName string) (labels.Selector, error) {
	return metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{
			types.GetLonghornLabelKey(types.LonghornLabelBackingImage): backingImageName,
		},
	})
}

// ListReplicasByBackingImage gets a list of Replicas using a specific backing image the given namespace.
func (s *DataStore) ListReplicasByBackingImage(backingImageName string) ([]*longhorn.Replica, error) {
	backingImageSelector, err := getBackingImageSelector(backingImageName)
	if err != nil {
		return nil, err
	}
	return s.rLister.Replicas(s.namespace).List(backingImageSelector)
}

// ListReplicasByNodeRO returns a list of all Replicas on node Name for the given namespace,
// the list contains direct references to the internal cache objects and should not be mutated.
// Consider using this function when you can guarantee read only access and don't want the overhead of deep copies
func (s *DataStore) ListReplicasByNodeRO(name string) ([]*longhorn.Replica, error) {
	nodeSelector, err := getNodeSelector(name)
	if err != nil {
		return nil, err
	}
	return s.rLister.Replicas(s.namespace).List(nodeSelector)
}

func tagNodeLabel(nodeID string, obj runtime.Object) error {
	// fix longhornnode label for object
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	labels := metadata.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[types.LonghornNodeKey] = nodeID
	metadata.SetLabels(labels)
	return nil
}

func tagDiskUUIDLabel(diskUUID string, obj runtime.Object) error {
	// fix longhorndiskuuid label for object
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	labels := metadata.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[types.LonghornDiskUUIDKey] = diskUUID
	metadata.SetLabels(labels)
	return nil
}

// tagLonghornNodeLabel fixes the new label `longhorn.io/node` for object.
// It can replace func `tagNodeLabel` if the old label `longhornnode` is
// deprecated.
func tagLonghornNodeLabel(nodeID string, obj runtime.Object) error {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	labels := metadata.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[types.GetLonghornLabelKey(types.LonghornLabelNode)] = nodeID
	metadata.SetLabels(labels)
	return nil
}

// tagLonghornDiskUUIDLabel fixes the new label `longhorn.io/disk-uuid` for
// object. It can replace func `tagDiskUUIDLabel` if the old label
// `longhorndiskuuid` is deprecated.
func tagLonghornDiskUUIDLabel(diskUUID string, obj runtime.Object) error {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	labels := metadata.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[types.GetLonghornLabelKey(types.LonghornLabelDiskUUID)] = diskUUID
	metadata.SetLabels(labels)
	return nil
}

func tagBackingImageLabel(backingImageName string, obj runtime.Object) error {
	// fix longhorn.io/backing-image label for object
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	labels := metadata.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[types.GetLonghornLabelKey(types.LonghornLabelBackingImage)] = backingImageName
	metadata.SetLabels(labels)
	return nil
}

// GetOwnerReferencesForNode returns a list contains a single OwnerReference
// for the given Node ID and name
func GetOwnerReferencesForNode(node *longhorn.Node) []metav1.OwnerReference {
	blockOwnerDeletion := true
	return []metav1.OwnerReference{
		{
			APIVersion:         longhorn.SchemeGroupVersion.String(),
			Kind:               types.LonghornKindNode,
			Name:               node.Name,
			UID:                node.UID,
			BlockOwnerDeletion: &blockOwnerDeletion,
		},
	}
}

// GetSettingAsInt gets the setting for the given name, returns as integer
// Returns error if the definition type is not integer
func (s *DataStore) GetSettingAsInt(settingName types.SettingName) (int64, error) {
	definition, ok := types.SettingDefinitions[settingName]
	if !ok {
		return -1, fmt.Errorf("setting %v is not supported", settingName)
	}
	settings, err := s.GetSetting(settingName)
	if err != nil {
		return -1, err
	}
	value := settings.Value

	if definition.Type == types.SettingTypeInt {
		result, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return -1, err
		}
		return result, nil
	}

	return -1, fmt.Errorf("The %v setting value couldn't change to integer, value is %v ", string(settingName), value)
}

// GetSettingAsBool gets the setting for the given name, returns as boolean
// Returns error if the definition type is not boolean
func (s *DataStore) GetSettingAsBool(settingName types.SettingName) (bool, error) {
	definition, ok := types.SettingDefinitions[settingName]
	if !ok {
		return false, fmt.Errorf("setting %v is not supported", settingName)
	}
	settings, err := s.GetSetting(settingName)
	if err != nil {
		return false, err
	}
	value := settings.Value

	if definition.Type == types.SettingTypeBool {
		result, err := strconv.ParseBool(value)
		if err != nil {
			return false, err
		}
		return result, nil
	}

	return false, fmt.Errorf("The %v setting value couldn't be converted to bool, value is %v ", string(settingName), value)
}

// GetSettingImagePullPolicy get the setting and return one of Kubernetes ImagePullPolicy definition
// Returns error if the ImagePullPolicy is invalid
func (s *DataStore) GetSettingImagePullPolicy() (corev1.PullPolicy, error) {
	ipp, err := s.GetSetting(types.SettingNameSystemManagedPodsImagePullPolicy)
	if err != nil {
		return "", err
	}
	switch ipp.Value {
	case string(types.SystemManagedPodsImagePullPolicyNever):
		return corev1.PullNever, nil
	case string(types.SystemManagedPodsImagePullPolicyIfNotPresent):
		return corev1.PullIfNotPresent, nil
	case string(types.SystemManagedPodsImagePullPolicyAlways):
		return corev1.PullAlways, nil
	}
	return "", fmt.Errorf("invalid image pull policy %v", ipp.Value)
}

func (s *DataStore) GetSettingTaintToleration() ([]corev1.Toleration, error) {
	setting, err := s.GetSetting(types.SettingNameTaintToleration)
	if err != nil {
		return nil, err
	}
	tolerationList, err := types.UnmarshalTolerations(setting.Value)
	if err != nil {
		return nil, err
	}
	return tolerationList, nil
}

func (s *DataStore) GetSettingSystemManagedComponentsNodeSelector() (map[string]string, error) {
	setting, err := s.GetSetting(types.SettingNameSystemManagedComponentsNodeSelector)
	if err != nil {
		return nil, err
	}
	nodeSelector, err := types.UnmarshalNodeSelector(setting.Value)
	if err != nil {
		return nil, err
	}
	return nodeSelector, nil
}

// ResetMonitoringEngineStatus clean and update Engine status
func (s *DataStore) ResetMonitoringEngineStatus(e *longhorn.Engine) (*longhorn.Engine, error) {
	e.Status.Endpoint = ""
	e.Status.LastRestoredBackup = ""
	e.Status.ReplicaModeMap = nil
	e.Status.BackupStatus = nil
	e.Status.RestoreStatus = nil
	e.Status.PurgeStatus = nil
	e.Status.RebuildStatus = nil
	ret, err := s.UpdateEngineStatus(e)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to reset engine status for %v", e.Name)
	}
	return ret, nil
}

// DeleteNode deletes Node for the given name and namespace
func (s *DataStore) DeleteNode(name string) error {
	return s.lhClient.LonghornV1beta1().Nodes(s.namespace).Delete(name, &metav1.DeleteOptions{})
}

// ListEnginesByNodeRO returns a list of all Engines on node Name for the given namespace,
// the list contains direct references to the internal cache objects and should not be mutated.
// Consider using this function when you can guarantee read only access and don't want the overhead of deep copies
func (s *DataStore) ListEnginesByNodeRO(name string) ([]*longhorn.Engine, error) {
	nodeSelector, err := getNodeSelector(name)
	engineList, err := s.eLister.Engines(s.namespace).List(nodeSelector)
	if err != nil {
		return nil, err
	}
	return engineList, nil
}

// GetOwnerReferencesForInstanceManager returns OwnerReference for the given
// instance Manager name and UID
func GetOwnerReferencesForInstanceManager(im *longhorn.InstanceManager) []metav1.OwnerReference {
	controller := true
	return []metav1.OwnerReference{
		{
			APIVersion: longhorn.SchemeGroupVersion.String(),
			Kind:       types.LonghornKindInstanceManager,
			Name:       im.Name,
			UID:        im.UID,
			// This field is needed so that `kubectl drain` can work without --force flag
			// See https://github.com/longhorn/longhorn/issues/1286#issuecomment-623283028 for more details
			Controller: &controller,
		},
	}
}

// CreateInstanceManager creates a Longhorn InstanceManager resource and
// verifies creation
func (s *DataStore) CreateInstanceManager(im *longhorn.InstanceManager) (*longhorn.InstanceManager, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, im); err != nil {
		return nil, err
	}
	ret, err := s.lhClient.LonghornV1beta1().InstanceManagers(s.namespace).Create(im)
	if err != nil {
		return nil, err
	}
	if SkipListerCheck {
		return ret, nil
	}

	obj, err := verifyCreation(im.Name, "instance manager", func(name string) (runtime.Object, error) {
		return s.getInstanceManagerRO(name)
	})
	if err != nil {
		return nil, err
	}
	ret, ok := obj.(*longhorn.InstanceManager)
	if !ok {
		return nil, fmt.Errorf("BUG: datastore: verifyCreation returned wrong type for instance manager")
	}

	return ret.DeepCopy(), nil
}

// DeleteInstanceManager won't result in immediately deletion since finalizer
// was set by default
func (s *DataStore) DeleteInstanceManager(name string) error {
	return s.lhClient.LonghornV1beta1().InstanceManagers(s.namespace).Delete(name, &metav1.DeleteOptions{})
}

func (s *DataStore) getInstanceManagerRO(name string) (*longhorn.InstanceManager, error) {
	return s.imLister.InstanceManagers(s.namespace).Get(name)
}

func (s *DataStore) getInstanceManager(name string) (*longhorn.InstanceManager, error) {
	resultRO, err := s.getInstanceManagerRO(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

// GetInstanceManager gets the InstanceManager for the given name and namespace.
// Returns new InstanceManager object
func (s *DataStore) GetInstanceManager(name string) (*longhorn.InstanceManager, error) {
	result, err := s.getInstanceManager(name)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// CheckInstanceManagerType checks and returns InstanceManager labels type
// Returns error if the InstanceManager type is not engine or replica
func CheckInstanceManagerType(im *longhorn.InstanceManager) (types.InstanceManagerType, error) {
	imTypeLabelkey := types.GetLonghornLabelKey(types.LonghornLabelInstanceManagerType)
	imType, exist := im.Labels[imTypeLabelkey]
	if !exist {
		return types.InstanceManagerType(""), fmt.Errorf("no label %v in instance manager %v", imTypeLabelkey, im.Name)
	}

	switch imType {
	case string(types.InstanceManagerTypeEngine):
		return types.InstanceManagerTypeEngine, nil
	case string(types.InstanceManagerTypeReplica):
		return types.InstanceManagerTypeReplica, nil
	}

	return types.InstanceManagerType(""), fmt.Errorf("unknown type %v for instance manager %v", imType, im.Name)
}

// ListInstanceManagersBySelector gets a list of InstanceManager by labels for
// the given namespace. Returns an object contains all InstanceManager
func (s *DataStore) ListInstanceManagersBySelector(node, instanceManagerImage string, managerType types.InstanceManagerType) (map[string]*longhorn.InstanceManager, error) {
	itemMap := map[string]*longhorn.InstanceManager{}

	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: types.GetInstanceManagerLabels(node, instanceManagerImage, managerType),
	})
	if err != nil {
		return nil, err
	}

	listRO, err := s.imLister.InstanceManagers(s.namespace).List(selector)
	if err != nil {
		return nil, err
	}
	for _, itemRO := range listRO {
		// Cannot use cached object from lister
		itemMap[itemRO.Name] = itemRO.DeepCopy()
	}
	return itemMap, nil
}

// GetInstanceManagerByInstance gets a list of InstanceManager for the given
// object. Returns error if more than one InstanceManager is found
func (s *DataStore) GetInstanceManagerByInstance(obj interface{}) (*longhorn.InstanceManager, error) {
	var (
		name, nodeID string
		imType       types.InstanceManagerType
	)

	image, err := s.GetSettingValueExisted(types.SettingNameDefaultInstanceManagerImage)
	if err != nil {
		return nil, err
	}

	switch obj.(type) {
	case *longhorn.Engine:
		engine := obj.(*longhorn.Engine)
		name = engine.Name
		nodeID = engine.Spec.NodeID
		imType = types.InstanceManagerTypeEngine
	case *longhorn.Replica:
		replica := obj.(*longhorn.Replica)
		name = replica.Name
		nodeID = replica.Spec.NodeID
		imType = types.InstanceManagerTypeReplica
	default:
		return nil, fmt.Errorf("unknown type for GetInstanceManagerByInstance, %+v", obj)
	}
	if nodeID == "" {
		return nil, fmt.Errorf("invalid request for GetInstanceManagerByInstance: no NodeID specified for instance %v", name)
	}

	imMap, err := s.ListInstanceManagersBySelector(nodeID, image, imType)
	if err != nil {
		return nil, err
	}
	if len(imMap) == 1 {
		for _, im := range imMap {
			return im, nil
		}

	}
	return nil, fmt.Errorf("can not find the only available instance manager for instance %v, node %v, instance manager image %v, type %v", name, nodeID, image, imType)
}

// ListInstanceManagersByNode returns ListInstanceManagersBySelector
func (s *DataStore) ListInstanceManagersByNode(node string, imType types.InstanceManagerType) (map[string]*longhorn.InstanceManager, error) {
	return s.ListInstanceManagersBySelector(node, "", imType)
}

// ListInstanceManagers gets a list of InstanceManagers for the given namespace.
// Returns a new InstanceManager object
func (s *DataStore) ListInstanceManagers() (map[string]*longhorn.InstanceManager, error) {
	itemMap := map[string]*longhorn.InstanceManager{}

	list, err := s.imLister.InstanceManagers(s.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	for _, itemRO := range list {
		// Cannot use cached object from lister
		itemMap[itemRO.Name] = itemRO.DeepCopy()
	}
	return itemMap, nil
}

// RemoveFinalizerForInstanceManager will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForInstanceManager(obj *longhorn.InstanceManager) error {
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err := s.lhClient.LonghornV1beta1().InstanceManagers(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for instance manager %v", obj.Name)
	}
	return nil
}

// UpdateInstanceManager updates Longhorn InstanceManager resource and verifies update
func (s *DataStore) UpdateInstanceManager(im *longhorn.InstanceManager) (*longhorn.InstanceManager, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, im); err != nil {
		return nil, err
	}

	obj, err := s.lhClient.LonghornV1beta1().InstanceManagers(s.namespace).Update(im)
	if err != nil {
		return nil, err
	}
	verifyUpdate(im.Name, obj, func(name string) (runtime.Object, error) {
		return s.getInstanceManagerRO(name)
	})
	return obj, nil
}

// UpdateInstanceManagerStatus updates Longhorn InstanceManager resource status
// and verifies update
func (s *DataStore) UpdateInstanceManagerStatus(im *longhorn.InstanceManager) (*longhorn.InstanceManager, error) {
	obj, err := s.lhClient.LonghornV1beta1().InstanceManagers(s.namespace).UpdateStatus(im)
	if err != nil {
		return nil, err
	}
	verifyUpdate(im.Name, obj, func(name string) (runtime.Object, error) {
		return s.getInstanceManagerRO(name)
	})
	return obj, nil
}

func verifyCreation(name, kind string, getMethod func(name string) (runtime.Object, error)) (runtime.Object, error) {
	// WORKAROUND: The immedidate read after object's creation can fail.
	// See https://github.com/longhorn/longhorn/issues/133
	var (
		ret runtime.Object
		err error
	)
	for i := 0; i < VerificationRetryCounts; i++ {
		if ret, err = getMethod(name); err == nil {
			break
		}
		if !ErrorIsNotFound(err) {
			break
		}
		time.Sleep(VerificationRetryInterval)
	}
	if err != nil {
		return nil, fmt.Errorf("Unable to verify the existence of newly created %s %s: %v", kind, name, err)
	}
	return ret, nil
}

func verifyUpdate(name string, obj runtime.Object, getMethod func(name string) (runtime.Object, error)) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		logrus.Errorf("BUG: datastore: cannot verify update for %v (%+v) because cannot get accessor: %v", name, obj, err)
		return
	}
	minimalResourceVersion := accessor.GetResourceVersion()
	verified := false
	for i := 0; i < VerificationRetryCounts; i++ {
		ret, err := getMethod(name)
		if err != nil {
			logrus.Errorf("datastore: failed to get updated object %v", name)
			return
		}
		accessor, err := meta.Accessor(ret)
		if err != nil {
			logrus.Errorf("BUG: datastore: cannot verify update for %v because cannot get accessor for updated object: %v", name, err)
			return
		}
		if resourceVersionAtLeast(accessor.GetResourceVersion(), minimalResourceVersion) {
			verified = true
			break
		}
		time.Sleep(VerificationRetryInterval)
	}
	if !verified {
		logrus.Errorf("Unable to verify the update of %s", name)
	}
}

// resourceVersionAtLeast depends on the Kubernetes internal resource version implementation
// See https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#concurrency-control-and-consistency
func resourceVersionAtLeast(curr, min string) bool {
	// skip unit testing code
	if curr == "" || min == "" {
		return true
	}
	currVersion, err := strconv.ParseInt(curr, 10, 64)
	if err != nil {
		logrus.Errorf("datastore: failed to parse current resource version %v: %v", curr, err)
		return false
	}
	minVersion, err := strconv.ParseInt(min, 10, 64)
	if err != nil {
		logrus.Errorf("datastore: failed to parse minimal resource version %v: %v", min, err)
		return false
	}
	return currVersion >= minVersion
}

// IsEngineImageCLIAPIVersionOne get engine image CLIAPIVersion for the given name.
// Returns true if CLIAPIVersion is 1
func (s *DataStore) IsEngineImageCLIAPIVersionOne(imageName string) (bool, error) {
	version, err := s.GetEngineImageCLIAPIVersion(imageName)
	if err != nil {
		return false, err
	}

	if version == 1 {
		return true, nil
	}
	return false, nil
}

// GetEngineImageCLIAPIVersion get engine image for the given name and returns the
// CLIAPIVersion
func (s *DataStore) GetEngineImageCLIAPIVersion(imageName string) (int, error) {
	if imageName == "" {
		return -1, fmt.Errorf("cannot check the CLI API Version based on empty image name")
	}
	ei, err := s.GetEngineImage(types.GetEngineImageChecksumName(imageName))
	if err != nil {
		return -1, errors.Wrapf(err, "failed to get engine image object based on image name %v", imageName)
	}

	return ei.Status.CLIAPIVersion, nil
}

// GetOwnerReferencesForShareManager returns OwnerReference for the given share manager name and UID
func GetOwnerReferencesForShareManager(sm *longhorn.ShareManager, isController bool) []metav1.OwnerReference {
	return []metav1.OwnerReference{
		{
			APIVersion: longhorn.SchemeGroupVersion.String(),
			Kind:       types.LonghornKindShareManager,
			Name:       sm.Name,
			UID:        sm.UID,
			Controller: &isController,
		},
	}
}

// CreateShareManager creates a Longhorn ShareManager resource and
// verifies creation
func (s *DataStore) CreateShareManager(sm *longhorn.ShareManager) (*longhorn.ShareManager, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, sm); err != nil {
		return nil, err
	}
	ret, err := s.lhClient.LonghornV1beta1().ShareManagers(s.namespace).Create(sm)
	if err != nil {
		return nil, err
	}
	if SkipListerCheck {
		return ret, nil
	}

	obj, err := verifyCreation(sm.Name, "share manager", func(name string) (runtime.Object, error) {
		return s.getShareManagerRO(name)
	})
	if err != nil {
		return nil, err
	}
	ret, ok := obj.(*longhorn.ShareManager)
	if !ok {
		return nil, fmt.Errorf("BUG: datastore: verifyCreation returned wrong type for share manager")
	}

	return ret.DeepCopy(), nil
}

// UpdateShareManager updates Longhorn ShareManager resource and verifies update
func (s *DataStore) UpdateShareManager(sm *longhorn.ShareManager) (*longhorn.ShareManager, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, sm); err != nil {
		return nil, err
	}

	obj, err := s.lhClient.LonghornV1beta1().ShareManagers(s.namespace).Update(sm)
	if err != nil {
		return nil, err
	}
	verifyUpdate(sm.Name, obj, func(name string) (runtime.Object, error) {
		return s.getShareManagerRO(name)
	})
	return obj, nil
}

// UpdateShareManagerStatus updates Longhorn ShareManager resource status and verifies update
func (s *DataStore) UpdateShareManagerStatus(sm *longhorn.ShareManager) (*longhorn.ShareManager, error) {
	obj, err := s.lhClient.LonghornV1beta1().ShareManagers(s.namespace).UpdateStatus(sm)
	if err != nil {
		return nil, err
	}
	verifyUpdate(sm.Name, obj, func(name string) (runtime.Object, error) {
		return s.getShareManagerRO(name)
	})
	return obj, nil
}

// DeleteShareManager won't result in immediately deletion since finalizer was set by default
func (s *DataStore) DeleteShareManager(name string) error {
	return s.lhClient.LonghornV1beta1().ShareManagers(s.namespace).Delete(name, &metav1.DeleteOptions{})
}

// RemoveFinalizerForShareManager will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForShareManager(obj *longhorn.ShareManager) error {
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	if _, err := s.lhClient.LonghornV1beta1().ShareManagers(s.namespace).Update(obj); err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for share manager %v", obj.Name)
	}
	return nil
}

func (s *DataStore) getShareManagerRO(name string) (*longhorn.ShareManager, error) {
	return s.smLister.ShareManagers(s.namespace).Get(name)
}

// GetShareManager gets the ShareManager for the given name and namespace.
// Returns a mutable ShareManager object
func (s *DataStore) GetShareManager(name string) (*longhorn.ShareManager, error) {
	result, err := s.getShareManagerRO(name)
	if err != nil {
		return nil, err
	}
	return result.DeepCopy(), nil
}

// ListShareManagers returns a map of ShareManagers indexed by name
func (s *DataStore) ListShareManagers() (map[string]*longhorn.ShareManager, error) {
	itemMap := map[string]*longhorn.ShareManager{}

	list, err := s.smLister.ShareManagers(s.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	for _, itemRO := range list {
		// Cannot use cached object from lister
		itemMap[itemRO.Name] = itemRO.DeepCopy()
	}
	return itemMap, nil
}

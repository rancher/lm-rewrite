package manager

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

func (m *VolumeManager) ListBackingImages() (map[string]*longhorn.BackingImage, error) {
	return m.ds.ListBackingImages()
}

func (m *VolumeManager) ListBackingImagesSorted() ([]*longhorn.BackingImage, error) {
	backingImageMap, err := m.ds.ListBackingImages()
	if err != nil {
		return []*longhorn.BackingImage{}, err
	}

	backingImages := make([]*longhorn.BackingImage, len(backingImageMap))
	backingImageNames, err := sortKeys(backingImageMap)
	if err != nil {
		return []*longhorn.BackingImage{}, err
	}
	for i, backingImageName := range backingImageNames {
		backingImages[i] = backingImageMap[backingImageName]
	}
	return backingImages, nil
}

func (m *VolumeManager) GetBackingImage(name string) (*longhorn.BackingImage, error) {
	return m.ds.GetBackingImage(name)
}

func (m *VolumeManager) ListBackingImageDataSources() (map[string]*longhorn.BackingImageDataSource, error) {
	return m.ds.ListBackingImageDataSources()
}

func (m *VolumeManager) GetBackingImageDataSource(name string) (*longhorn.BackingImageDataSource, error) {
	return m.ds.GetBackingImageDataSource(name)
}

func (m *VolumeManager) CreateBackingImage(name, checksum, sourceType string, parameters map[string]string) (bi *longhorn.BackingImage, bids *longhorn.BackingImageDataSource, err error) {
	name = util.AutoCorrectName(name, datastore.NameMaximumLength)
	if !util.ValidateName(name) {
		return nil, nil, fmt.Errorf("invalid name %v", name)
	}

	switch types.BackingImageDataSourceType(sourceType) {
	case types.BackingImageDataSourceTypeDownload:
		if parameters[types.DataSourceTypeDownloadParameterURL] == "" {
			return nil, nil, fmt.Errorf("invalid parameter %+v for source type %v", parameters, sourceType)
		}
	case types.BackingImageDataSourceTypeUpload:
	default:
		return nil, nil, fmt.Errorf("unknown backing image source type %v", sourceType)
	}

	if _, err := m.ds.GetBackingImage(name); err == nil {
		return nil, nil, fmt.Errorf("backing image already exists")
	} else if !apierrors.IsNotFound(err) {
		return nil, nil, errors.Wrapf(err, "failed to check backing image existence before creation")
	}
	if bids, err := m.ds.GetBackingImageDataSource(name); err == nil {
		if bids.DeletionTimestamp == nil {
			if err := m.ds.DeleteBackingImageDataSource(name); err != nil && !apierrors.IsNotFound(err) {
				return nil, nil, errors.Wrapf(err, "failed to clean up old backing image data source before creation")
			}
		}
		return nil, nil, errors.Wrapf(err, "need to wait for old backing image data source removal before creation")
	} else if !apierrors.IsNotFound(err) {
		return nil, nil, fmt.Errorf("failed to check backing image data source existence before creation")
	}

	var diskUUID, diskPath, nodeID string
	nodes, err := m.ds.ListNodes()
	if err != nil {
		return nil, nil, err
	}
	for _, node := range nodes {
		if types.GetCondition(node.Status.Conditions, types.NodeConditionTypeSchedulable).Status != types.ConditionStatusTrue {
			continue
		}
		for diskName, diskStatus := range node.Status.DiskStatus {
			if types.GetCondition(diskStatus.Conditions, types.DiskConditionTypeSchedulable).Status != types.ConditionStatusTrue {
				continue
			}
			if _, exists := node.Spec.Disks[diskName]; !exists {
				continue
			}
			diskUUID = diskStatus.DiskUUID
			diskPath = node.Spec.Disks[diskName].Path
			nodeID = node.Name
			break
		}
		if diskUUID != "" && diskPath != "" && nodeID != "" {
			break
		}
	}
	if diskUUID == "" || diskPath == "" || nodeID == "" {
		return nil, nil, fmt.Errorf("cannot find a schedulable disk for backing image %v creation", name)
	}

	defer func() {
		if err == nil {
			return
		}
		m.ds.DeleteBackingImage(name)
	}()

	bi = &longhorn.BackingImage{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: types.GetBackingImageLabels(),
		},
		Spec: types.BackingImageSpec{
			Disks: map[string]struct{}{
				diskUUID: struct{}{},
			},
			Checksum: checksum,
		},
	}
	if bi, err = m.ds.CreateBackingImage(bi); err != nil {
		return nil, nil, err
	}

	bids = &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			OwnerReferences: datastore.GetOwnerReferencesForBackingImage(bi),
		},
		Spec: types.BackingImageDataSourceSpec{
			NodeID:     nodeID,
			DiskUUID:   diskUUID,
			DiskPath:   diskPath,
			Checksum:   checksum,
			SourceType: types.BackingImageDataSourceType(sourceType),
			Parameters: parameters,
		},
	}
	if bids, err = m.ds.CreateBackingImageDataSource(bids); err != nil {
		return nil, nil, err
	}

	logrus.Infof("Created backing image %v", name)
	return bi, bids, nil
}

func (m *VolumeManager) DeleteBackingImage(name string) error {
	replicas, err := m.ds.ListReplicasByBackingImage(name)
	if err != nil {
		return err
	}
	if len(replicas) != 0 {
		return fmt.Errorf("cannot delete backing image %v since there are replicas using it", name)
	}
	if err := m.ds.DeleteBackingImage(name); err != nil {
		return err
	}
	logrus.Infof("Deleting backing image %v", name)
	return nil
}

func (m *VolumeManager) CleanUpBackingImageInDisks(name string, disks []string) (*longhorn.BackingImage, error) {
	defer logrus.Infof("Cleaning up backing image %v in disks %+v", name, disks)
	bi, err := m.GetBackingImage(name)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get backing image %v", name)
	}
	replicas, err := m.ds.ListReplicasByBackingImage(name)
	if err != nil {
		return nil, err
	}
	disksInUse := map[string]struct{}{}
	for _, r := range replicas {
		disksInUse[r.Spec.DiskID] = struct{}{}
	}
	for _, id := range disks {
		if _, exists := disksInUse[id]; exists {
			return nil, fmt.Errorf("cannot clean up backing image %v in disk %v since there is at least one replica using it", name, id)
		}
		delete(bi.Spec.Disks, id)
	}
	return m.ds.UpdateBackingImage(bi)
}

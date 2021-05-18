package manager

import (
	"fmt"
	"strings"

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

func (m *VolumeManager) GetBackingImage(name string) (*longhorn.BackingImage, error) {
	return m.ds.GetBackingImage(name)
}

func (m *VolumeManager) CreateBackingImage(name, url string, requireUpload bool) (*longhorn.BackingImage, error) {
	url = strings.TrimSpace(url)
	if url == "" && !requireUpload {
		return nil, fmt.Errorf("cannot create backing image with empty image URL")
	}

	name = util.AutoCorrectName(name, datastore.NameMaximumLength)
	if !util.ValidateName(name) {
		return nil, fmt.Errorf("invalid name %v", name)
	}

	bi := &longhorn.BackingImage{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: types.GetBackingImageLabels(),
		},
		Spec: types.BackingImageSpec{
			ImageURL:      url,
			Disks:         map[string]struct{}{},
			RequireUpload: requireUpload,
		},
	}

	// For upload backing image, the file should be there before the 1st
	// replica starts to use it.
	if bi.Spec.RequireUpload {
		node, err := m.ds.GetRandomReadyNode()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to find a ready node for backing image %v upload", name)
		}
		for _, diskStatus := range node.Status.DiskStatus {
			if types.GetCondition(diskStatus.Conditions, types.DiskConditionTypeSchedulable).Status == types.ConditionStatusTrue {
				bi.Spec.Disks[diskStatus.DiskUUID] = struct{}{}
				break
			}
		}
		if len(bi.Spec.Disks) == 0 {
			return nil, fmt.Errorf("cannot find a schedulable disk for backing image %v upload", name)
		}
	}

	bi, err := m.ds.CreateBackingImage(bi)
	if err != nil {
		return nil, err
	}
	logrus.Infof("Created backing image %v with URL %v", name, url)
	return bi, nil
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
	// Deep copy
	existingDiskSpec := map[string]struct{}{}
	for k, v := range bi.Spec.Disks {
		existingDiskSpec[k] = v
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
	if !bi.Spec.RequireUpload {
		return m.ds.UpdateBackingImage(bi)
	}

	// For upload backing image, Longhorn should retain at least one ready
	// file or the uploading file.
	cleanupDiskMap := map[string]struct{}{}
	for _, diskUUID := range disks {
		cleanupDiskMap[diskUUID] = struct{}{}
	}
	containsRetainedDownloadedDisk := false
	containsDownloadedDisk := false
	for diskUUID := range existingDiskSpec {
		if bi.Status.DiskFileStateMap[diskUUID] != types.BackingImageStateReady {
			continue
		}
		if _, exists := cleanupDiskMap[diskUUID]; !exists {
			containsRetainedDownloadedDisk = true
		}
		containsDownloadedDisk = true
	}
	if containsRetainedDownloadedDisk {
		return m.ds.UpdateBackingImage(bi)
	}
	if containsDownloadedDisk {
		return nil, fmt.Errorf("cannot clean up all downloaded files in disks for upload backing image")
	}
	// No ready entry. The upload is still in progress.
	for diskUUID := range existingDiskSpec {
		if bi.Status.DiskFileStateMap[diskUUID] == types.BackingImageStateFailed {
			continue
		}
		if _, exists := cleanupDiskMap[diskUUID]; exists {
			return nil, fmt.Errorf("cannot clean up the file in disk %v since the uploading is in progress", diskUUID)
		}
	}
	return m.ds.UpdateBackingImage(bi)
}

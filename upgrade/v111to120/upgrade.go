package v111to120

import (
	"reflect"

	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

const (
	upgradeLogPrefix = "upgrade from v1.1.1 to v1.2.0: "
)

func UpgradeCRs(namespace string, lhClient *lhclientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"UpgradeCRs failed")
	}()
	if err := upgradeBackingImages(namespace, lhClient); err != nil {
		return err
	}

	return nil
}

const (
	DeprecatedBackingImageStateDownloaded  = "downloaded"
	DeprecatedBackingImageStateDownloading = "downloading"
)

func upgradeBackingImages(namespace string, lhClient *lhclientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, "upgrade backing images failed")
	}()
	biList, err := lhClient.LonghornV1beta1().BackingImages(namespace).List(metav1.ListOptions{})
	if err != nil {
		return err
	}
	nodeList, err := lhClient.LonghornV1beta1().Nodes(namespace).List(metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, bi := range biList.Items {
		existingBI := bi.DeepCopy()
		if bi.Status.DiskFileStatusMap == nil {
			bi.Status.DiskFileStatusMap = map[string]*types.BackingImageDiskFileStatus{}
		}
		for diskUUID, state := range bi.Status.DiskDownloadStateMap {
			if _, exists := bi.Status.DiskFileStatusMap[diskUUID]; !exists {
				bi.Status.DiskFileStatusMap[diskUUID] = &types.BackingImageDiskFileStatus{}
			}
			switch string(state) {
			case DeprecatedBackingImageStateDownloaded:
				bi.Status.DiskFileStatusMap[diskUUID].State = types.BackingImageStateReady
			case DeprecatedBackingImageStateDownloading:
				bi.Status.DiskFileStatusMap[diskUUID].State = types.BackingImageStateInProgress
			default:
				bi.Status.DiskFileStatusMap[diskUUID].State = types.BackingImageState(state)
			}
		}
		bi.Status.DiskDownloadStateMap = map[string]types.BackingImageDownloadState{}

		for diskUUID, progress := range bi.Status.DiskDownloadProgressMap {
			if _, exists := bi.Status.DiskFileStatusMap[diskUUID]; !exists {
				continue
			}
			bi.Status.DiskFileStatusMap[diskUUID].Progress = progress
		}
		bi.Status.DiskDownloadProgressMap = map[string]int{}

		if !reflect.DeepEqual(bi.Status, existingBI.Status) {
			if _, err := lhClient.LonghornV1beta1().BackingImages(namespace).UpdateStatus(&bi); err != nil {
				return err
			}
		}

		if bi.Spec.ImageURL != "" {
			bi, err := lhClient.LonghornV1beta1().BackingImages(namespace).Get(bi.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if err := checkAndCreateBackingImageDataSource(namespace, lhClient, bi, nodeList); err != nil {
				return err
			}
			bi.Spec.ImageURL = ""
			if _, err := lhClient.LonghornV1beta1().BackingImages(namespace).Update(bi); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkAndCreateBackingImageDataSource(namespace string, lhClient *lhclientset.Clientset, bi *longhorn.BackingImage, nodeList *longhorn.NodeList) (err error) {
	bids, err := lhClient.LonghornV1beta1().BackingImageDataSources(namespace).Get(bi.Name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		// Pick up an random disk in the backing image spec to create the backing image data source
		var availableNode, availableDiskUUID, availableDiskPath string
		for _, node := range nodeList.Items {
			for diskName, status := range node.Status.DiskStatus {
				spec, exists := node.Spec.Disks[diskName]
				if !exists {
					continue
				}
				for diskUUID := range bi.Spec.Disks {
					if diskUUID != status.DiskUUID {
						continue
					}
					availableNode = node.Name
					availableDiskUUID = status.DiskUUID
					availableDiskPath = spec.Path
					break
				}
				if availableNode != "" {
					break
				}
			}
		}

		bids = &longhorn.BackingImageDataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name:            bi.Name,
				OwnerReferences: datastore.GetOwnerReferencesForBackingImage(bi),
			},
			Spec: types.BackingImageDataSourceSpec{
				NodeID:     availableNode,
				DiskUUID:   availableDiskUUID,
				DiskPath:   availableDiskPath,
				SourceType: types.BackingImageDataSourceTypeDownload,
				Parameters: map[string]string{types.DataSourceTypeDownloadParameterURL: bi.Spec.ImageURL},
				Started:    true,
			},
		}
		if bids, err = lhClient.LonghornV1beta1().BackingImageDataSources(namespace).Create(bids); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}

	// Then blindly set the state as ready
	bids.Status.OwnerID = bids.Spec.NodeID
	bids.Status.CurrentState = types.BackingImageStateReady
	bids.Status.Size = bi.Status.Size
	bids.Status.Progress = 100
	if bids, err = lhClient.LonghornV1beta1().BackingImageDataSources(namespace).UpdateStatus(bids); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}

	return nil
}

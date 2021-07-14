package v111to120

import (
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	for _, bi := range biList.Items {
		if bi.Status.DiskFileStateMap == nil {
			bi.Status.DiskFileStateMap = map[string]types.BackingImageState{}
		}
		for diskUUID, state := range bi.Status.DiskDownloadStateMap {
			switch string(state) {
			case DeprecatedBackingImageStateDownloaded:
				bi.Status.DiskFileStateMap[diskUUID] = types.BackingImageStateReady
			case DeprecatedBackingImageStateDownloading:
				bi.Status.DiskFileStateMap[diskUUID] = types.BackingImageStateInProgress
			default:
				bi.Status.DiskFileStateMap[diskUUID] = types.BackingImageState(state)
			}
		}
		bi.Status.DiskDownloadStateMap = map[string]types.BackingImageDownloadState{}

		if bi.Status.DiskFileHandlingProgressMap == nil {
			bi.Status.DiskFileHandlingProgressMap = map[string]int{}
		}
		for diskUUID, progress := range bi.Status.DiskDownloadProgressMap {
			bi.Status.DiskFileHandlingProgressMap[diskUUID] = progress
		}
		bi.Status.DiskDownloadProgressMap = map[string]int{}

		if _, err := lhClient.LonghornV1beta1().BackingImages(namespace).UpdateStatus(&bi); err != nil {
			return err
		}
	}
	return nil
}

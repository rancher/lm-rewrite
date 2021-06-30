package manager

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

var (
	ConflictRetryCount = 5
)

func UpdateVolumeLastBackup(volumeName string, backupTarget *engineapi.BackupTarget,
	getVolume func(name string) (*longhorn.Volume, error),
	updateVolume func(v *longhorn.Volume) (*longhorn.Volume, error)) (err error) {

	defer func() {
		err = errors.Wrapf(err, "failed to UpdateVolumeLastBackup for %v", volumeName)
	}()

	backupVolumeMetadataURL := backupstore.EncodeMetadataURL("", volumeName, backupTarget.URL)
	backupVolume, err := backupTarget.InspectBackupVolumeMetadata(backupVolumeMetadataURL)
	if err != nil {
		return err
	}
	return SyncVolumeLastBackupWithBackupVolume(volumeName, backupVolume, getVolume, updateVolume)
}

func SyncVolumeLastBackupWithBackupVolume(volumeName string, backupVolume *engineapi.BackupVolume,
	getVolume func(name string) (*longhorn.Volume, error),
	updateVolume func(v *longhorn.Volume) (*longhorn.Volume, error)) (err error) {

	defer func() {
		err = errors.Wrapf(err, "failed to SyncVolumeWithBackupVolume for %v", volumeName)
	}()

	lastBackup := ""
	lastBackupAt := ""
	if backupVolume != nil {
		lastBackup = backupVolume.LastBackupName
		lastBackupAt = backupVolume.LastBackupAt
	}
	for i := 0; i < ConflictRetryCount; i++ {
		v, err := getVolume(volumeName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				return nil
			}
			return err
		}
		// for standby volume just restored from a backup, field LastBackupAt will be empty.
		if v.Status.LastBackup == lastBackup && v.Status.LastBackupAt == lastBackupAt {
			return nil
		}
		v.Status.LastBackup = lastBackup
		v.Status.LastBackupAt = lastBackupAt
		v, err = updateVolume(v)
		if err == nil {
			if v.Status.LastBackup != "" {
				logrus.Debugf("Volume %v LastBackup updated to %v at %v",
					v.Name, v.Status.LastBackup, v.Status.LastBackupAt)
			} else {
				logrus.Debugf("Volume %v LastBackup has been unset", v.Name)
			}
			return nil
		}
		if !datastore.ErrorIsConflict(err) {
			return err
		}
		logrus.Debugf("Retrying updating LastBackup for volume %v due to conflict", volumeName)
	}
	return fmt.Errorf("Cannot update LastBackup for volume %v due to too many conflicts", volumeName)
}

func SyncVolumesLastBackupWithBackupVolumes(backupVolumes map[string]*engineapi.BackupVolume,
	listVolumes func() (map[string]*longhorn.Volume, error),
	getVolume func(name string) (*longhorn.Volume, error),
	updateVolume func(v *longhorn.Volume) (*longhorn.Volume, error)) {
	volumes, err := listVolumes()
	if err != nil {
		logrus.Errorf("manager: failed to list volumes in SyncVolumesLastBackupWithBackupVolumes")
		return
	}
	for _, v := range volumes {
		var bv *engineapi.BackupVolume
		if backupVolumes != nil {
			// Keep updating last backup for restore/DR volumes
			if v.Status.RestoreRequired || v.Status.IsStandby {
				bvName, _, _, err := backupstore.DecodeMetadataURL(v.Spec.FromBackup)
				if err != nil {
					logrus.Errorf("cannot parse field FromBackup of standby volume %v: %v", v.Name, err)
					return
				}
				bv = backupVolumes[bvName]
			} else {
				bv = backupVolumes[v.Name]
			}
		}
		// bv can be nil
		if err := SyncVolumeLastBackupWithBackupVolume(v.Name, bv, getVolume, updateVolume); err != nil {
			logrus.Errorf("failed to update last backup for %+v: %v", bv, err)
		}
	}
}

func GenerateBackupTarget(ds *datastore.DataStore) (*engineapi.BackupTarget, error) {
	targetURL, err := ds.GetSettingValueExisted(types.SettingNameBackupTarget)
	if err != nil {
		return nil, err
	}
	engineImage, err := ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if err != nil {
		return nil, err
	}
	credential, err := GetBackupCredentialConfig(ds)
	if err != nil {
		return nil, err
	}
	return engineapi.NewBackupTarget(targetURL, engineImage, credential), nil
}

func GetBackupCredentialConfig(ds *datastore.DataStore) (map[string]string, error) {
	backupTarget, err := ds.GetSettingValueExisted(types.SettingNameBackupTarget)
	if err != nil {
		return nil, fmt.Errorf("cannot backup: unable to get settings %v",
			types.SettingNameBackupTarget)
	}
	backupType, err := util.CheckBackupType(backupTarget)
	if err != nil {
		return nil, err
	}
	if backupType == types.BackupStoreTypeS3 {
		secretName, err := ds.GetSettingValueExisted(types.SettingNameBackupTargetCredentialSecret)
		if err != nil {
			return nil, fmt.Errorf("cannot backup: unable to get settings %v",
				types.SettingNameBackupTargetCredentialSecret)
		}
		if secretName == "" {
			return nil, errors.New("Could not backup for s3 without credential secret")
		}
		return ds.GetCredentialFromSecret(secretName)
	}
	return nil, nil
}

package manager

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	BackupStatusQueryInterval = 2 * time.Second
)

func (m *VolumeManager) ListSnapshots(volumeName string) (map[string]*types.Snapshot, error) {
	if volumeName == "" {
		return nil, fmt.Errorf("volume name required")
	}
	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return nil, err
	}
	return engine.SnapshotList()
}

func (m *VolumeManager) GetSnapshot(snapshotName, volumeName string) (*types.Snapshot, error) {
	if volumeName == "" || snapshotName == "" {
		return nil, fmt.Errorf("volume and snapshot name required")
	}
	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return nil, err
	}
	snapshot, err := engine.SnapshotGet(snapshotName)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, fmt.Errorf("cannot find snapshot '%s' for volume '%s'", snapshotName, volumeName)
	}
	return snapshot, nil
}

func (m *VolumeManager) CreateSnapshot(snapshotName string, labels map[string]string, volumeName string) (*types.Snapshot, error) {
	if volumeName == "" {
		return nil, fmt.Errorf("volume name required")
	}

	for k, v := range labels {
		if strings.Contains(k, "=") || strings.Contains(v, "=") {
			return nil, fmt.Errorf("labels cannot contain '='")
		}
	}

	if err := m.checkVolumeNotInMigration(volumeName); err != nil {
		return nil, err
	}

	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return nil, err
	}
	snapshotName, err = engine.SnapshotCreate(snapshotName, labels)
	if err != nil {
		return nil, err
	}
	snap, err := engine.SnapshotGet(snapshotName)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, fmt.Errorf("cannot found just created snapshot '%s', for volume '%s'", snapshotName, volumeName)
	}
	logrus.Debugf("Created snapshot %v with labels %+v for volume %v", snapshotName, labels, volumeName)
	return snap, nil
}

func (m *VolumeManager) DeleteSnapshot(snapshotName, volumeName string) error {
	if volumeName == "" || snapshotName == "" {
		return fmt.Errorf("volume and snapshot name required")
	}

	if err := m.checkVolumeNotInMigration(volumeName); err != nil {
		return err
	}

	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return err
	}
	if err := engine.SnapshotDelete(snapshotName); err != nil {
		return err
	}
	logrus.Debugf("Deleted snapshot %v for volume %v", snapshotName, volumeName)
	return nil
}

func (m *VolumeManager) RevertSnapshot(snapshotName, volumeName string) error {
	if volumeName == "" || snapshotName == "" {
		return fmt.Errorf("volume and snapshot name required")
	}

	if err := m.checkVolumeNotInMigration(volumeName); err != nil {
		return err
	}

	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return err
	}
	snapshot, err := engine.SnapshotGet(snapshotName)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return fmt.Errorf("not found snapshot '%s', for volume '%s'", snapshotName, volumeName)
	}
	if err := engine.SnapshotRevert(snapshotName); err != nil {
		return err
	}
	logrus.Debugf("Revert to snapshot %v for volume %v", snapshotName, volumeName)
	return nil
}

func (m *VolumeManager) PurgeSnapshot(volumeName string) error {
	if volumeName == "" {
		return fmt.Errorf("volume name required")
	}

	if err := m.checkVolumeNotInMigration(volumeName); err != nil {
		return err
	}

	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return err
	}

	if err := engine.SnapshotPurge(); err != nil {
		return err
	}
	logrus.Debugf("Started snapshot purge for volume %v", volumeName)
	return nil
}

func (m *VolumeManager) BackupSnapshot(backupName, volumeName, snapshotName, backingImageName, backingImageURL string, labels map[string]string) error {
	if volumeName == "" || snapshotName == "" {
		return fmt.Errorf("volume and snapshot name required")
	}

	if err := m.checkVolumeNotInMigration(volumeName); err != nil {
		return err
	}

	// check backup target is available
	_, err := m.GetSettingValueExisted(types.SettingNameBackupTarget)
	if err != nil {
		return err
	}
	_, err = GetBackupCredentialConfig(m.ds)
	if err != nil {
		return err
	}

	backupCR := &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:   backupName,
			Labels: types.GetVolumeLabels(volumeName),
		},
		Spec: types.BackupSnapshotSpec{
			SnapshotName:    snapshotName,
			Labels:          labels,
			BackingImage:    backingImageName,
			BackingImageURL: backingImageURL,
		},
	}
	_, err = m.ds.CreateBackup(backupCR)
	if err != nil {
		return err
	}
	// Add finalizer to the backup CR
	return m.ds.AddFinalizerForBackup(backupName)
}

func (m *VolumeManager) checkVolumeNotInMigration(volumeName string) error {
	v, err := m.ds.GetVolume(volumeName)
	if err != nil {
		return err
	}
	if v.Spec.MigrationNodeID != "" {
		return fmt.Errorf("cannot operate during migration")
	}
	return nil
}

func (m *VolumeManager) GetEngineClient(volumeName string) (client engineapi.EngineClient, err error) {
	var e *longhorn.Engine

	defer func() {
		err = errors.Wrapf(err, "cannot get client for volume %v", volumeName)
	}()
	es, err := m.ds.ListVolumeEngines(volumeName)
	if err != nil {
		return nil, err
	}
	if len(es) == 0 {
		return nil, fmt.Errorf("cannot find engine")
	}
	if len(es) != 1 {
		return nil, fmt.Errorf("more than one engine exists")
	}
	for _, e = range es {
		break
	}
	if e.Status.CurrentState != types.InstanceStateRunning {
		return nil, fmt.Errorf("engine is not running")
	}
	if isReady, err := m.ds.CheckEngineImageReadiness(e.Status.CurrentImage, m.currentNodeID); !isReady {
		if err != nil {
			return nil, fmt.Errorf("cannot get engine client with image %v: %v", e.Status.CurrentImage, err)
		}
		return nil, fmt.Errorf("cannot get engine client with image %v because it isn't deployed on this node", e.Status.CurrentImage)
	}

	engineCollection := &engineapi.EngineCollection{}
	return engineCollection.NewEngineClient(&engineapi.EngineClientRequest{
		VolumeName:  e.Spec.VolumeName,
		EngineImage: e.Status.CurrentImage,
		IP:          e.Status.IP,
		Port:        e.Status.Port,
	})
}

func (m *VolumeManager) ListBackupVolumes() (map[string]*engineapi.BackupVolume, error) {
	backupVolumes, err := m.ds.ListBackupVolume()
	if err != nil {
		return nil, err
	}

	bvs := make(map[string]*engineapi.BackupVolume)
	for backupVolumeName, backupVolume := range backupVolumes {
		if backupVolume.Status.LastSyncedAt == nil {
			// Skip the backup volume that has not synced yet
			continue
		}

		bvs[backupVolumeName] = &engineapi.BackupVolume{
			Name:             backupVolumeName,
			Size:             backupVolume.Status.Size,
			Labels:           backupVolume.Status.Labels,
			Created:          backupVolume.Status.CreateAt,
			LastBackupName:   backupVolume.Status.LastBackupName,
			LastBackupAt:     backupVolume.Status.LastBackupAt,
			DataStored:       backupVolume.Status.DataStored,
			Messages:         backupVolume.Status.Messages,
			BackingImageName: backupVolume.Status.BackingImageName,
			BackingImageURL:  backupVolume.Status.BackingImageURL,
		}
	}

	// side effect, update known volumes
	SyncVolumesLastBackupWithBackupVolumes(bvs, m.ds.ListVolumes, m.ds.GetVolume, m.ds.UpdateVolumeStatus)

	return bvs, nil
}

func (m *VolumeManager) GetBackupVolume(volumeName string) (*engineapi.BackupVolume, error) {
	backupVolume, err := m.ds.GetBackupVolumeRO(volumeName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return &engineapi.BackupVolume{}, nil
		}
		return nil, err
	}

	if backupVolume.Status.LastSyncedAt == nil {
		// Skip the backup volume that has not synced yet
		return &engineapi.BackupVolume{}, nil
	}

	bv := &engineapi.BackupVolume{
		Name:             volumeName,
		Size:             backupVolume.Status.Size,
		Labels:           backupVolume.Status.Labels,
		Created:          backupVolume.Status.CreateAt,
		LastBackupName:   backupVolume.Status.LastBackupName,
		LastBackupAt:     backupVolume.Status.LastBackupAt,
		DataStored:       backupVolume.Status.DataStored,
		Messages:         backupVolume.Status.Messages,
		BackingImageName: backupVolume.Status.BackingImageName,
		BackingImageURL:  backupVolume.Status.BackingImageURL,
	}
	return bv, nil
}

func (m *VolumeManager) DeleteBackupVolume(volumeName string) error {
	backupVolume, err := m.ds.GetBackupVolumeRO(volumeName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	// Request to delete remote config
	backupVolume.Spec.DeleteRemoteConfig = true
	backupVolume, err = m.ds.UpdateBackupVolume(backupVolume)
	if err != nil && !datastore.ErrorIsConflict(err) {
		return err
	}
	return m.ds.DeleteBackupVolume(volumeName)
}

func (m *VolumeManager) ListBackupsForVolume(volumeName string) ([]*engineapi.Backup, error) {
	backups, err := m.ds.ListBackup(volumeName)
	if err != nil {
		return nil, err
	}

	volumeSnapshotBackups := make([]*engineapi.Backup, 0)
	for backupName, backup := range backups {
		if backup.Status.LastSyncedAt == nil {
			// Skip the backup that has not synced yet
			continue
		}

		volumeSnapshotBackups = append(volumeSnapshotBackups, &engineapi.Backup{
			Name:                   backupName,
			URL:                    backup.Status.URL,
			SnapshotName:           backup.Status.SnapshotName,
			SnapshotCreated:        backup.Status.SnapshotCreateAt,
			Created:                backup.Status.BackupCreateAt,
			Size:                   backup.Status.Size,
			Labels:                 backup.Status.Labels,
			VolumeName:             backup.Status.VolumeName,
			VolumeSize:             backup.Status.VolumeSize,
			VolumeCreated:          backup.Status.VolumeCreated,
			VolumeBackingImageName: backup.Status.VolumeBackingImageName,
			VolumeBackingImageURL:  backup.Status.VolumeBackingImageURL,
			Messages:               backup.Status.Messages,
		})
	}
	return volumeSnapshotBackups, nil
}

func (m *VolumeManager) GetBackup(backupName, volumeName string) (*engineapi.Backup, error) {
	backup, err := m.ds.GetBackupRO(backupName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return &engineapi.Backup{}, nil
		}
		return nil, err
	}

	if backup.Status.LastSyncedAt == nil {
		// Skip the backup that has not synced yet
		return &engineapi.Backup{}, nil
	}

	volumeSnapshotBackup := &engineapi.Backup{
		Name:                   backupName,
		URL:                    backup.Status.URL,
		SnapshotName:           backup.Status.SnapshotName,
		SnapshotCreated:        backup.Status.SnapshotCreateAt,
		Created:                backup.Status.BackupCreateAt,
		Size:                   backup.Status.Size,
		Labels:                 backup.Status.Labels,
		VolumeName:             backup.Status.VolumeName,
		VolumeSize:             backup.Status.VolumeSize,
		VolumeCreated:          backup.Status.VolumeCreated,
		VolumeBackingImageName: backup.Status.VolumeBackingImageName,
		VolumeBackingImageURL:  backup.Status.VolumeBackingImageURL,
		Messages:               backup.Status.Messages,
	}
	return volumeSnapshotBackup, nil
}

func (m *VolumeManager) DeleteBackup(backupName, volumeName string) error {
	backup, err := m.ds.GetBackupRO(backupName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	// Request to delete remote config
	backup.Spec.DeleteRemoteConfig = true
	backup, err = m.ds.UpdateBackup(backup)
	if err != nil && !datastore.ErrorIsConflict(err) {
		return err
	}
	return m.ds.DeleteBackup(backupName)
}

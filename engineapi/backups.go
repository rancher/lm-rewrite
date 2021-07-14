package engineapi

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

type BackupTarget struct {
	URL        string
	Image      string
	Credential map[string]string
}

func NewBackupTarget(backupTarget, engineImage string, credential map[string]string) *BackupTarget {
	return &BackupTarget{
		URL:        backupTarget,
		Image:      engineImage,
		Credential: credential,
	}
}

func (b *BackupTarget) LonghornEngineBinary() string {
	return filepath.Join(types.GetEngineBinaryDirectoryOnHostForImage(b.Image), "longhorn")
}

// getBackupCredentialEnv returns the environment variables as KEY=VALUE in string slice
func getBackupCredentialEnv(backupTarget string, credential map[string]string) ([]string, error) {
	envs := []string{}
	backupType, err := util.CheckBackupType(backupTarget)
	if err != nil {
		return envs, err
	}

	if backupType != types.BackupStoreTypeS3 || credential == nil {
		return envs, nil
	}

	var missingKeys []string
	if credential[types.AWSAccessKey] == "" {
		missingKeys = append(missingKeys, types.AWSAccessKey)
	}
	if credential[types.AWSSecretKey] == "" {
		missingKeys = append(missingKeys, types.AWSSecretKey)
	}
	// If AWS IAM Role not present, then the AWS credentials must be exists
	if credential[types.AWSIAMRoleArn] == "" && len(missingKeys) > 0 {
		return nil, fmt.Errorf("Could not backup to %s, missing %v in the secret", backupType, missingKeys)
	}
	if len(missingKeys) == 0 {
		envs = append(envs, fmt.Sprintf("%s=%s", types.AWSAccessKey, credential[types.AWSAccessKey]))
		envs = append(envs, fmt.Sprintf("%s=%s", types.AWSSecretKey, credential[types.AWSSecretKey]))
	}
	envs = append(envs, fmt.Sprintf("%s=%s", types.AWSEndPoint, credential[types.AWSEndPoint]))
	envs = append(envs, fmt.Sprintf("%s=%s", types.AWSCert, credential[types.AWSCert]))
	envs = append(envs, fmt.Sprintf("%s=%s", types.HTTPSProxy, credential[types.HTTPSProxy]))
	envs = append(envs, fmt.Sprintf("%s=%s", types.HTTPProxy, credential[types.HTTPProxy]))
	envs = append(envs, fmt.Sprintf("%s=%s", types.NOProxy, credential[types.NOProxy]))
	envs = append(envs, fmt.Sprintf("%s=%s", types.VirtualHostedStyle, credential[types.VirtualHostedStyle]))
	return envs, nil
}

func (b *BackupTarget) ExecuteEngineBinary(args ...string) (string, error) {
	envs, err := getBackupCredentialEnv(b.URL, b.Credential)
	if err != nil {
		return "", err
	}
	return util.Execute(envs, b.LonghornEngineBinary(), args...)
}

func (b *BackupTarget) ExecuteEngineBinaryWithoutTimeout(args ...string) (string, error) {
	envs, err := getBackupCredentialEnv(b.URL, b.Credential)
	if err != nil {
		return "", err
	}
	return util.ExecuteWithoutTimeout(envs, b.LonghornEngineBinary(), args...)
}

// parseBackupVolumeNamesList parses a list of backup volume names into a sorted string slice
func parseBackupVolumeNamesList(output string) ([]string, error) {
	data := map[string]struct{}{}
	if err := json.Unmarshal([]byte(output), &data); err != nil {
		return nil, errors.Wrapf(err, "error parsing backup volume names: \n%s", output)
	}

	volumeNames := []string{}
	for volumeName := range data {
		volumeNames = append(volumeNames, volumeName)
	}
	sort.Strings(volumeNames)
	return volumeNames, nil
}

// ListBackupVolumeNames returns a list of backup volume names
func (b *BackupTarget) ListBackupVolumeNames() ([]string, error) {
	output, err := b.ExecuteEngineBinary("backup", "ls", "--volume-only", b.URL)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error listing backup volume names")
	}
	return parseBackupVolumeNamesList(output)
}

// parseBackupNamesList parses a list of backup names into a sorted string slice
func parseBackupNamesList(output, volumeName string) ([]string, error) {
	data := map[string]*BackupVolume{}
	if err := json.Unmarshal([]byte(output), &data); err != nil {
		return nil, errors.Wrapf(err, "error parsing backup names: \n%s", output)
	}

	volumeData, ok := data[volumeName]
	if !ok {
		return nil, fmt.Errorf("cannot find the volume name %s in the data", volumeName)
	}

	backupNames := []string{}
	for backupName := range volumeData.Backups {
		backupNames = append(backupNames, backupName)
	}
	sort.Strings(backupNames)
	return backupNames, nil
}

// ListBackupNames returns a list of backup names
func (b *BackupTarget) ListBackupNames(volumeName string) ([]string, error) {
	if volumeName == "" {
		return nil, nil
	}
	output, err := b.ExecuteEngineBinary("backup", "ls", "--volume", volumeName, b.URL)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error listing volume %s backup", volumeName)
	}
	return parseBackupNamesList(output, volumeName)
}

// DeleteBackupVolume deletes the backup volume from the remote backup target
func (b *BackupTarget) DeleteBackupVolume(volumeName string) error {
	_, err := b.ExecuteEngineBinaryWithoutTimeout("backup", "rm", "--volume", volumeName, b.URL)
	if err != nil {
		if types.ErrorIsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "error deleting backup volume")
	}
	logrus.Infof("Complete deleting backup volume %s", volumeName)
	return nil
}

// parseBackupVolumeConfig parses a backup volume config
func parseBackupVolumeConfig(output string) (*BackupVolume, error) {
	backupVolume := new(BackupVolume)
	if err := json.Unmarshal([]byte(output), backupVolume); err != nil {
		return nil, errors.Wrapf(err, "error parsing one backup volume config: \n%s", output)
	}
	return backupVolume, nil
}

// InspectBackupVolumeConfig inspects a backup volume config with the given volume config URL
func (b *BackupTarget) InspectBackupVolumeConfig(volumeMetadataURL string) (*BackupVolume, error) {
	output, err := b.ExecuteEngineBinary("backup", "inspect-volume", volumeMetadataURL)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error getting backup volume config %s", volumeMetadataURL)
	}
	return parseBackupVolumeConfig(output)
}

// parseBackupConfig parses a backup config
func parseBackupConfig(output string) (*Backup, error) {
	backup := new(Backup)
	if err := json.Unmarshal([]byte(output), backup); err != nil {
		return nil, errors.Wrapf(err, "error parsing one backup config: \n%s", output)
	}
	return backup, nil
}

// InspectBackupConfig inspects a backup config with the given backup config URL
func (b *BackupTarget) InspectBackupConfig(backupMetadataURL string) (*Backup, error) {
	output, err := b.ExecuteEngineBinary("backup", "inspect", backupMetadataURL)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error getting backup config %s", backupMetadataURL)
	}
	return parseBackupConfig(output)
}

// parseConfigMetadata parses the config metadata
func parseConfigMetadata(output string) (*ConfigMetadata, error) {
	metadata := new(ConfigMetadata)
	if err := json.Unmarshal([]byte(output), metadata); err != nil {
		return nil, errors.Wrapf(err, "error parsing config metadata: \n%s", output)
	}
	return metadata, nil
}

// GetConfigMetadata returns the config metadata with the given URL
func (b *BackupTarget) GetConfigMetadata(url string) (*ConfigMetadata, error) {
	output, err := b.ExecuteEngineBinary("backup", "config-metadata", url)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error getting config metadata %s", url)
	}
	return parseConfigMetadata(output)
}

// DeleteBackup deletes the backup from the remote backup target
func (b *BackupTarget) DeleteBackup(backupURL string) error {
	_, err := b.ExecuteEngineBinaryWithoutTimeout("backup", "rm", backupURL)
	if err != nil {
		if types.ErrorIsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "error deleting backup %v", backupURL)
	}
	logrus.Infof("Complete deleting backup %s", backupURL)
	return nil
}

func (e *Engine) SnapshotBackup(backupName, snapName, backupTarget, backingImageName, backingImageURL string, labels map[string]string, credential map[string]string) (string, error) {
	if snapName == VolumeHeadName {
		return "", fmt.Errorf("invalid operation: cannot backup %v", VolumeHeadName)
	}
	snap, err := e.SnapshotGet(snapName)
	if err != nil {
		return "", errors.Wrapf(err, "error getting snapshot '%s', volume '%s'", snapName, e.name)
	}
	if snap == nil {
		return "", errors.Errorf("could not find snapshot '%s' to backup, volume '%s'", snapName, e.name)
	}
	if (backingImageName == "" && backingImageURL != "") || (backingImageName != "" && backingImageURL == "") {
		return "", errors.Errorf("invalid backing image name %v and URL %v for backup creation", backingImageName, backingImageURL)
	}
	args := []string{"backup", "create", "--dest", backupTarget}
	if backingImageName != "" {
		args = append(args, "--backing-image-name", backingImageName, "--backing-image-url", backingImageURL)
	}
	if backupName != "" {
		args = append(args, "--backup-name", backupName)
	}
	for k, v := range labels {
		args = append(args, "--label", k+"="+v)
	}
	args = append(args, snapName)

	// get environment variables if backup for s3
	envs, err := getBackupCredentialEnv(backupTarget, credential)
	if err != nil {
		return "", err
	}
	output, err := e.ExecuteEngineBinaryWithoutTimeout(envs, args...)
	if err != nil {
		return "", err
	}
	backupCreateInfo := BackupCreateInfo{}
	if err := json.Unmarshal([]byte(output), &backupCreateInfo); err != nil {
		return "", err
	}

	logrus.Debugf("Backup %v created for volume %v snapshot %v", backupCreateInfo.BackupID, e.Name(), snapName)
	return backupCreateInfo.BackupID, nil
}

func (e *Engine) SnapshotBackupStatus() (map[string]*types.BackupStatus, error) {
	args := []string{"backup", "status"}
	output, err := e.ExecuteEngineBinary(args...)
	if err != nil {
		return nil, err
	}
	backups := make(map[string]*types.BackupStatus, 0)
	if err := json.Unmarshal([]byte(output), &backups); err != nil {
		return nil, err
	}
	return backups, nil
}

func (e *Engine) BackupRestore(backupTarget, backupName, backupVolumeName, lastRestored string, credential map[string]string) error {
	backup := backupstore.EncodeMetadataURL(backupName, backupVolumeName, backupTarget)

	// get environment variables if backup for s3
	envs, err := getBackupCredentialEnv(backupTarget, credential)
	if err != nil {
		return err
	}

	args := []string{"backup", "restore", backup}
	// TODO: Remove this compatible code and update the function signature
	//  when the manager doesn't support the engine v1.0.0 or older version.
	if lastRestored != "" {
		args = append(args, "--incrementally", "--last-restored", lastRestored)
	}

	if output, err := e.ExecuteEngineBinaryWithoutTimeout(envs, args...); err != nil {
		var taskErr TaskError
		if jsonErr := json.Unmarshal([]byte(output), &taskErr); jsonErr != nil {
			logrus.Warnf("Cannot unmarshal the restore error, maybe it's not caused by the replica restore failure: %v", jsonErr)
			return err
		}
		return taskErr
	}

	logrus.Debugf("Backup %v restored for volume %v", backup, e.Name())
	return nil
}

func (e *Engine) BackupRestoreStatus() (map[string]*types.RestoreStatus, error) {
	args := []string{"backup", "restore-status"}
	output, err := e.ExecuteEngineBinary(args...)
	if err != nil {
		return nil, err
	}
	replicaStatusMap := make(map[string]*types.RestoreStatus)
	if err := json.Unmarshal([]byte(output), &replicaStatusMap); err != nil {
		return nil, err
	}
	return replicaStatusMap, nil
}

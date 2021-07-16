package engineapi

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/mitchellh/mapstructure"
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

func parseBackup(v interface{}) (*Backup, error) {
	backup := new(Backup)
	if err := mapstructure.Decode(v, backup); err != nil {
		return nil, errors.Wrapf(err, "Error parsing backup info %+v", v)
	}
	return backup, nil
}

func parseBackupsList(output, volumeName string) ([]*Backup, error) {
	data := map[string]*BackupVolume{}
	if err := json.Unmarshal([]byte(output), &data); err != nil {
		return nil, errors.Wrapf(err, "error parsing BackupsList: \n%s", output)
	}
	BackupTarget := []*Backup{}
	volume := data[volumeName]
	for _, v := range data[volumeName].Backups {
		backup, err := parseBackup(v)
		if err != nil {
			return nil, err
		}
		backup.VolumeName = volume.Name
		backup.VolumeSize = volume.Size
		backup.VolumeCreated = volume.Created
		BackupTarget = append(BackupTarget, backup)
	}

	return BackupTarget, nil
}

func parseBackupVolumesList(output string) (map[string]*BackupVolume, error) {
	data := map[string]*BackupVolume{}
	if err := json.Unmarshal([]byte(output), &data); err != nil {
		return nil, errors.Wrapf(err, "error parsing BackupVolumesList: \n%s", output)
	}
	volumes := map[string]*BackupVolume{}

	for name, v := range data {
		if v.Messages != nil {
			for mType, mContent := range v.Messages {
				if mType == backupstore.MessageTypeError {
					logrus.Errorf("message from backupVolume[%v], type[%v], content[%v]",
						name, mType, mContent)
				} else {
					logrus.Warnf("message from backupVolume[%v], type[%v], content[%v]",
						name, mType, mContent)
				}
			}
		}
		volumes[name] = &BackupVolume{
			Name:                 name,
			Size:                 v.Size,
			Labels:               v.Labels,
			Created:              v.Created,
			LastBackupName:       v.LastBackupName,
			LastBackupAt:         v.LastBackupAt,
			BackingImageName:     v.BackingImageName,
			BackingImageChecksum: v.BackingImageChecksum,
			DataStored:           v.DataStored,
			Messages:             v.Messages,
		}
	}

	return volumes, nil
}

func parseOneBackup(output string) (*Backup, error) {
	data := map[string]interface{}{}
	if err := json.Unmarshal([]byte(output), &data); err != nil {
		return nil, errors.Wrapf(err, "error parsing one backup: \n%s", output)
	}
	return parseBackup(data)
}

func (b *BackupTarget) ListVolumes() (map[string]*BackupVolume, error) {
	output, err := b.ExecuteEngineBinary("backup", "ls", "--volume-only", b.URL)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error listing backup volumes")
	}
	return parseBackupVolumesList(output)
}

func (b *BackupTarget) GetVolume(volumeName string) (*BackupVolume, error) {
	output, err := b.ExecuteEngineBinary("backup", "ls", "--volume", volumeName, "--volume-only", b.URL)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error getting backup volume")
	}
	list, err := parseBackupVolumesList(output)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting backup volume")
	}
	return list[volumeName], nil
}

func (b *BackupTarget) DeleteVolume(volumeName string) error {
	_, err := b.ExecuteEngineBinaryWithoutTimeout("backup", "rm", "--volume", volumeName, b.URL)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			logrus.Warnf("delete: could not find the backup volume: '%s'", volumeName)
			return nil
		}
		return errors.Wrapf(err, "error deleting backup volume")
	}
	return nil
}
func (b *BackupTarget) List(volumeName string) ([]*Backup, error) {
	if volumeName == "" {
		return nil, nil
	}
	output, err := b.ExecuteEngineBinary("backup", "ls", "--volume", volumeName, b.URL)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error listing backups")
	}
	return parseBackupsList(output, volumeName)
}

func (b *BackupTarget) GetBackup(backupURL string) (*Backup, error) {
	output, err := b.ExecuteEngineBinary("backup", "inspect", backupURL)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error getting backup")
	}
	return parseOneBackup(output)
}

func (b *BackupTarget) DeleteBackup(backupURL string) error {
	logrus.Infof("Start Deleting backup %s", backupURL)
	_, err := b.ExecuteEngineBinaryWithoutTimeout("backup", "rm", backupURL)
	if err != nil {
		if types.ErrorIsNotFound(err) {
			logrus.Warnf("delete: could not find the backup: '%s'", backupURL)
			return nil
		}
		return errors.Wrapf(err, "error deleting backup %v", backupURL)
	}
	logrus.Infof("Complete deleting backup %s", backupURL)
	return nil
}

func GetBackupURL(backupTarget, backupName, volName string) string {
	return fmt.Sprintf("%s?backup=%s&volume=%s", backupTarget, backupName, volName)
}

func (e *Engine) SnapshotBackup(snapName, backupTarget, backingImageName, backingImageChecksum string, labels map[string]string, credential map[string]string) (string, error) {
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
	version, err := e.Version(true)
	if err != nil {
		return "", err
	}
	args := []string{"backup", "create", "--dest", backupTarget}
	if backingImageName != "" {
		args = append(args, "--backing-image-name", backingImageName)
		// TODO: Remove this if there is no backward compatibility
		if version.ClientVersion.CLIAPIVersion <= CLIVersionFour {
			args = append(args, "--backing-image-url", "deprecated-field")
		} else if backingImageChecksum != "" {
			args = append(args, "--backing-image-checksum", backingImageChecksum)
		}
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

func (e *Engine) BackupRestore(backupTarget, backupName, backupVolume, lastRestored string, credential map[string]string) error {
	backup := GetBackupURL(backupTarget, backupName, backupVolume)

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

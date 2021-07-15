package engineapi

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const listBackupVolumeNames = `
{
	"pvc-1": {},
	"pvc-2": {},
	"pvc-3": {}
}
`
const listBackupNames = `
{
	"qq": {
		"Backups": {
			"vfs:///var/lib/longhorn/backups/default?backup=backup-072d7a718f854328\u0026volume=qq": {},
			"vfs:///var/lib/longhorn/backups/default?backup=backup-3ddb6c6a09424a05\u0026volume=qq": {}
		}
	}
}
`

const oneBackupVolumeConfig = `
{
	"Name": "pvc-1",
	"Size": "1073741824",
	"Labels":{
		"KubernetesStatus": "{\"pvName\":\"pvc-1\",\"namespace\":\"default\",\"pvcName\":\"longhorn-1-volv-pvc\",\"workloadsStatus\":[{\"podName\":\"volume-test-2\",\"workloadType\":\"\"}]}",
		"foo": "bar"
	},
	"Created": "2017-03-25T02:26:59Z",
	"LastBackupName": "backup-1",
	"LastBackupAt": "2017-03-25T02:27:00Z",
	"DataStored": "1163919360"
}
`

const oneVolumeSnapshotBackupConfig = `
{
	"Name": "backup-072d7a718f854328",
	"URL": "vfs:///var/lib/longhorn/backups/default?backup=backup-072d7a718f854328\u0026volume=qq",
	"SnapshotName": "volume-snap-snap4.img",
	"SnapshotCreated": "2017-03-25T02:26:59Z",
	"Created": "2017-03-25T02:27:00Z",
	"Size": "169869312",
	"VolumeName": "qq",
	"VolumeSize": "10737418240",
	"VolumeCreated": "2017-03-25T02:25:53Z",
	"VolumeBackingImageName": "",
	"VolumeBackingImageURL":  ""
}
`

const configMetadata = `
{
	"FileTime": "2017-03-25T02:26:59Z"
}
`

func TestGetBackupCredentialEnv(t *testing.T) {
	tests := []struct {
		name         string
		backupTarget string
		credential   map[string]string
		expectError  bool
	}{
		{
			name:         "unsupported backup target",
			backupTarget: "http://localhost",
			credential:   map[string]string{},
		},
		{
			name:         "provides only AWS access key",
			backupTarget: "s3://backupbucket@us-east-1/",
			credential: map[string]string{
				"AWS_ACCESS_KEY_ID": "my-aws-access-key-id",
			},
			expectError: true,
		},
		{
			name:         "provides only AWS secret access key",
			backupTarget: "s3://backupbucket@us-east-1/",
			credential: map[string]string{
				"AWS_SECRET_ACCESS_KEY": "my-aws-secret-access-key",
			},
			expectError: true,
		},
		{
			name:         "either AWS credentials nor AWS IAM role provided",
			backupTarget: "s3://backupbucket@us-east-1/",
			credential:   map[string]string{},
			expectError:  true,
		},
		{
			name:         "provides AWS credential",
			backupTarget: "s3://backupbucket@us-east-1/",
			credential: map[string]string{
				"AWS_ACCESS_KEY_ID":     "my-aws-access-key-id",
				"AWS_SECRET_ACCESS_KEY": "my-aws-secret-access-key",
			},
		},
		{
			name:         "provides AWS IAM role",
			backupTarget: "s3://backupbucket@us-east-1/",
			credential: map[string]string{
				"AWS_IAM_ROLE_ARN": "AWS_IAM_ARN: arn:aws:iam::013456789:role/longhorn",
			},
		},
		{
			name:         "provides both AWS credential and AWS IAM role",
			backupTarget: "s3://backupbucket@us-east-1/",
			credential: map[string]string{
				"AWS_ACCESS_KEY_ID":     "my-aws-access-key-id",
				"AWS_SECRET_ACCESS_KEY": "my-aws-secret-access-key",
				"AWS_IAM_ROLE_ARN":      "AWS_IAM_ARN: arn:aws:iam::013456789:role/longhorn",
			},
		},
		{
			name:         "provides nfs backup target",
			backupTarget: "nfs://longhorn-test-nfs-svc.default:/opt/backupstore",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := require.New(t)

			envs, err := getBackupCredentialEnv(tt.backupTarget, tt.credential)
			if tt.expectError {
				assert.NotNil(err)
			} else {
				assert.Nil(err)

				for _, env := range envs {
					ss := strings.SplitN(env, "=", 2)
					assert.Equal(tt.credential[ss[0]], ss[1])
				}
			}
		})
	}
}

func TestParseBackupVolumeNamesList(t *testing.T) {
	assert := require.New(t)

	backupVolumeNames, err := parseBackupVolumeNamesList(listBackupVolumeNames)
	assert.Nil(err)
	assert.Equal(backupVolumeNames, []string{"pvc-1", "pvc-2", "pvc-3"})
}

func TestParseBackupNamesList(t *testing.T) {
	assert := require.New(t)

	backupNames, err := parseBackupNamesList(listBackupNames, "qq")
	assert.Nil(err)
	assert.Equal(backupNames, []string{
		"vfs:///var/lib/longhorn/backups/default?backup=backup-072d7a718f854328\u0026volume=qq",
		"vfs:///var/lib/longhorn/backups/default?backup=backup-3ddb6c6a09424a05\u0026volume=qq",
	})

	_, err = parseBackupNamesList(listBackupNames, "QQ")
	assert.NotNil(err)
}

func TestParseOneBackupVolumeConfig(t *testing.T) {
	assert := require.New(t)

	volumeConfig, err := parseBackupVolumeConfig(oneBackupVolumeConfig)
	assert.Nil(err)
	assert.Equal(BackupVolume{
		Name: "pvc-1",
		Size: "1073741824",
		Labels: map[string]string{
			"KubernetesStatus": "{\"pvName\":\"pvc-1\",\"namespace\":\"default\",\"pvcName\":\"longhorn-1-volv-pvc\",\"workloadsStatus\":[{\"podName\":\"volume-test-2\",\"workloadType\":\"\"}]}",
			"foo":              "bar",
		},
		Created:        "2017-03-25T02:26:59Z",
		LastBackupName: "backup-1",
		LastBackupAt:   "2017-03-25T02:27:00Z",
		DataStored:     "1163919360",
	}, *volumeConfig)
}

func TestParseBackupConfig(t *testing.T) {
	assert := require.New(t)

	backupConfig, err := parseBackupConfig(oneVolumeSnapshotBackupConfig)
	assert.Nil(err)
	assert.Equal(Backup{
		Name:                   "backup-072d7a718f854328",
		URL:                    "vfs:///var/lib/longhorn/backups/default?backup=backup-072d7a718f854328\u0026volume=qq",
		SnapshotName:           "volume-snap-snap4.img",
		SnapshotCreated:        "2017-03-25T02:26:59Z",
		Created:                "2017-03-25T02:27:00Z",
		Size:                   "169869312",
		VolumeName:             "qq",
		VolumeSize:             "10737418240",
		VolumeCreated:          "2017-03-25T02:25:53Z",
		VolumeBackingImageName: "",
		VolumeBackingImageURL:  "",
	}, *backupConfig)
}

func TestParseConfigMetadata(t *testing.T) {
	assert := require.New(t)

	configMetadata, err := parseConfigMetadata(configMetadata)
	assert.Nil(err)

	modificationTime, err := time.Parse(time.RFC3339, "2017-03-25T02:26:59Z")
	assert.Nil(err)

	assert.Equal(ConfigMetadata{ModificationTime: modificationTime}, *configMetadata)
}

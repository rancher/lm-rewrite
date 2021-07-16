package client

type RancherClient struct {
	RancherBaseClient

	ApiVersion                    ApiVersionOperations
	Error                         ErrorOperations
	AttachInput                   AttachInputOperations
	DetachInput                   DetachInputOperations
	SnapshotInput                 SnapshotInputOperations
	Backup                        BackupOperations
	BackupInput                   BackupInputOperations
	BackupStatus                  BackupStatusOperations
	RestoreStatus                 RestoreStatusOperations
	PurgeStatus                   PurgeStatusOperations
	RebuildStatus                 RebuildStatusOperations
	RecurringJob                  RecurringJobOperations
	ReplicaRemoveInput            ReplicaRemoveInputOperations
	SalvageInput                  SalvageInputOperations
	ActivateInput                 ActivateInputOperations
	ExpandInput                   ExpandInputOperations
	EngineUpgradeInput            EngineUpgradeInputOperations
	Replica                       ReplicaOperations
	Controller                    ControllerOperations
	DiskUpdate                    DiskUpdateOperations
	UpdateReplicaCountInput       UpdateReplicaCountInputOperations
	UpdateReplicaAutoBalanceInput UpdateReplicaAutoBalanceInputOperations
	UpdateDataLocalityInput       UpdateDataLocalityInputOperations
	UpdateAccessModeInput         UpdateAccessModeInputOperations
	WorkloadStatus                WorkloadStatusOperations
	PVCreateInput                 PVCreateInputOperations
	PVCCreateInput                PVCCreateInputOperations
	SettingDefinition             SettingDefinitionOperations
	VolumeCondition               VolumeConditionOperations
	NodeCondition                 NodeConditionOperations
	DiskCondition                 DiskConditionOperations
	SupportBundle                 SupportBundleOperations
	SupportBundleInitateInput     SupportBundleInitateInputOperations
	Tag                           TagOperations
	InstanceManager               InstanceManagerOperations
	BackingImageCleanupInput      BackingImageCleanupInputOperations
	Volume                        VolumeOperations
	Snapshot                      SnapshotOperations
	BackupVolume                  BackupVolumeOperations
	Setting                       SettingOperations
	RecurringInput                RecurringInputOperations
	EngineImage                   EngineImageOperations
	BackingImage                  BackingImageOperations
	Node                          NodeOperations
	DiskUpdateInput               DiskUpdateInputOperations
	DiskInfo                      DiskInfoOperations
	KubernetesStatus              KubernetesStatusOperations
	BackupListOutput              BackupListOutputOperations
	SnapshotListOutput            SnapshotListOutputOperations
	BackingImageDiskFileStatus    BackingImageDiskFileStatusOperations
}

func constructClient(rancherBaseClient *RancherBaseClientImpl) *RancherClient {
	client := &RancherClient{
		RancherBaseClient: rancherBaseClient,
	}

	client.ApiVersion = newApiVersionClient(client)
	client.Error = newErrorClient(client)
	client.AttachInput = newAttachInputClient(client)
	client.DetachInput = newDetachInputClient(client)
	client.SnapshotInput = newSnapshotInputClient(client)
	client.Backup = newBackupClient(client)
	client.BackupInput = newBackupInputClient(client)
	client.BackupStatus = newBackupStatusClient(client)
	client.RestoreStatus = newRestoreStatusClient(client)
	client.PurgeStatus = newPurgeStatusClient(client)
	client.RebuildStatus = newRebuildStatusClient(client)
	client.RecurringJob = newRecurringJobClient(client)
	client.ReplicaRemoveInput = newReplicaRemoveInputClient(client)
	client.SalvageInput = newSalvageInputClient(client)
	client.ActivateInput = newActivateInputClient(client)
	client.ExpandInput = newExpandInputClient(client)
	client.EngineUpgradeInput = newEngineUpgradeInputClient(client)
	client.Replica = newReplicaClient(client)
	client.Controller = newControllerClient(client)
	client.DiskUpdate = newDiskUpdateClient(client)
	client.UpdateReplicaCountInput = newUpdateReplicaCountInputClient(client)
	client.UpdateReplicaAutoBalanceInput = newUpdateReplicaAutoBalanceInputClient(client)
	client.UpdateDataLocalityInput = newUpdateDataLocalityInputClient(client)
	client.UpdateAccessModeInput = newUpdateAccessModeInputClient(client)
	client.WorkloadStatus = newWorkloadStatusClient(client)
	client.PVCreateInput = newPVCreateInputClient(client)
	client.PVCCreateInput = newPVCCreateInputClient(client)
	client.SettingDefinition = newSettingDefinitionClient(client)
	client.VolumeCondition = newVolumeConditionClient(client)
	client.NodeCondition = newNodeConditionClient(client)
	client.DiskCondition = newDiskConditionClient(client)
	client.SupportBundle = newSupportBundleClient(client)
	client.SupportBundleInitateInput = newSupportBundleInitateInputClient(client)
	client.Tag = newTagClient(client)
	client.InstanceManager = newInstanceManagerClient(client)
	client.BackingImageCleanupInput = newBackingImageCleanupInputClient(client)
	client.Volume = newVolumeClient(client)
	client.Snapshot = newSnapshotClient(client)
	client.BackupVolume = newBackupVolumeClient(client)
	client.Setting = newSettingClient(client)
	client.RecurringInput = newRecurringInputClient(client)
	client.EngineImage = newEngineImageClient(client)
	client.BackingImage = newBackingImageClient(client)
	client.Node = newNodeClient(client)
	client.DiskUpdateInput = newDiskUpdateInputClient(client)
	client.DiskInfo = newDiskInfoClient(client)
	client.KubernetesStatus = newKubernetesStatusClient(client)
	client.BackupListOutput = newBackupListOutputClient(client)
	client.SnapshotListOutput = newSnapshotListOutputClient(client)
	client.BackingImageDiskFileStatus = newBackingImageDiskFileStatusClient(client)

	return client
}

func NewRancherClient(opts *ClientOpts) (*RancherClient, error) {
	rancherBaseClient := &RancherBaseClientImpl{
		Types: map[string]Schema{},
	}
	client := constructClient(rancherBaseClient)

	err := setupRancherBaseClient(rancherBaseClient, opts)
	if err != nil {
		return nil, err
	}

	return client, nil
}

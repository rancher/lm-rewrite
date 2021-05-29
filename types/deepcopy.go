package types

import "github.com/longhorn/backupstore"

func (v *VolumeSpec) DeepCopyInto(to *VolumeSpec) {
	*to = *v
	if v.DiskSelector != nil {
		to.DiskSelector = make([]string, len(v.DiskSelector))
		for i := 0; i < len(v.DiskSelector); i++ {
			to.DiskSelector[i] = v.DiskSelector[i]
		}
	}
	if v.NodeSelector != nil {
		to.NodeSelector = make([]string, len(v.NodeSelector))
		for i := 0; i < len(v.NodeSelector); i++ {
			to.NodeSelector[i] = v.NodeSelector[i]
		}
	}
	if v.RecurringJobs != nil {
		to.RecurringJobs = make([]RecurringJob, len(v.RecurringJobs))
		for i := 0; i < len(v.RecurringJobs); i++ {
			toRecurringJob := v.RecurringJobs[i]
			if v.RecurringJobs[i].Labels != nil {
				toRecurringJob.Labels = make(map[string]string)
				for key, value := range v.RecurringJobs[i].Labels {
					toRecurringJob.Labels[key] = value
				}
			}
			to.RecurringJobs[i] = toRecurringJob
		}
	}
}

func (v *VolumeStatus) DeepCopyInto(to *VolumeStatus) {
	*to = *v
	if v.Conditions != nil {
		to.Conditions = make(map[string]Condition)
		for key, value := range v.Conditions {
			to.Conditions[key] = value
		}
	}

	if v.KubernetesStatus.WorkloadsStatus != nil {
		to.KubernetesStatus.WorkloadsStatus = make([]WorkloadStatus, len(v.KubernetesStatus.WorkloadsStatus))
		copy(to.KubernetesStatus.WorkloadsStatus, v.KubernetesStatus.WorkloadsStatus)
	}
}

func (e *EngineSpec) DeepCopyInto(to *EngineSpec) {
	*to = *e
	if e.ReplicaAddressMap != nil {
		to.ReplicaAddressMap = make(map[string]string)
		for key, value := range e.ReplicaAddressMap {
			to.ReplicaAddressMap[key] = value
		}
	}
	if e.UpgradedReplicaAddressMap != nil {
		to.UpgradedReplicaAddressMap = make(map[string]string)
		for key, value := range e.UpgradedReplicaAddressMap {
			to.UpgradedReplicaAddressMap[key] = value
		}
	}
}

func (e *EngineStatus) DeepCopyInto(to *EngineStatus) {
	*to = *e
	if e.BackupStatus != nil {
		to.BackupStatus = make(map[string]*BackupStatus)
		for key, value := range e.BackupStatus {
			to.BackupStatus[key] = &BackupStatus{}
			*to.BackupStatus[key] = *value
		}
	}
	if e.ReplicaModeMap != nil {
		to.ReplicaModeMap = make(map[string]ReplicaMode)
		for key, value := range e.ReplicaModeMap {
			to.ReplicaModeMap[key] = value
		}
	}
	if e.RestoreStatus != nil {
		to.RestoreStatus = make(map[string]*RestoreStatus)
		for key, value := range e.RestoreStatus {
			to.RestoreStatus[key] = &RestoreStatus{}
			*to.RestoreStatus[key] = *value
		}
	}
	if e.PurgeStatus != nil {
		to.PurgeStatus = make(map[string]*PurgeStatus)
		for key, value := range e.PurgeStatus {
			to.PurgeStatus[key] = &PurgeStatus{}
			*to.PurgeStatus[key] = *value
		}
	}
	if e.RebuildStatus != nil {
		to.RebuildStatus = make(map[string]*RebuildStatus)
		for key, value := range e.RebuildStatus {
			to.RebuildStatus[key] = &RebuildStatus{}
			*to.RebuildStatus[key] = *value
		}
	}
	if e.Snapshots != nil {
		to.Snapshots = make(map[string]*Snapshot)
		for key, source := range e.Snapshots {
			to.Snapshots[key] = &Snapshot{}
			*to.Snapshots[key] = *source
			out := to.Snapshots[key]

			if source.Children != nil {
				out.Children = make(map[string]bool)
				for key, value := range source.Children {
					out.Children[key] = value
				}
			}

			if source.Labels != nil {
				out.Labels = make(map[string]string)
				for key, value := range source.Labels {
					out.Labels[key] = value
				}
			}
		}
	}
}

func (n *NodeSpec) DeepCopyInto(to *NodeSpec) {
	*to = *n
	if n.Disks != nil {
		to.Disks = make(map[string]DiskSpec)
		for key, value := range n.Disks {
			toDisk := value
			if value.Tags != nil {
				toDisk.Tags = make([]string, len(value.Tags))
				for i := 0; i < len(value.Tags); i++ {
					toDisk.Tags[i] = value.Tags[i]
				}
			}
			to.Disks[key] = toDisk
		}
	}
	if n.Tags != nil {
		to.Tags = make([]string, len(n.Tags))
		for i := 0; i < len(n.Tags); i++ {
			to.Tags[i] = n.Tags[i]
		}
	}
}

func (n *NodeStatus) DeepCopyInto(to *NodeStatus) {
	*to = *n
	if n.DiskStatus != nil {
		to.DiskStatus = make(map[string]*DiskStatus)
		for key, value := range n.DiskStatus {
			toDiskStatus := &DiskStatus{}
			value.DeepCopyInto(toDiskStatus)
			to.DiskStatus[key] = toDiskStatus
		}
	}
	if n.Conditions != nil {
		to.Conditions = make(map[string]Condition)
		for key, value := range n.Conditions {
			to.Conditions[key] = value
		}
	}
}

func (n *DiskStatus) DeepCopyInto(to *DiskStatus) {
	*to = *n
	if n.Conditions != nil {
		to.Conditions = make(map[string]Condition)
		for key, value := range n.Conditions {
			to.Conditions[key] = value
		}
	}
	if n.ScheduledReplica != nil {
		to.ScheduledReplica = make(map[string]int64)
		for key, value := range n.ScheduledReplica {
			to.ScheduledReplica[key] = value
		}
	}
}

func (n *InstanceManagerStatus) DeepCopyInto(to *InstanceManagerStatus) {
	*to = *n
	if n.Instances != nil {
		to.Instances = make(map[string]InstanceProcess)
		for key, value := range n.Instances {
			to.Instances[key] = value
		}
	}
}

func (ei *EngineImageStatus) DeepCopyInto(to *EngineImageStatus) {
	*to = *ei
	if ei.Conditions != nil {
		to.Conditions = make(map[string]Condition)
		for key, value := range ei.Conditions {
			to.Conditions[key] = value
		}
	}
	if ei.NodeDeploymentMap != nil {
		to.NodeDeploymentMap = make(map[string]bool)
		for key, value := range ei.NodeDeploymentMap {
			to.NodeDeploymentMap[key] = value
		}
	}
}

func (bi *BackingImageSpec) DeepCopyInto(to *BackingImageSpec) {
	*to = *bi
	if bi.Disks != nil {
		to.Disks = map[string]struct{}{}
		for key, value := range bi.Disks {
			to.Disks[key] = value
		}
	}
}

func (bi *BackingImageStatus) DeepCopyInto(to *BackingImageStatus) {
	*to = *bi
	if bi.DiskDownloadStateMap != nil {
		to.DiskDownloadStateMap = make(map[string]BackingImageDownloadState)
		for key, value := range bi.DiskDownloadStateMap {
			to.DiskDownloadStateMap[key] = value
		}
	}
	if bi.DiskDownloadProgressMap != nil {
		to.DiskDownloadProgressMap = make(map[string]int)
		for key, value := range bi.DiskDownloadProgressMap {
			to.DiskDownloadProgressMap[key] = value
		}
	}
	if bi.DiskLastRefAtMap != nil {
		to.DiskLastRefAtMap = make(map[string]string)
		for key, value := range bi.DiskLastRefAtMap {
			to.DiskLastRefAtMap[key] = value
		}
	}
}

func (bim *BackingImageManagerSpec) DeepCopyInto(to *BackingImageManagerSpec) {
	*to = *bim
	if bim.BackingImages != nil {
		to.BackingImages = make(map[string]string)
		for key, value := range bim.BackingImages {
			to.BackingImages[key] = value
		}
	}
}

func (bim *BackingImageManagerStatus) DeepCopyInto(to *BackingImageManagerStatus) {
	*to = *bim
	if bim.BackingImageFileMap != nil {
		to.BackingImageFileMap = make(map[string]BackingImageFileInfo)
		for key, value := range bim.BackingImageFileMap {
			to.BackingImageFileMap[key] = value
		}
	}
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *BackupTargetSpec) DeepCopyInto(out *BackupTargetSpec) {
	*out = *in
	out.PollInterval = in.PollInterval
	return
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *BackupTargetStatus) DeepCopyInto(out *BackupTargetStatus) {
	*out = *in
	if in.LastSyncedAt != nil {
		in, out := &in.LastSyncedAt, &out.LastSyncedAt
		*out = (*in).DeepCopy()
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new BackupTargetStatus.
func (in *BackupTargetStatus) DeepCopy() *BackupTargetStatus {
	if in == nil {
		return nil
	}
	out := new(BackupTargetStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new BackupTargetSpec.
func (in *BackupTargetSpec) DeepCopy() *BackupTargetSpec {
	if in == nil {
		return nil
	}
	out := new(BackupTargetSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *BackupVolumeSpec) DeepCopyInto(out *BackupVolumeSpec) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new BackupVolumeSpec.
func (in *BackupVolumeSpec) DeepCopy() *BackupVolumeSpec {
	if in == nil {
		return nil
	}
	out := new(BackupVolumeSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *BackupVolumeStatus) DeepCopyInto(out *BackupVolumeStatus) {
	*out = *in
	if in.Labels != nil {
		in, out := &in.Labels, &out.Labels
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	if in.Messages != nil {
		in, out := &in.Messages, &out.Messages
		*out = make(map[backupstore.MessageType]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	if in.LastSyncedAt != nil {
		in, out := &in.LastSyncedAt, &out.LastSyncedAt
		*out = (*in).DeepCopy()
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new BackupVolumeStatus.
func (in *BackupVolumeStatus) DeepCopy() *BackupVolumeStatus {
	if in == nil {
		return nil
	}
	out := new(BackupVolumeStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *BackupSnapshotSpec) DeepCopyInto(out *BackupSnapshotSpec) {
	*out = *in
	if in.Labels != nil {
		in, out := &in.Labels, &out.Labels
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new BackupSnapshotSpec.
func (in *BackupSnapshotSpec) DeepCopy() *BackupSnapshotSpec {
	if in == nil {
		return nil
	}
	out := new(BackupSnapshotSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *BackupSnapshotStatus) DeepCopyInto(out *BackupSnapshotStatus) {
	*out = *in
	if in.Labels != nil {
		in, out := &in.Labels, &out.Labels
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	if in.Messages != nil {
		in, out := &in.Messages, &out.Messages
		*out = make(map[backupstore.MessageType]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	if in.LastSyncedAt != nil {
		in, out := &in.LastSyncedAt, &out.LastSyncedAt
		*out = (*in).DeepCopy()
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new BackupStatus.
func (in *BackupSnapshotStatus) DeepCopy() *BackupSnapshotStatus {
	if in == nil {
		return nil
	}
	out := new(BackupSnapshotStatus)
	in.DeepCopyInto(out)
	return out
}

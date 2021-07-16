package csi

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

const (
	// we wait 1m30s for the volume state polling, this leaves 20s for the rest of the function call
	timeoutAttachDetach     = 90 * time.Second
	tickAttachDetach        = 2 * time.Second
	timeoutBackupInitiation = 60 * time.Second
	tickBackupInitiation    = 5 * time.Second
)

type ControllerServer struct {
	apiClient   *longhornclient.RancherClient
	nodeID      string
	caps        []*csi.ControllerServiceCapability
	accessModes []*csi.VolumeCapability_AccessMode
}

func NewControllerServer(apiClient *longhornclient.RancherClient, nodeID string) *ControllerServer {
	return &ControllerServer{
		apiClient: apiClient,
		nodeID:    nodeID,
		caps: getControllerServiceCapabilities(
			[]csi.ControllerServiceCapability_RPC_Type{
				csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
				csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
				csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
				csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
			}),
		accessModes: getVolumeCapabilityAccessModes(
			[]csi.VolumeCapability_AccessMode_Mode{
				csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			}),
	}
}

func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {

	volumeID := util.AutoCorrectName(req.GetName(), datastore.NameMaximumLength)
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}
	volumeCaps := req.GetVolumeCapabilities()
	if err := cs.validateVolumeCapabilities(volumeCaps); err != nil {
		return nil, err
	}
	volumeParameters := req.GetParameters()
	if volumeParameters == nil {
		volumeParameters = map[string]string{}
	}
	var reqVolSizeBytes int64
	if req.GetCapacityRange() != nil {
		reqVolSizeBytes = req.GetCapacityRange().GetRequiredBytes()
	}
	if reqVolSizeBytes < util.MinimalVolumeSize {
		logrus.Infof("volume %s requested capacity %v is smaller than minimal capacity %v, enforcing minimal capacity.", volumeID, reqVolSizeBytes, util.MinimalVolumeSize)
		reqVolSizeBytes = util.MinimalVolumeSize
	}
	// Round up to multiple of 2 * 1024 * 1024
	reqVolSizeBytes = util.RoundUpSize(reqVolSizeBytes)

	// check if we need to restore from a csi snapshot
	// we don't support volume cloning at the moment
	source := req.VolumeContentSource
	if source != nil {
		if source.GetVolume() != nil {
			return nil, status.Error(codes.InvalidArgument, "volume cloning not supported")
		}

		snapshot := source.GetSnapshot()
		_, backupVolume, backupName := decodeSnapshotID(snapshot.SnapshotId)
		bv, err := cs.apiClient.BackupVolume.ById(backupVolume)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "cannot restore csi snapshot %s backup volume %s unavailable", snapshot.SnapshotId, backupVolume)
		}

		backup, err := cs.apiClient.BackupVolume.ActionBackupGet(bv, &longhornclient.BackupInput{Name: backupName})
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "cannot restore csi snapshot %v backup %s unavailable", snapshot.SnapshotId, backupName)
		}

		// use the fromBackup method for the csi snapshot restores as well
		// the same parameter was previously only used for restores based on the storage class
		volumeParameters["fromBackup"] = backup.Url
	}

	existVol, err := cs.apiClient.Volume.ById(volumeID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if existVol != nil {
		logrus.Debugf("CreateVolume: got an exist volume: %s", existVol.Name)

		exVolSize, err := util.ConvertSize(existVol.Size)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		if exVolSize != reqVolSizeBytes {
			return nil, status.Errorf(codes.AlreadyExists, "volume %s size %v differs from requested size %v", existVol.Name, exVolSize, reqVolSizeBytes)
		}

		// pass through the volume content source in case this volume is in the process of being created
		rsp := &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      existVol.Id,
				CapacityBytes: exVolSize,
				VolumeContext: volumeParameters,
				ContentSource: source,
			},
		}

		return rsp, nil
	}

	// regardless of the used storage class, if this is requested in rwx mode
	// we need to mark the volume as a shared volume
	for _, cap := range volumeCaps {
		if requiresSharedAccess(nil, cap) {
			volumeParameters["share"] = "true"
			break
		}
	}

	vol, err := getVolumeOptions(volumeParameters)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if vol.BackingImage != "" {
		// There will be an empty BackingImage object rather than nil returned even if there is an error
		existingBackingImage, err := cs.apiClient.BackingImage.ById(vol.BackingImage)
		if err != nil && !strings.Contains(err.Error(), "not found") {
			return nil, status.Errorf(codes.Internal, "volume %s unable to retrieve backing image %s error %v", volumeID, vol.BackingImage, err)
		}
		// A new backing image will be created automatically only if:
		//   1. there is no existing backing image named `backingImage`
		//   2. there is valid data source type as well as corresponding parameters
		if existingBackingImage == nil || existingBackingImage.Name == "" {
			bidsType := ""
			bidsParameters := map[string]string{}
			switch strings.ToLower(volumeParameters["backingImageDataSourceType"]) {
			case string(types.BackingImageDataSourceTypeDownload):
				bidsType = string(types.BackingImageDataSourceTypeDownload)
				parametersStr := volumeParameters["backingImageDataSourceParameters"]
				if err := json.Unmarshal([]byte(parametersStr), &bidsParameters); err != nil {
					return nil, status.Errorf(codes.InvalidArgument, "volume %s unable to create missing backing image with parameters %s, error: %v", volumeID, parametersStr, err)
				}
			case "":
				// backward compatibility
				if volumeParameters["backingImageURL"] == "" {
					return nil, status.Errorf(codes.NotFound, "volume %s missing backing image %v unable to create volume", volumeID, vol.BackingImage)
				}
				bidsType = string(types.BackingImageDataSourceTypeDownload)
				bidsParameters[types.DataSourceTypeDownloadParameterURL] = volumeParameters["backingImageURL"]
			default:
				return nil, status.Errorf(codes.InvalidArgument, "volume %s unable to create missing backing image %v with data source type %v", volumeID, vol.BackingImage, volumeParameters["backingImageDataSourceType"])
			}

			if _, err := cs.apiClient.BackingImage.Create(&longhornclient.BackingImage{
				Name:             vol.BackingImage,
				ExpectedChecksum: volumeParameters["backingImageChecksum"],
				SourceType:       bidsType,
				Parameters:       bidsParameters,
			}); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
		}
	}

	vol.Name = volumeID
	vol.Size = fmt.Sprintf("%d", reqVolSizeBytes)

	logrus.Infof("CreateVolume: creating a volume by API client, name: %s, size: %s accessMode: %v", vol.Name, vol.Size, vol.AccessMode)
	resVol, err := cs.apiClient.Volume.Create(vol)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Do we have a better condition than this?
	checkVolumeCreated := func(vol *longhornclient.Volume) bool {
		return vol.State == string(types.VolumeStateDetached)
	}

	if !cs.waitForVolumeState(resVol.Id, "volume created", checkVolumeCreated, true, false) {
		return nil, status.Error(codes.DeadlineExceeded, "cannot wait for volume creation to complete")
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      resVol.Id,
			CapacityBytes: reqVolSizeBytes,
			VolumeContext: volumeParameters,
			ContentSource: source,
		},
	}, nil
}

func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}

	existVol, err := cs.apiClient.Volume.ById(volumeID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if existVol == nil {
		logrus.Infof("DeleteVolume: volume %s not found", volumeID)
		return &csi.DeleteVolumeResponse{}, nil
	}

	logrus.Debugf("DeleteVolume: volume %s exists", volumeID)
	if err = cs.apiClient.Volume.Delete(existVol); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	checkVolumeDeleted := func(vol *longhornclient.Volume) bool {
		return vol == nil
	}
	if !cs.waitForVolumeState(req.GetVolumeId(), "volume deleted", checkVolumeDeleted, false, true) {
		return nil, status.Errorf(codes.DeadlineExceeded, "failed to delete volume %s", volumeID)
	}

	logrus.Infof("DeleteVolume: volume %s deleted", volumeID)
	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: cs.caps,
	}, nil
}

func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}

	existVol, err := cs.apiClient.Volume.ById(volumeID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if existVol == nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", volumeID)
	}

	if err := cs.validateVolumeCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeContext:      req.GetVolumeContext(),
			VolumeCapabilities: req.GetVolumeCapabilities(),
			Parameters:         req.GetParameters(),
		},
	}, nil
}

// ControllerPublishVolume will attach the volume to the specified node
func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}

	nodeID := req.GetNodeId()
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node id missing in request")
	}

	volumeCapability := req.GetVolumeCapability()
	if volumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability missing in request")
	}

	// TODO: #1875 API returns error instead of not found, so we cannot differenciate between a retrieval failure and non existing resource
	if _, err := cs.apiClient.Node.ById(nodeID); err != nil {
		return nil, status.Errorf(codes.NotFound, "node %s not found", nodeID)
	}

	volume, err := cs.apiClient.Volume.ById(volumeID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if volume == nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", volumeID)
	}

	if volume.Frontend != string(types.VolumeFrontendBlockDev) {
		return nil, status.Errorf(codes.InvalidArgument, "volume %s invalid frontend type %s", volumeID, volume.Frontend)
	}

	if requiresSharedAccess(volume, volumeCapability) {
		volume, err = cs.updateVolumeAccessMode(volume, types.AccessModeReadWriteMany)
		if err != nil {
			return nil, err
		}
	}

	// TODO: JM Restore should be handled by the volume attach call, consider returning `codes.Aborted`
	// TODO: JM should readiness be handled by the caller?
	//  Most of the readiness conditions are covered by the attach, except auto attachment which requires changes to the design
	//  should be handled by the processing of the api return codes
	if !volume.Ready {
		return nil, status.Errorf(codes.Aborted, "volume %s in state %v is not ready for workloads", volumeID, volume.State)
	}

	// TODO: JM if volume is already attached to a different node, return code `codes.FailedPrecondition`
	//  this should be handled by the processing of the api return code
	if !requiresSharedAccess(volume, volumeCapability) &&
		volume.State == string(types.VolumeStateAttached) &&
		len(volume.Controllers) > 0 && volume.Controllers[0].HostId != nodeID {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %s cannot be attached to node %s is already attached to node %s",
			volumeID, nodeID, volume.Controllers[0].HostId)
	}

	return cs.publishVolume(volume, nodeID, func() error {
		checkVolumePublished := func(vol *longhornclient.Volume) bool {
			return isVolumeAvailableOn(vol, nodeID) || isVolumeShareAvailable(vol)
		}
		if !cs.waitForVolumeState(volumeID, "volume published", checkVolumePublished, false, false) {
			return status.Errorf(codes.DeadlineExceeded, "volume %s failed to attach to node %s", volumeID, nodeID)
		}
		return nil
	})
}

// publishVolume sends the actual attach request to the longhorn api and executes the passed waitForResult func
func (cs *ControllerServer) publishVolume(volume *longhornclient.Volume, nodeID string, waitForResult func() error) (*csi.ControllerPublishVolumeResponse, error) {
	logrus.Debugf("ControllerPublishVolume: volume %s is ready to be attached, and the requested node is %s", volume.Name, nodeID)
	input := &longhornclient.AttachInput{
		HostId:          nodeID,
		DisableFrontend: false,
	}

	logrus.Infof("ControllerPublishVolume: volume %s with accessMode %s requesting publishing to %s", volume.Name, volume.AccessMode, nodeID)
	if _, err := cs.apiClient.Volume.ActionAttach(volume, input); err != nil {
		// TODO: JM process the returned error and return the correct error responses for kubernetes
		//  i.e. FailedPrecondition if the RWO volume is already attached to a different node
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := waitForResult(); err != nil {
		return nil, err
	}

	logrus.Infof("ControllerPublishVolume: volume %s with accessMode %s published to %s", volume.Name, volume.AccessMode, nodeID)
	return &csi.ControllerPublishVolumeResponse{}, nil
}

func (cs *ControllerServer) updateVolumeAccessMode(volume *longhornclient.Volume, accessMode types.AccessMode) (*longhornclient.Volume, error) {
	mode := string(accessMode)
	if volume.AccessMode == mode {
		return volume, nil
	}

	volumeName := volume.Name
	input := &longhornclient.UpdateAccessModeInput{AccessMode: mode}
	volume, err := cs.apiClient.Volume.ActionUpdateAccessMode(volume, input)
	if err != nil {
		logrus.WithError(err).Errorf("Failed to change Volume %s access mode to %s", volumeName, mode)
		return nil, status.Error(codes.Internal, err.Error())
	}

	logrus.Infof("Changed Volume %s access mode to %s", volumeName, mode)
	return volume, nil
}

// ControllerUnpublishVolume will detach the volume
func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}

	// if nodeID == "" means to detach from all nodes
	nodeID := req.GetNodeId()

	volume, err := cs.apiClient.Volume.ById(volumeID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// VOLUME_NOT_FOUND is no longer the ControllerUnpublishVolume error
	// See https://github.com/container-storage-interface/spec/issues/382 for details
	if volume == nil {
		logrus.Infof("ControllerUnpublishVolume: volume %s no longer exists", volumeID)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	return cs.unpublishVolume(volume, nodeID, func() error {
		isSharedVolume := requiresSharedAccess(volume, nil) && !volume.Migratable
		checkVolumeUnpublished := func(vol *longhornclient.Volume) bool {
			return isSharedVolume || isVolumeUnavailableOn(vol, nodeID)
		}

		if !cs.waitForVolumeState(volumeID, "volume unpublished", checkVolumeUnpublished, false, true) {
			return status.Errorf(codes.DeadlineExceeded, "Failed to detach volume %s from node %s", volumeID, volumeID)
		}
		return nil
	})
}

// unpublishVolume sends the actual detach request to the longhorn api and executes the passed waitForResult func
func (cs *ControllerServer) unpublishVolume(volume *longhornclient.Volume, nodeID string, waitForResult func() error) (*csi.ControllerUnpublishVolumeResponse, error) {
	logrus.Debugf("requesting Volume %s detachment for %s", volume.Name, nodeID)
	_, err := cs.apiClient.Volume.ActionDetach(volume, &longhornclient.DetachInput{HostId: nodeID})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err = waitForResult(); err != nil {
		return nil, err
	}

	logrus.Debugf("Volume %s unpublished from %s", volume.Name, nodeID)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *ControllerServer) ListVolumes(context.Context, *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) GetCapacity(context.Context, *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	csiLabels := req.Parameters
	csiSnapshotName := req.GetName()
	csiVolumeName := req.GetSourceVolumeId()
	if len(csiVolumeName) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume name must be provided")
	} else if len(csiSnapshotName) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot name must be provided")
	}

	// we check for backup existence first, since it's possible that
	// the actual volume is no longer available but the backup still is.
	backupVolume, err := cs.apiClient.BackupVolume.ById(csiVolumeName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	backupListOutput, err := cs.apiClient.BackupVolume.ActionBackupList(backupVolume)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// NOTE: csi-snapshots assume a 1 to 1 relationship, longhorn allows for multiple backups of a snapshot
	var backup *longhornclient.Backup
	for _, b := range backupListOutput.Data {
		if b.SnapshotName == csiSnapshotName {
			backup = &b
			break
		}
	}

	// since there is a backup file for this on the backupstore we can assume successful completion
	// since the backup.cfg only gets written after all the blocks have been transferred
	if backup != nil {
		rsp := createSnapshotResponse(backup.VolumeName, backup.Name, backup.SnapshotCreated, backup.VolumeSize, 100)
		return rsp, nil
	}

	existVol, err := cs.apiClient.Volume.ById(csiVolumeName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if existVol == nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", csiVolumeName)
	}

	var snapshot *longhornclient.Snapshot
	snapshotListOutput, err := cs.apiClient.Volume.ActionSnapshotList(existVol)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	for _, snap := range snapshotListOutput.Data {
		if snap.Name == csiSnapshotName {
			snapshot = &snap
			break
		}
	}

	// if a backup has been deleted from the backupstore but the runtime information is still present in the volume
	// we want to create a new backup same as if the backup operation has failed
	backupStatus, err := cs.getBackupStatus(csiVolumeName, csiSnapshotName)
	if err != nil {
		return nil, err
	}

	if backupStatus != nil && backupStatus.Progress != 100 && backupStatus.Error == "" {
		var creationTime string
		if snapshot != nil {
			creationTime = snapshot.Created
		}

		rsp := createSnapshotResponse(csiVolumeName, backupStatus.Id, creationTime, existVol.Size, int(backupStatus.Progress))
		return rsp, nil
	}

	// no existing backup and no local snapshot, create a new one
	if snapshot == nil {
		logrus.Infof("CreateSnapshot: volume %s initiating snapshot %s", existVol.Name, csiSnapshotName)
		snapshot, err = cs.apiClient.Volume.ActionSnapshotCreate(existVol, &longhornclient.SnapshotInput{
			Labels: csiLabels,
			Name:   csiSnapshotName,
		})

		// failed to create snapshot, so there is no way to backup
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	// create backup based on local volume snapshot
	logrus.Infof("CreateSnapshot: volume %s initiating backup for snapshot %s", existVol.Name, csiSnapshotName)
	existVol, err = cs.apiClient.Volume.ActionSnapshotBackup(existVol, &longhornclient.SnapshotInput{
		Labels: csiLabels,
		Name:   csiSnapshotName,
	})

	// failed to kick off backup
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// we need to wait for backup initiation since we only know the backupID after the fact
	backupStatus, err = cs.waitForBackupInitiation(csiVolumeName, csiSnapshotName)
	if err != nil {
		return nil, err
	}

	logrus.Infof("CreateSnapshot: volume %s backup %s of snapshot %s in progress", existVol.Name, backupStatus.Id, csiSnapshotName)
	rsp := createSnapshotResponse(existVol.Name, backupStatus.Id, snapshot.Created, existVol.Size, int(backupStatus.Progress))
	logrus.Debugf("ControllerServer CreateSnapshot rsp: %v", rsp)
	return rsp, nil
}

func createSnapshotResponse(volumeName, backupName, snapshotTime, volumeSize string, progress int) *csi.CreateSnapshotResponse {
	creationTime, err := toProtoTimestamp(snapshotTime)
	if err != nil {
		logrus.Errorf("Failed to parse creation time %v for backup %v", snapshotTime, backupName)
	}

	size, _ := util.ConvertSize(volumeSize)
	size = util.RoundUpSize(size)
	snapshotID := encodeSnapshotID(volumeName, backupName)
	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SizeBytes:      size,
			SnapshotId:     snapshotID,
			SourceVolumeId: volumeName,
			CreationTime:   creationTime,
			ReadyToUse:     progress == 100,
		},
	}
}

// encodeSnapshotID encodes the backup volume as part of the snapshotID
// so we don't need to iterate over all backup volumes,
// when trying to find a backup for deletion or restoration
func encodeSnapshotID(volumeName, backupName string) string {
	return fmt.Sprintf("bs://%s/%s", volumeName, backupName)
}

// decodeSnapshotID splits up the snapshotID back into it's components
// backupType will be used once we implement the VolumeBackup crd
func decodeSnapshotID(snapshotID string) (backupType, volumeName, backupName string) {
	split := strings.Split(snapshotID, "://")
	if len(split) < 2 {
		return "", "", snapshotID
	}
	backupType = split[0]

	split = strings.Split(split[1], "/")
	volumeName = split[0]
	backupName = split[1]

	return backupType, volumeName, backupName
}

func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	snapshotID := req.GetSnapshotId()
	if len(snapshotID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing snapshot id in request")
	}

	_, backupVolumeName, backupName := decodeSnapshotID(snapshotID)
	backupVolume, err := cs.apiClient.BackupVolume.ById(backupVolumeName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if backupVolume != nil && backupVolume.Name != "" {
		backupVolume, err = cs.apiClient.BackupVolume.ActionBackupDelete(backupVolume, &longhornclient.BackupInput{Name: backupName})
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

func (cs *ControllerServer) ListSnapshots(context.Context, *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}

	if req.CapacityRange == nil {
		return nil, status.Error(codes.InvalidArgument, "capacity range missing in request")
	}
	requestedSize := req.CapacityRange.GetRequiredBytes()

	existVol, err := cs.apiClient.Volume.ById(volumeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	if existVol == nil {
		return nil, status.Errorf(codes.NotFound, "volume %s missing", volumeID)
	}
	if len(existVol.Controllers) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "volume %s invalid controller count %v", volumeID, len(existVol.Controllers))
	}
	// Support offline expansion only
	if existVol.State != string(types.VolumeStateDetached) {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %s invalid state %v for expansion", volumeID, existVol.State)
	}
	existingSize, err := strconv.ParseInt(existVol.Size, 10, 64)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	if existVol, err = cs.apiClient.Volume.ActionExpand(existVol, &longhornclient.ExpandInput{
		Size: strconv.FormatInt(requestedSize, 10),
	}); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	// kubernetes doesn't support volume shrinking and the csi spec specifies to return true
	// in the case where the current capacity is bigger or equal to the requested capacity
	// that's why we return the volumeSize below instead of the requested capacity
	volumeExpansionComplete := func(vol *longhornclient.Volume) bool {
		engineReady := false
		if len(vol.Controllers) > 0 {
			engine := vol.Controllers[0]
			engineSize, _ := strconv.ParseInt(engine.Size, 10, 64)
			engineReady = engineSize >= requestedSize && !engine.IsExpanding
		}
		size, _ := strconv.ParseInt(vol.Size, 10, 64)
		return size >= requestedSize && engineReady
	}

	// we wait for completion of the expansion, to ensure that longhorn and kubernetes state are in sync
	// should this time out kubernetes will retry the expansion call since the call is idempotent
	// we will exit early if the volume already has the requested size
	if !cs.waitForVolumeState(volumeID, "volume expansion", volumeExpansionComplete, false, false) {
		return nil, status.Errorf(codes.DeadlineExceeded, "volume %s expansion from existing capacity %v to requested capacity %v failed",
			volumeID, existingSize, requestedSize)
	}

	volumeSize, err := strconv.ParseInt(existVol.Size, 10, 64)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         volumeSize,
		NodeExpansionRequired: false,
	}, nil
}

// isVolumeAvailableOn checks that the volume is attached and that an engine is running on the requested node
func isVolumeAvailableOn(vol *longhornclient.Volume, node string) bool {
	return vol.State == string(types.VolumeStateAttached) && isEngineOnNodeAvailable(vol, node)
}

// isVolumeUnavailableOn checks that the volume is not attached to the requested node
func isVolumeUnavailableOn(vol *longhornclient.Volume, node string) bool {
	isValidState := vol.State == string(types.VolumeStateAttached) || vol.State == string(types.VolumeStateDetached)
	return isValidState && !isEngineOnNodeAvailable(vol, node)
}

func isEngineOnNodeAvailable(vol *longhornclient.Volume, node string) bool {
	for _, controller := range vol.Controllers {
		if controller.HostId == node && controller.Endpoint != "" {
			return true
		}
	}

	return false
}

func isVolumeShareAvailable(vol *longhornclient.Volume) bool {
	return vol.AccessMode == string(types.AccessModeReadWriteMany) &&
		vol.ShareState == string(types.ShareManagerStateRunning) && vol.ShareEndpoint != ""
}

func (cs *ControllerServer) waitForVolumeState(volumeID string, stateDescription string,
	predicate func(vol *longhornclient.Volume) bool, notFoundRetry, notFoundReturn bool) bool {
	timer := time.NewTimer(timeoutAttachDetach)
	defer timer.Stop()
	timeout := timer.C

	ticker := time.NewTicker(tickAttachDetach)
	defer ticker.Stop()
	tick := ticker.C

	for {
		select {
		case <-timeout:
			logrus.Warnf("waitForVolumeState: timeout while waiting for volume %s state %s", volumeID, stateDescription)
			return false
		case <-tick:
			logrus.Debugf("Polling volume %s state for %s at %s", volumeID, stateDescription, time.Now().String())
			existVol, err := cs.apiClient.Volume.ById(volumeID)
			if err != nil {
				logrus.Warnf("waitForVolumeState: error while waiting for volume %s state %s error %s", volumeID, stateDescription, err)
				continue
			}
			if existVol == nil {
				logrus.Warnf("waitForVolumeState: volume %s does not exist", volumeID)
				if notFoundRetry {
					continue
				}
				return notFoundReturn
			}
			if predicate(existVol) {
				return true
			}
		}
	}
}

// waitForBackupInitiation polls the volumes backup status till there is a backup in progress
// this is necessary since the backup name is only known after the backup is initiated
func (cs *ControllerServer) waitForBackupInitiation(volumeName, snapshotName string) (*longhornclient.BackupStatus, error) {
	timer := time.NewTimer(timeoutBackupInitiation)
	defer timer.Stop()
	timeout := timer.C

	ticker := time.NewTicker(tickBackupInitiation)
	defer ticker.Stop()
	tick := ticker.C

	for {
		select {
		case <-timeout:
			msg := fmt.Sprintf("waitForBackupInitiation: timeout while waiting for backup initiation for volume %s for snapshot %s", volumeName, snapshotName)
			logrus.Warn(msg)
			return nil, status.Error(codes.DeadlineExceeded, msg)
		case <-tick:

			backupStatus, err := cs.getBackupStatus(volumeName, snapshotName)
			if err != nil {
				return nil, err
			}

			if backupStatus != nil {
				logrus.Infof("Backup %v initiated for volume %v for snapshot %v", backupStatus.Id, volumeName, snapshotName)
				return backupStatus, nil
			}
		}
	}
}

func (cs *ControllerServer) getBackupStatus(volumeName, snapshotName string) (*longhornclient.BackupStatus, error) {
	existVol, err := cs.apiClient.Volume.ById(volumeName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if existVol == nil {
		msg := fmt.Sprintf("could not retrieve backup status the volume %s doesn't exist", volumeName)
		logrus.Warn(msg)
		return nil, status.Error(codes.NotFound, msg)
	}

	var backupStatus *longhornclient.BackupStatus
	for _, status := range existVol.BackupStatus {
		if status.Snapshot == snapshotName {
			backupStatus = &status
			break
		}
	}

	return backupStatus, nil
}

func (cs *ControllerServer) validateVolumeCapabilities(volumeCaps []*csi.VolumeCapability) error {
	if volumeCaps == nil {
		return status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
	}

	for _, cap := range volumeCaps {
		if cap.GetMount() == nil && cap.GetBlock() == nil {
			return status.Error(codes.InvalidArgument, "cannot have both mount and block access type be undefined")
		}
		if cap.GetMount() != nil && cap.GetBlock() != nil {
			return status.Error(codes.InvalidArgument, "cannot have both block and mount access type")
		}

		supportedMode := false
		for _, m := range cs.accessModes {
			if cap.GetAccessMode().GetMode() == m.GetMode() {
				supportedMode = true
				break
			}
		}
		if !supportedMode {
			return status.Errorf(codes.InvalidArgument, "access mode %v is not supported", cap.GetAccessMode().Mode.String())
		}
	}

	return nil
}

func getControllerServiceCapabilities(cl []csi.ControllerServiceCapability_RPC_Type) []*csi.ControllerServiceCapability {
	var cscs []*csi.ControllerServiceCapability

	for _, cap := range cl {
		logrus.Infof("Enabling controller service capability: %v", cap.String())
		cscs = append(cscs, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		})
	}

	return cscs
}

func getVolumeCapabilityAccessModes(vc []csi.VolumeCapability_AccessMode_Mode) []*csi.VolumeCapability_AccessMode {
	var vca []*csi.VolumeCapability_AccessMode
	for _, c := range vc {
		logrus.Infof("Enabling volume access mode: %v", c.String())
		vca = append(vca, &csi.VolumeCapability_AccessMode{Mode: c})
	}
	return vca
}

func toProtoTimestamp(s string) (*timestamp.Timestamp, error) {
	t, err := util.ParseTimeZ(s)
	if err != nil {
		return nil, err
	}

	return ptypes.TimestampProto(t)
}

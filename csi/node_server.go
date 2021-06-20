package csi

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"golang.org/x/sys/unix"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/kubernetes/pkg/util/mount"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	"github.com/longhorn/longhorn-manager/csi/nfs"
	"github.com/longhorn/longhorn-manager/types"
)

type NodeServer struct {
	apiClient *longhornclient.RancherClient
	nodeID    string
	locks     *OperationLocks
	caps      []*csi.NodeServiceCapability
}

func NewNodeServer(apiClient *longhornclient.RancherClient, nodeID string) *NodeServer {
	return &NodeServer{
		apiClient: apiClient,
		nodeID:    nodeID,
		locks:     NewOperationLocks(),
		caps: getNodeServiceCapabilities(
			[]csi.NodeServiceCapability_RPC_Type{
				csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
			}),
	}
}

// NodePublishVolume will mount the volume /dev/longhorn/<volume_name> to target_path
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path missing in request")
	}

	volumeCapability := req.GetVolumeCapability()
	if volumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability missing in request")
	}

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}

	if req.GetReadonly() {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %s does not support readonly mode", volumeID)
	}

	logrus.Debugf(OperationTryLockFMT, "NodePublishVolume", volumeID)
	if !ns.locks.TryAcquire(volumeID) {
		return nil, status.Errorf(codes.Aborted, OperationPendingFMT, volumeID)
	}
	defer ns.locks.Release(volumeID)

	volume, err := ns.apiClient.Volume.ById(volumeID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if volume == nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", volumeID)
	}

	// For mount volumes, we don't want multiple controllers for a volume, since the filesystem could get messed up
	if len(volume.Controllers) == 0 || (len(volume.Controllers) > 1 && volumeCapability.GetBlock() == nil) {
		return nil, status.Errorf(codes.InvalidArgument, "volume %s invalid controller count %v", volumeID, len(volume.Controllers))
	}

	if volume.DisableFrontend || volume.Frontend != string(types.VolumeFrontendBlockDev) {
		return nil, status.Errorf(codes.InvalidArgument, "volume %s invalid frontend type %v is disabled %v", volumeID, volume.Frontend, volume.DisableFrontend)
	}

	// Check volume attachment status
	if volume.State != string(types.VolumeStateAttached) || volume.Controllers[0].Endpoint == "" {
		return nil, status.Errorf(codes.InvalidArgument, "volume %s hasn't been attached yet", volumeID)
	}

	if !volume.Ready {
		return nil, status.Errorf(codes.Aborted, "volume %s is not ready for workloads", volumeID)
	}

	devicePath := volume.Controllers[0].Endpoint
	if requiresSharedAccess(volume, volumeCapability) && !volume.Migratable {

		if volume.AccessMode != string(types.AccessModeReadWriteMany) {
			return nil, status.Errorf(codes.FailedPrecondition, "volume %s requires shared access but is not marked for shared use", volumeID)
		}

		if !isVolumeShareAvailable(volume) {
			return nil, status.Errorf(codes.Aborted, "volume %s share not yet available", volumeID)
		}

		// namespace mounter that operates in the host namespace
		nse, err := nfs.NewNsEnter()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create nsenter executor, err: %v", err)
		}
		nfsMounter := nfs.NewMounter(nse)
		return ns.nodePublishSharedVolume(volumeID, volume.ShareEndpoint, targetPath, nfsMounter)
	} else if volumeCapability.GetBlock() != nil {
		mounter := &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: mount.NewOSExec()}
		return ns.nodePublishBlockVolume(volumeID, devicePath, targetPath, mounter)
	} else if volumeCapability.GetMount() != nil {
		userExt4Params, _ := ns.apiClient.Setting.ById(string(types.SettingNameMkfsExt4Parameters))

		// mounter assumes ext4 by default
		fsType := volumeCapability.GetMount().GetFsType()
		if fsType == "" {
			fsType = "ext4"
		}

		mounter := &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: mount.NewOSExec()}
		// we allow the user to provide additional params for ext4 filesystem creation.
		// this allows an ext4 fs to be mounted on older kernels, see https://github.com/longhorn/longhorn/issues/1208
		if fsType == "ext4" && userExt4Params != nil && userExt4Params.Value != "" {
			ext4Params := userExt4Params.Value
			logrus.Infof("volume %v using user provided ext4 fs creation params: %s", volumeID, ext4Params)
			cmdParamMapping := map[string]string{"mkfs." + fsType: ext4Params}
			mounter = &mount.SafeFormatAndMount{
				Interface: mount.New(""),
				Exec:      NewForcedParamsOsExec(cmdParamMapping),
			}
		}

		return ns.nodePublishMountVolume(volumeID, devicePath, targetPath,
			fsType, volumeCapability.GetMount().GetMountFlags(), mounter)
	}

	return nil, status.Errorf(codes.InvalidArgument, "volume %v does not support volume capability, neither Mount nor Block specified", volumeID)
}

func (ns *NodeServer) nodePublishSharedVolume(volumeName, shareEndpoint, targetPath string, mounter mount.Interface) (*csi.NodePublishVolumeResponse, error) {
	// It's used to check if a directory is a mount point and it will create the directory if not exist. Hence this target path cannot be used for block volume.
	notMnt, err := isLikelyNotMountPointAttach(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !notMnt {
		logrus.Debugf("NodePublishVolume: the volume %s has already been mounted", volumeName)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	uri, err := url.Parse(shareEndpoint)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid share endpoint %v for volume %v", shareEndpoint, volumeName)
	}

	// share endpoint is of the form nfs://server/export
	fsType := uri.Scheme
	if fsType != "nfs" {
		return nil, status.Errorf(codes.InvalidArgument, "Unsupported share type %v for volume %v share endpoint %v", fsType, volumeName, shareEndpoint)
	}

	server := uri.Host
	exportPath := uri.Path
	export := fmt.Sprintf("%s:%s", server, exportPath)
	mountOptions := []string{
		"vers=4.1",
		"noresvport",
		"soft", // for this release we use soft mode, so we can always cleanup mount points
		"sync",
		"intr",
		"timeo=30",  // This is tenths of a second, so a 3 second timeout, each retrans the timeout will be linearly increased, 3s, 6s, 9s
		"retrans=3", // We try the io operation for a total of 3 times, before failing, max runtime of 18s
		// "clientaddr=" // TODO: try to set the client address of the mount to the ip of the pod that is consuming the volume
	}

	if err := mounter.Mount(export, targetPath, fsType, mountOptions); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	logrus.Infof("NodePublishVolume: mounted shared volume %v on node %v via share endpoint %v", volumeName, ns.nodeID, shareEndpoint)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *NodeServer) nodePublishMountVolume(volumeName, devicePath, targetPath, fsType string, mountFlags []string, mounter *mount.SafeFormatAndMount) (*csi.NodePublishVolumeResponse, error) {
	isMnt, err := ensureMountPoint(targetPath, mounter)
	if err != nil {
		msg := fmt.Sprintf("NodePublishVolume: failed to prepare mount point for volume %v error %v", volumeName, err)
		logrus.Error(msg)
		return nil, status.Error(codes.Internal, msg)
	}

	if isMnt {
		logrus.Debugf("NodePublishVolume: found existing healthy mount point for volume %v skipping mount", volumeName)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if err := mounter.FormatAndMount(devicePath, targetPath, fsType, mountFlags); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	logrus.Debugf("NodePublishVolume: done MountVolume %s", volumeName)

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *NodeServer) nodePublishBlockVolume(volumeName, devicePath, targetPath string, mounter *mount.SafeFormatAndMount) (*csi.NodePublishVolumeResponse, error) {
	// we ensure the parent directory exists and is valid
	if _, err := ensureMountPoint(filepath.Dir(targetPath), mounter); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to prepare mount point for block device %v error %v", devicePath, err)
	}

	// create file where we can bind mount the device to
	if err := makeFile(targetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "Error in making file %v", err)
	}

	if err := mounter.Mount(devicePath, targetPath, "", []string{"bind"}); err != nil {
		if removeErr := os.Remove(targetPath); removeErr != nil {
			return nil, status.Errorf(codes.Internal, "Could not remove mount target %q: %v", targetPath, err)
		}
		return nil, status.Errorf(codes.Internal, "Could not mount %q at %q: %v", devicePath, targetPath, err)
	}
	logrus.Debugf("NodePublishVolume: done BlockVolume %s", volumeName)

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path missing in request")
	}

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}

	logrus.Debugf(OperationTryLockFMT, "NodeUnpublishVolume", volumeID)
	if !ns.locks.TryAcquire(volumeID) {
		return nil, status.Errorf(codes.Aborted, OperationPendingFMT, volumeID)
	}
	defer ns.locks.Release(volumeID)

	if err := cleanupMountPoint(targetPath, mount.New("")); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to cleanup volume %s mount point %v error %v", volumeID, targetPath, err))
	}

	logrus.Infof("NodeUnpublishVolume: volume %s unmounted from path %s", volumeID, targetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *NodeServer) NodeStageVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest) (
	*csi.NodeStageVolumeResponse, error) {

	return nil, status.Error(codes.Unimplemented, "")
}

func (ns *NodeServer) NodeUnstageVolume(
	ctx context.Context,
	req *csi.NodeUnstageVolumeRequest) (
	*csi.NodeUnstageVolumeResponse, error) {

	return nil, status.Error(codes.Unimplemented, "")
}

func (ns *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	volumePath := req.GetVolumePath()
	if volumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume path missing in request")
	}

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}

	logrus.Debugf(OperationTryLockFMT, "NodeGetVolumeStats", volumeID)
	if !ns.locks.TryAcquire(volumeID) {
		return nil, status.Errorf(codes.Aborted, OperationPendingFMT, volumeID)
	}
	defer ns.locks.Release(volumeID)

	existVol, err := ns.apiClient.Volume.ById(volumeID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if existVol == nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", volumeID)
	}

	isBlockVolume, err := isBlockDevice(volumePath)
	if err != nil {
		// ENOENT means the volumePath does not exist
		// See https://man7.org/linux/man-pages/man2/stat.2.html for details.
		if errors.Is(err, unix.ENOENT) {
			return nil, status.Errorf(codes.NotFound, "volume %v is not mounted on path %v", volumeID, volumePath)
		}
		return nil, status.Errorf(codes.Internal, "failed to check volume mode for volume path %v: %v", volumePath, err)
	}

	if isBlockVolume {
		volCapacity, err := strconv.ParseInt(existVol.Size, 10, 64)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to convert volume size %v: %v", existVol.Size, err)
		}
		return &csi.NodeGetVolumeStatsResponse{
			Usage: []*csi.VolumeUsage{
				&csi.VolumeUsage{
					Total: volCapacity,
					Unit:  csi.VolumeUsage_BYTES,
				},
			},
		}, nil
	}

	stats, err := getFilesystemStatistics(volumePath)
	if err != nil {
		// ENOENT means the volumePath does not exist
		// See http://man7.org/linux/man-pages/man2/statfs.2.html for details.
		if errors.Is(err, unix.ENOENT) {
			return nil, status.Errorf(codes.NotFound, "volume %v is not mounted on path %v", volumeID, volumePath)
		}
		return nil, status.Errorf(codes.Internal, "failed to retrieve capacity statistics for volume path %v: %v", volumePath, err)
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			&csi.VolumeUsage{
				Available: stats.availableBytes,
				Total:     stats.totalBytes,
				Used:      stats.usedBytes,
				Unit:      csi.VolumeUsage_BYTES,
			},
			&csi.VolumeUsage{
				Available: stats.availableInodes,
				Total:     stats.totalInodes,
				Used:      stats.usedInodes,
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}, nil
}

// NodeExpandVolume is designed to expand the file system for ONLINE expansion,
// But Longhorn supports OFFLINE expansion only.
func (ns *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	volumePath := req.GetVolumePath()
	if volumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume path missing in request")
	}

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}

	logrus.Debugf(OperationTryLockFMT, "NodeExpandVolume", volumeID)
	if !ns.locks.TryAcquire(volumeID) {
		return nil, status.Errorf(codes.Aborted, OperationPendingFMT, volumeID)
	}
	defer ns.locks.Release(volumeID)

	return nil, status.Errorf(codes.Unimplemented, "volume %s cannot be expanded driver only supports offline expansion", volumeID)
}

func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId:            ns.nodeID,
		MaxVolumesPerNode: 0, // technically the scsi kernel limit is the max limit of volumes
	}, nil
}

func (ns *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: ns.caps,
	}, nil
}

func getNodeServiceCapabilities(cs []csi.NodeServiceCapability_RPC_Type) []*csi.NodeServiceCapability {
	var nscs []*csi.NodeServiceCapability

	for _, cap := range cs {
		logrus.Infof("Enabling node service capability: %v", cap.String())
		nscs = append(nscs, &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: cap,
				},
			},
		})
	}

	return nscs
}

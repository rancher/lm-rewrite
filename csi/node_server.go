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
	caps      []*csi.NodeServiceCapability
}

func NewNodeServer(apiClient *longhornclient.RancherClient, nodeID string) *NodeServer {
	return &NodeServer{
		apiClient: apiClient,
		nodeID:    nodeID,
		caps: getNodeServiceCapabilities(
			[]csi.NodeServiceCapability_RPC_Type{
				csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
			}),
	}
}

// NodePublishVolume will mount the volume /dev/longhorn/<volume_name> to target_path
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	logrus.Infof("NodeServer NodePublishVolume req: %v", req)

	targetPath := req.GetTargetPath()
	if targetPath == "" {
		msg := fmt.Sprint("NodePublishVolume: missing target path in request")
		logrus.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	volumeCapability := req.GetVolumeCapability()
	if volumeCapability == nil {
		msg := fmt.Sprint("NodePublishVolume: missing volume capability in request")
		logrus.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	existVol, err := ns.apiClient.Volume.ById(req.GetVolumeId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if existVol == nil {
		msg := fmt.Sprintf("NodePublishVolume: the volume %s not exists", req.GetVolumeId())
		logrus.Warn(msg)
		return nil, status.Error(codes.NotFound, msg)
	}

	if len(existVol.Controllers) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "There should be a controller for volume %s", req.GetVolumeId())
	}

	// For mount volumes, we don't want multiple controllers for a volume, since the filesystem could get messed up
	if len(existVol.Controllers) > 1 && volumeCapability.GetBlock() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "There should be only one controller for volume %s", req.GetVolumeId())
	}

	// Check volume frontend settings
	if existVol.DisableFrontend || existVol.Frontend != string(types.VolumeFrontendBlockDev) {
		return nil, status.Errorf(codes.InvalidArgument, "There is no block device frontend for volume %s", req.GetVolumeId())
	}

	// Check volume attachment status
	if existVol.State != string(types.VolumeStateAttached) || existVol.Controllers[0].Endpoint == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Volume %s hasn't been attached yet", req.GetVolumeId())
	}

	if !existVol.Ready {
		return nil, status.Errorf(codes.Aborted, "The attached volume %s should be ready for workloads before the mount", req.GetVolumeId())
	}

	readOnly := req.GetReadonly()
	if readOnly {
		return nil, status.Error(codes.FailedPrecondition, "Not support readOnly")
	}

	devicePath := existVol.Controllers[0].Endpoint
	if requiresSharedAccess(existVol, volumeCapability) && !existVol.Migratable {

		if existVol.AccessMode != string(types.AccessModeReadWriteMany) {
			return nil, status.Errorf(codes.FailedPrecondition, "The volume %s requires shared access but is not marked for shared use", req.GetVolumeId())
		}

		if !isVolumeShareAvailable(existVol) {
			return nil, status.Errorf(codes.Aborted, "The volume %s share should be available before the mount", req.GetVolumeId())
		}

		// namespace mounter that operates in the host namespace
		nse, err := nfs.NewNsEnter()
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("Failed to create nsenter executor, err: %v", err))
		}
		nfsMounter := nfs.NewMounter(nse)
		return ns.nodePublishSharedVolume(req.GetVolumeId(), existVol.ShareEndpoint, targetPath, nfsMounter)
	} else if volumeCapability.GetBlock() != nil {
		mounter := &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: mount.NewOSExec()}
		return ns.nodePublishBlockVolume(req.GetVolumeId(), devicePath, targetPath, mounter)
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
			logrus.Infof("enabling user provided ext4 fs creation params: %s for volume: %s", ext4Params, req.GetVolumeId())
			cmdParamMapping := map[string]string{"mkfs." + fsType: ext4Params}
			mounter = &mount.SafeFormatAndMount{
				Interface: mount.New(""),
				Exec:      NewForcedParamsOsExec(cmdParamMapping),
			}
		}

		return ns.nodePublishMountVolume(req.GetVolumeId(), devicePath, targetPath,
			fsType, volumeCapability.GetMount().GetMountFlags(), mounter)
	}

	return nil, status.Error(codes.InvalidArgument, "Invalid volume capability, neither Mount nor Block")
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
	logrus.Infof("NodeServer NodeUnpublishVolume req: %v", req)

	if req.GetVolumeId() == "" {
		msg := fmt.Sprint("NodeUnpublishVolume: missing volume id in request")
		logrus.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	targetPath := req.GetTargetPath()
	if targetPath == "" {
		msg := fmt.Sprint("NodeUnpublishVolume: missing target path in request")
		logrus.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	if err := cleanupMountPoint(targetPath, mount.New("")); err != nil {
		msg := fmt.Sprintf("NodeUnpublishVolume: failed to cleanup mount point %v error %v", targetPath, err)
		logrus.Warn(msg)
		return nil, status.Error(codes.Internal, fmt.Sprintf("NodeUnpublishVolume: failed to cleanup mount point %v error %v", targetPath, err))
	}
	logrus.Infof("NodeUnpublishVolume: unmounted volume %s from path %s", req.GetVolumeId(), targetPath)

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
	if req.GetVolumeId() == "" {
		msg := "NodeGetVolumeStats: missing volume id in request"
		logrus.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	if req.GetVolumePath() == "" {
		msg := "NodeGetVolumeStats: missing volume path in request"
		logrus.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	if req.GetStagingTargetPath() != "" {
		return nil, status.Error(codes.FailedPrecondition, "Not support stagingTargetPath")
	}

	existVol, err := ns.apiClient.Volume.ById(req.GetVolumeId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if existVol == nil {
		msg := fmt.Sprintf("NodeGetVolumeStats: the volume %v not exists", req.GetVolumeId())
		logrus.Warn(msg)
		return nil, status.Error(codes.NotFound, msg)
	}

	volumePath := req.GetVolumePath()
	if volumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeGetVolumeStats volume Volume Path cannot be empty")
	}

	isBlockVolume, err := isBlockDevice(volumePath)
	if err != nil {
		// ENOENT means the volumePath does not exist
		// See https://man7.org/linux/man-pages/man2/stat.2.html for details.
		if errors.Is(err, unix.ENOENT) {
			return nil, status.Errorf(codes.NotFound, "volume path %v is not mounted", volumePath)
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
			return nil, status.Errorf(codes.NotFound, "volume path %v is not mounted", volumePath)
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
	return nil, status.Errorf(codes.Unimplemented, "")
}

func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.nodeID,
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

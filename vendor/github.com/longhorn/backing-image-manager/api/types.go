package api

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/longhorn/backing-image-manager/pkg/rpc"
)

type BackingImage struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	UUID      string `json:"uuid"`
	Size      int64  `json:"size"`
	Directory string `json:"directory"`

	Status BackingImageStatus `json:"status"`
}

type BackingImageStatus struct {
	State                string `json:"state"`
	SendingReference     int    `json:"sendingReference"`
	ErrorMsg             string `json:"errorMsg"`
	SenderManagerAddress string `json:"senderManagerAddress"`
	Progress             int    `json:"progress"`
	UploadPort           int32  `json:"uploadPort"`
}

func RPCToBackingImage(obj *rpc.BackingImageResponse) *BackingImage {
	return &BackingImage{
		Name:      obj.Spec.Name,
		URL:       obj.Spec.Url,
		UUID:      obj.Spec.Uuid,
		Size:      obj.Spec.Size,
		Directory: obj.Spec.Directory,

		Status: BackingImageStatus{
			State:                obj.Status.State,
			SendingReference:     int(obj.Status.SendingReference),
			ErrorMsg:             obj.Status.ErrorMsg,
			SenderManagerAddress: obj.Status.SenderManagerAddress,
			Progress:             int(obj.Status.Progress),
			UploadPort:           obj.Status.UploadPort,
		},
	}
}

func RPCToBackingImageList(obj *rpc.ListResponse) map[string]*BackingImage {
	ret := map[string]*BackingImage{}
	for name, bi := range obj.BackingImages {
		ret[name] = RPCToBackingImage(bi)
	}
	return ret
}

type BackingImageStream struct {
	conn      *grpc.ClientConn
	ctxCancel context.CancelFunc
	stream    rpc.BackingImageManagerService_WatchClient
}

func NewBackingImageStream(conn *grpc.ClientConn, ctxCancel context.CancelFunc, stream rpc.BackingImageManagerService_WatchClient) *BackingImageStream {
	return &BackingImageStream{
		conn,
		ctxCancel,
		stream,
	}
}

func (s *BackingImageStream) Close() error {
	s.ctxCancel()
	if err := s.conn.Close(); err != nil {
		return errors.Wrapf(err, "error closing backing image watcher gRPC connection")
	}
	return nil
}

func (s *BackingImageStream) Recv() error {
	_, err := s.stream.Recv()
	return err
}

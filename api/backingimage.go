package api

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

func (s *Server) BackingImageList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	bil, err := s.backingImageList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(bil)
	return nil
}

func (s *Server) backingImageList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	biList, err := s.m.ListBackingImagesSorted()
	if err != nil {
		return nil, errors.Wrap(err, "error listing backing image")
	}
	bidsMap, err := s.m.ListBackingImageDataSources()
	if err != nil {
		return nil, errors.Wrap(err, "error listing backing image")
	}
	for _, bi := range biList {
		if _, exists := bidsMap[bi.Name]; exists {
			continue
		}
		bids, err := s.prepareBackingImageDataSource(bi)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get data source for backing image %v during backing image listing", bi.Name)
		}
		bidsMap[bi.Name] = bids
	}
	return toBackingImageCollection(biList, bidsMap, apiContext), nil
}

func (s *Server) BackingImageGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	id := mux.Vars(req)["name"]

	bi, err := s.m.GetBackingImage(id)
	if err != nil {
		return errors.Wrapf(err, "error get backing image '%s'", id)
	}
	bids, err := s.prepareBackingImageDataSource(bi)
	if err != nil {
		return errors.Wrapf(err, "error get backing image data source '%s'", id)
	}

	apiContext.Write(toBackingImageResource(bi, bids, apiContext))
	return nil
}

func (s *Server) BackingImageCreate(rw http.ResponseWriter, req *http.Request) error {
	var input BackingImage
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}

	bi, bids, err := s.m.CreateBackingImage(input.Name, input.ExpectedChecksum, input.SourceType, input.Parameters)
	if err != nil {
		return errors.Wrapf(err, "unable to create backing image %v from source type %v with parameters %+v", input.Name, input.SourceType, input.Parameters)
	}
	apiContext.Write(toBackingImageResource(bi, bids, apiContext))
	return nil
}

func (s *Server) BackingImageDelete(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	if err := s.m.DeleteBackingImage(id); err != nil {
		return errors.Wrap(err, "unable to delete backing image")
	}

	return nil
}

func (s *Server) BackingImageCleanup(rw http.ResponseWriter, req *http.Request) error {
	var input BackingImageCleanupInput
	apiContext := api.GetApiContext(req)

	id := mux.Vars(req)["name"]
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	bi, err := s.m.CleanUpBackingImageInDisks(id, input.Disks)
	if err != nil {
		return errors.Wrapf(err, "unable to cleanup backing image %v for disk %+v", id, input.Disks)
	}
	bids, err := s.prepareBackingImageDataSource(bi)
	if err != nil {
		return errors.Wrapf(err, "error get engine image data source '%s'", id)
	}
	apiContext.Write(toBackingImageResource(bi, bids, apiContext))
	return nil
}

func (s *Server) prepareBackingImageDataSource(bi *longhorn.BackingImage) (*longhorn.BackingImageDataSource, error) {
	// For deleting backing image, the related data source object may be cleaned up.
	if bi.DeletionTimestamp != nil {
		return nil, nil
	}
	return s.getBackingImageDataSourceWithRetry(bi.Name)
}

func (s *Server) getBackingImageDataSourceWithRetry(name string) (*longhorn.BackingImageDataSource, error) {
	obj, err := util.RetryOnNotFoundCause(func() (interface{}, error) {
		return s.m.GetBackingImageDataSource(name)
	})
	if err != nil {
		return nil, err
	}
	bids, ok := obj.(*longhorn.BackingImageDataSource)
	if !ok {
		return nil, fmt.Errorf("BUG: cannot convert to backing image data source %v object", name)
	}
	return bids, nil
}

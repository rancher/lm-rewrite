package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
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
	list, err := s.m.ListBackingImages()
	if err != nil {
		return nil, errors.Wrap(err, "error listing backing image")
	}
	return toBackingImageCollection(list, apiContext), nil
}

func (s *Server) BackingImageGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	id := mux.Vars(req)["name"]

	bi, err := s.m.GetBackingImage(id)
	if err != nil {
		return errors.Wrapf(err, "error get engine image '%s'", id)
	}
	apiContext.Write(toBackingImageResource(bi, apiContext))
	return nil
}

func (s *Server) BackingImageCreate(rw http.ResponseWriter, req *http.Request) error {
	var input BackingImage
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}

	bi, err := s.m.CreateBackingImage(input.Name, input.ImageURL, input.RequireUpload)
	if err != nil {
		return errors.Wrapf(err, "unable to create backing image %v for URL %v", input.Name, input.ImageURL)
	}
	apiContext.Write(toBackingImageResource(bi, apiContext))
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
	apiContext.Write(toBackingImageResource(bi, apiContext))
	return nil
}

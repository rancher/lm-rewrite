package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"

	"github.com/gorilla/mux"
	bsutil "github.com/longhorn/backupstore/util"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

func (s *Server) SnapshotCreate(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "fail to create snapshot")
	}()
	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	volName := mux.Vars(req)["name"]

	vol, err := s.m.Get(volName)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}

	if vol.Status.IsStandby {
		return fmt.Errorf("cannot create snapshot for standby volume %v", vol.Name)
	}

	snapshot, err := s.m.CreateSnapshot(input.Name, input.Labels, volName)
	if err != nil {
		return err
	}
	apiContext.Write(toSnapshotResource(snapshot))
	return nil
}

func (s *Server) SnapshotList(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "fail to list snapshot")
	}()

	volName := mux.Vars(req)["name"]

	snapList, err := s.m.ListSnapshots(volName)
	if err != nil {
		return err
	}
	api.GetApiContext(req).Write(toSnapshotCollection(snapList))
	return nil
}

func (s *Server) SnapshotGet(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "fail to get snapshot")
	}()

	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}
	volName := mux.Vars(req)["name"]

	snap, err := s.m.GetSnapshot(input.Name, volName)
	if err != nil {
		return err
	}
	api.GetApiContext(req).Write(toSnapshotResource(snap))
	return nil
}

func (s *Server) SnapshotDelete(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "fail to delete snapshot")
	}()

	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	volName := mux.Vars(req)["name"]

	vol, err := s.m.Get(volName)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}

	if vol.Status.IsStandby {
		return fmt.Errorf("cannot delete snapshot for standby volume %v", vol.Name)
	}

	if err := s.m.DeleteSnapshot(input.Name, volName); err != nil {
		return err
	}
	return s.responseWithVolume(w, req, volName, nil)
}

func (s *Server) SnapshotRevert(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "fail to revert snapshot")
	}()

	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}
	volName := mux.Vars(req)["name"]

	vol, err := s.m.Get(volName)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}

	if vol.Status.IsStandby {
		return fmt.Errorf("cannot revert snapshot for standby volume %v", vol.Name)
	}

	if vol.Spec.Frontend != "" && !vol.Spec.DisableFrontend {
		return fmt.Errorf("cannot revert snapshot for volume %v with frontend enabled", vol.Name)
	}

	if err := s.m.RevertSnapshot(input.Name, volName); err != nil {
		return err
	}

	return nil
}

func (s *Server) SnapshotBackup(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "fail to backup snapshot")
	}()

	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	volName := mux.Vars(req)["name"]

	vol, err := s.m.Get(volName)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}

	if vol.Status.IsStandby {
		return fmt.Errorf("cannot create backup for standby volume %v", vol.Name)
	}
	backingImageURL := ""
	if vol.Spec.BackingImage != "" {
		bi, err := s.m.GetBackingImage(vol.Spec.BackingImage)
		if err != nil {
			return err
		}
		backingImageURL = bi.Spec.ImageURL
	}

	labels, err := util.ValidateSnapshotLabels(input.Labels)
	if err != nil {
		return err
	}

	// Cannot directly compare the structs since KubernetesStatus contains a slice which cannot be compared.
	if !reflect.DeepEqual(vol.Status.KubernetesStatus, types.KubernetesStatus{}) {
		kubeStatus, err := json.Marshal(vol.Status.KubernetesStatus)
		if err != nil {
			return errors.Wrapf(err, "BUG: could not convert volume %v's KubernetesStatus to json", volName)
		}
		labels[types.KubernetesStatusLabel] = string(kubeStatus)
	}

	if err := s.m.BackupSnapshot(bsutil.GenerateName("backup"), volName, input.Name, vol.Spec.BackingImage, backingImageURL, labels); err != nil {
		return err
	}

	return s.responseWithVolume(w, req, volName, nil)
}

func (s *Server) SnapshotPurge(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "fail to purge snapshot")
	}()

	volName := mux.Vars(req)["name"]
	if err := s.m.PurgeSnapshot(volName); err != nil {
		return err
	}

	return s.responseWithVolume(w, req, volName, nil)
}

package api

import (
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/longhorn/longhorn-manager/monitoring"
)

var (
	RetryCounts   = 5
	RetryInterval = 100 * time.Millisecond
)

type HandleFuncWithError func(http.ResponseWriter, *http.Request) error

func HandleError(s *client.Schemas, t HandleFuncWithError) http.Handler {
	return api.ApiHandler(s, http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		var err error
		for i := 0; i < RetryCounts; i++ {
			err = t(rw, req)
			if !apierrors.IsConflict(errors.Cause(err)) {
				break
			}
			logrus.Warnf("Retry API call due to conflict")
			time.Sleep(RetryInterval)
		}
		if err != nil {
			logrus.Warnf("HTTP handling error %v", err)
			apiContext := api.GetApiContext(req)
			apiContext.WriteErr(err)
		}
	}))
}

func NewRouter(s *Server) *mux.Router {
	schemas := NewSchema()
	r := mux.NewRouter().StrictSlash(true)
	f := HandleError

	versionsHandler := api.VersionsHandler(schemas, "v1")
	versionHandler := api.VersionHandler(schemas, "v1")
	r.Methods("GET").Path("/").Handler(versionsHandler)
	r.Methods("GET").Path("/metrics").Handler(monitoring.Handler())
	r.Methods("GET").Path("/v1").Handler(versionHandler)
	r.Methods("GET").Path("/v1/apiversions").Handler(versionsHandler)
	r.Methods("GET").Path("/v1/apiversions/v1").Handler(versionHandler)
	r.Methods("GET").Path("/v1/schemas").Handler(api.SchemasHandler(schemas))
	r.Methods("GET").Path("/v1/schemas/{id}").Handler(api.SchemaHandler(schemas))

	r.Methods("GET").Path("/v1/settings").Handler(f(schemas, s.SettingList))
	r.Methods("GET").Path("/v1/settings/{name}").Handler(f(schemas, s.SettingGet))
	r.Methods("PUT").Path("/v1/settings/{name}").Handler(f(schemas, s.SettingSet))

	r.Methods("GET").Path("/v1/volumes").Handler(f(schemas, s.VolumeList))
	r.Methods("GET").Path("/v1/volumes/{name}").Handler(f(schemas, s.VolumeGet))
	r.Methods("DELETE").Path("/v1/volumes/{name}").Handler(f(schemas, s.VolumeDelete))
	r.Methods("POST").Path("/v1/volumes").Handler(f(schemas, s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.VolumeCreate)))
	volumeActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"attach":             s.VolumeAttach,
		"detach":             s.VolumeDetach,
		"salvage":            s.VolumeSalvage,
		"recurringUpdate":    s.VolumeRecurringUpdate,
		"updateDataLocality": s.VolumeUpdateDataLocality,
		"updateAccessMode":   s.VolumeUpdateAccessMode,
		"activate":           s.VolumeActivate,
		"expand":             s.VolumeExpand,
		"cancelExpansion":    s.VolumeCancelExpansion,

		"updateReplicaCount":       s.VolumeUpdateReplicaCount,
		"updateReplicaAutoBalance": s.VolumeUpdateReplicaAutoBalance,
		"replicaRemove":            s.ReplicaRemove,

		"engineUpgrade": s.EngineUpgrade,

		"snapshotPurge":  s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotPurge),
		"snapshotCreate": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotCreate),
		"snapshotList":   s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotList),
		"snapshotGet":    s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotGet),
		"snapshotDelete": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotDelete),
		"snapshotRevert": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotRevert),
		"snapshotBackup": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotBackup),

		"pvCreate":  s.PVCreate,
		"pvcCreate": s.PVCCreate,
	}
	for name, action := range volumeActions {
		r.Methods("POST").Path("/v1/volumes/{name}").Queries("action", name).Handler(f(schemas, action))
	}

	r.Methods("GET").Path("/v1/backupvolumes").Handler(f(schemas, s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupVolumeList)))
	r.Methods("GET").Path("/v1/backupvolumes/{volName}").Handler(f(schemas, s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupVolumeGet)))
	r.Methods("DELETE").Path("/v1/backupvolumes/{volName}").Handler(f(schemas, s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupVolumeDelete)))
	backupActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"backupList":   s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupList),
		"backupGet":    s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupGet),
		"backupDelete": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupDelete),
	}
	for name, action := range backupActions {
		r.Methods("POST").Path("/v1/backupvolumes/{volName}").Queries("action", name).Handler(f(schemas, action))
	}

	r.Methods("GET").Path("/v1/nodes").Handler(f(schemas, s.NodeList))
	r.Methods("GET").Path("/v1/nodes/{name}").Handler(f(schemas, s.NodeGet))
	r.Methods("PUT").Path("/v1/nodes/{name}").Handler(f(schemas, s.NodeUpdate))
	r.Methods("DELETE").Path("/v1/nodes/{name}").Handler(f(schemas, s.NodeDelete))
	nodeActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"diskUpdate": s.DiskUpdate,
	}
	for name, action := range nodeActions {
		r.Methods("POST").Path("/v1/nodes/{name}").Queries("action", name).Handler(f(schemas, action))
	}

	r.Methods("GET").Path("/v1/engineimages").Handler(f(schemas, s.EngineImageList))
	r.Methods("GET").Path("/v1/engineimages/{name}").Handler(f(schemas, s.EngineImageGet))
	r.Methods("DELETE").Path("/v1/engineimages/{name}").Handler(f(schemas, s.EngineImageDelete))
	r.Methods("POST").Path("/v1/engineimages").Handler(f(schemas, s.EngineImageCreate))

	r.Methods("Get").Path("/v1/events").Handler(f(schemas, s.EventList))

	r.Methods("GET").Path("/v1/disktags").Handler(f(schemas, s.DiskTagList))
	r.Methods("GET").Path("/v1/nodetags").Handler(f(schemas, s.NodeTagList))

	r.Methods("GET").Path("/v1/instancemanagers").Handler(f(schemas, s.InstanceManagerList))
	r.Methods("GET").Path("/v1/instancemanagers/{name}").Handler(f(schemas, s.InstanceManagerGet))

	r.Methods("GET").Path("/v1/backingimages").Handler(f(schemas, s.BackingImageList))
	r.Methods("GET").Path("/v1/backingimages/{name}").Handler(f(schemas, s.BackingImageGet))
	r.Methods("POST").Path("/v1/backingimages").Handler(f(schemas, s.BackingImageCreate))
	r.Methods("DELETE").Path("/v1/backingimages/{name}").Handler(f(schemas, s.BackingImageDelete))
	backingImageActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"backingImageCleanup": s.BackingImageCleanup,

		BackingImageUploadActionUploadServerStart: s.fwd.Handler(s.fwd.HandleProxyRequestForBackingImageUpload, s.fwd.GetHTTPAddressForBackingImageUpload(UploadServerAddressFromBackingImage(s.m)), s.BackingImageGet),
		BackingImageUploadActionChunkPrepare:      s.fwd.Handler(s.fwd.HandleProxyRequestForBackingImageUpload, s.fwd.GetHTTPAddressForBackingImageUpload(UploadServerAddressFromBackingImage(s.m)), s.BackingImageGet),
		BackingImageUploadActionChunkUpload:       s.fwd.Handler(s.fwd.HandleProxyRequestForBackingImageUpload, s.fwd.GetHTTPAddressForBackingImageUpload(UploadServerAddressFromBackingImage(s.m)), s.BackingImageGet),
		BackingImageUploadActionChunkCoalesce:     s.fwd.Handler(s.fwd.HandleProxyRequestForBackingImageUpload, s.fwd.GetHTTPAddressForBackingImageUpload(UploadServerAddressFromBackingImage(s.m)), s.BackingImageGet),
		BackingImageUploadActionUploadServerClose: s.fwd.Handler(s.fwd.HandleProxyRequestForBackingImageUpload, s.fwd.GetHTTPAddressForBackingImageUpload(UploadServerAddressFromBackingImage(s.m)), s.BackingImageGet),
		BackingImageUploadActionUpload:            s.fwd.Handler(s.fwd.HandleProxyRequestForBackingImageUpload, s.fwd.GetHTTPAddressForBackingImageUpload(UploadServerAddressFromBackingImage(s.m)), s.BackingImageGet),
	}
	for name, action := range backingImageActions {
		r.Methods("POST").Path("/v1/backingimages/{name}").Queries("action", name).Handler(f(schemas, action))
	}

	r.Methods("POST").Path("/v1/supportbundles").Handler(f(schemas, s.InitiateSupportBundle))
	r.Methods("GET").Path("/v1/supportbundles/{name}/{bundleName}").Handler(f(schemas,
		s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromNode(s.m)), s.QuerySupportBundle)))
	r.Methods("GET").Path("/v1/supportbundles/{name}/{bundleName}/download").Handler(f(schemas,
		s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromNode(s.m)), s.DownloadSupportBundle)))

	settingListStream := NewStreamHandlerFunc("settings", s.wsc.NewWatcher("setting"), s.settingList)
	r.Path("/v1/ws/settings").Handler(f(schemas, settingListStream))
	r.Path("/v1/ws/{period}/settings").Handler(f(schemas, settingListStream))

	volumeListStream := NewStreamHandlerFunc("volumes", s.wsc.NewWatcher("volume", "engine", "replica"), s.volumeList)
	r.Path("/v1/ws/volumes").Handler(f(schemas, volumeListStream))
	r.Path("/v1/ws/{period}/volumes").Handler(f(schemas, volumeListStream))

	nodeListStream := NewStreamHandlerFunc("nodes", s.wsc.NewWatcher("node"), s.nodeList)
	r.Path("/v1/ws/nodes").Handler(f(schemas, nodeListStream))
	r.Path("/v1/ws/{period}/nodes").Handler(f(schemas, nodeListStream))

	engineImageStream := NewStreamHandlerFunc("engineimages", s.wsc.NewWatcher("engineImage"), s.engineImageList)
	r.Path("/v1/ws/engineimages").Handler(f(schemas, engineImageStream))
	r.Path("/v1/ws/{period}/engineimages").Handler(f(schemas, engineImageStream))

	backingImageStream := NewStreamHandlerFunc("backingimages", s.wsc.NewWatcher("backingImage"), s.backingImageList)
	r.Path("/v1/ws/backingimages").Handler(f(schemas, backingImageStream))
	r.Path("/v1/ws/{period}/backingimages").Handler(f(schemas, backingImageStream))

	eventListStream := NewStreamHandlerFunc("events", s.wsc.NewWatcher("event"), s.eventList)
	r.Path("/v1/ws/events").Handler(f(schemas, eventListStream))
	r.Path("/v1/ws/{period}/events").Handler(f(schemas, eventListStream))

	return r
}

package engineapi

import (
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	devtypes "github.com/longhorn/go-iscsi-helper/types"
	imutil "github.com/longhorn/longhorn-instance-manager/pkg/util"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

type EngineCollection struct{}

type Engine struct {
	name  string
	image string
	ip    string
	port  int
	cURL  string
}

func (c *EngineCollection) NewEngineClient(request *EngineClientRequest) (EngineClient, error) {
	if request.EngineImage == "" {
		return nil, fmt.Errorf("Invalid empty engine image from request")
	}
	if request.IP != "" && request.Port == 0 {
		return nil, fmt.Errorf("Invalid empty port from request with valid IP")
	}
	return &Engine{
		name:  request.VolumeName,
		image: request.EngineImage,
		ip:    request.IP,
		port:  request.Port,
		cURL:  imutil.GetURL(request.IP, request.Port),
	}, nil
}

func (e *Engine) Name() string {
	return e.name
}

func (e *Engine) LonghornEngineBinary() string {
	return filepath.Join(types.GetEngineBinaryDirectoryOnHostForImage(e.image), "longhorn")
}

func (e *Engine) ExecuteEngineBinary(args ...string) (string, error) {
	args = append([]string{"--url", e.cURL}, args...)
	return util.Execute([]string{}, e.LonghornEngineBinary(), args...)
}

func (e *Engine) ExecuteEngineBinaryWithTimeout(timeout time.Duration, args ...string) (string, error) {
	args = append([]string{"--url", e.cURL}, args...)
	return util.ExecuteWithTimeout(timeout, []string{}, e.LonghornEngineBinary(), args...)
}

func (e *Engine) ExecuteEngineBinaryWithoutTimeout(envs []string, args ...string) (string, error) {
	args = append([]string{"--url", e.cURL}, args...)
	return util.ExecuteWithoutTimeout(envs, e.LonghornEngineBinary(), args...)
}

func parseReplica(s string) (*Replica, error) {
	fields := strings.Fields(s)
	if len(fields) < 2 {
		return nil, errors.Errorf("cannot parse line `%s`", s)
	}
	url := fields[0]
	mode := types.ReplicaMode(fields[1])
	if mode != types.ReplicaModeRW && mode != types.ReplicaModeWO {
		mode = types.ReplicaModeERR
	}
	return &Replica{
		URL:  url,
		Mode: mode,
	}, nil
}

func (e *Engine) ReplicaList() (map[string]*Replica, error) {
	output, err := e.ExecuteEngineBinary("ls")
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list replicas from controller '%s'", e.name)
	}
	replicas := make(map[string]*Replica)
	lines := strings.Split(output, "\n")
	for _, l := range lines {
		if strings.HasPrefix(l, "ADDRESS") {
			continue
		}
		if l == "" {
			continue
		}
		replica, err := parseReplica(l)
		if err != nil {
			return nil, errors.Wrapf(err, "error parsing replica status from `%s`, output %v", l, output)
		}
		replicas[replica.URL] = replica
	}
	return replicas, nil
}

func (e *Engine) ReplicaAdd(url string, isRestoreVolume bool) error {
	if err := ValidateReplicaURL(url); err != nil {
		return err
	}
	cmd := []string{"add", url}
	if isRestoreVolume {
		cmd = append(cmd, "--restore")
	}
	if _, err := e.ExecuteEngineBinaryWithoutTimeout([]string{}, cmd...); err != nil {
		return errors.Wrapf(err, "failed to add replica address='%s' to controller '%s'", url, e.name)
	}
	return nil
}

func (e *Engine) ReplicaRemove(url string) error {
	if err := ValidateReplicaURL(url); err != nil {
		return err
	}
	if _, err := e.ExecuteEngineBinary("rm", url); err != nil {
		return errors.Wrapf(err, "failed to rm replica address='%s' from controller '%s'", url, e.name)
	}
	return nil
}

func (e *Engine) Info() (*Volume, error) {
	output, err := e.ExecuteEngineBinary("info")
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get volume info")
	}

	info := &Volume{}
	if err := json.Unmarshal([]byte(output), info); err != nil {
		return nil, errors.Wrapf(err, "cannot decode volume info: %v", output)
	}
	return info, nil
}

func (e *Engine) Endpoint() (string, error) {
	info, err := e.Info()
	if err != nil {
		return "", err
	}

	switch info.Frontend {
	case devtypes.FrontendTGTBlockDev:
		return info.Endpoint, nil
	case devtypes.FrontendTGTISCSI:
		// it will looks like this in the end
		// iscsi://10.42.0.12:3260/iqn.2014-09.com.rancher:vol-name/1
		hostPort := net.JoinHostPort(e.ip, DefaultISCSIPort)
		return "iscsi://" + hostPort + "/" + info.Endpoint + "/" + DefaultISCSILUN, nil
	case "":
		return "", nil
	}
	return "", fmt.Errorf("Unknown frontend %v", info.Endpoint)
}

func (e *Engine) Version(clientOnly bool) (*EngineVersion, error) {
	cmdline := []string{"version"}
	if clientOnly {
		cmdline = append(cmdline, "--client-only")
	} else {
		cmdline = append([]string{"--url", e.cURL}, cmdline...)
	}
	output, err := util.Execute([]string{}, e.LonghornEngineBinary(), cmdline...)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get volume version")
	}

	version := &EngineVersion{}
	if err := json.Unmarshal([]byte(output), version); err != nil {
		return nil, errors.Wrapf(err, "cannot decode volume version: %v", output)
	}
	return version, nil
}

func (e *Engine) Expand(size int64) error {
	if _, err := e.ExecuteEngineBinary("expand", "--size", strconv.FormatInt(size, 10)); err != nil {
		return errors.Wrapf(err, "cannot get expand volume engine to size %v", size)
	}

	return nil
}

func (e *Engine) ReplicaRebuildStatus() (map[string]*types.RebuildStatus, error) {
	output, err := e.ExecuteEngineBinary("replica-rebuild-status")
	if err != nil {
		return nil, errors.Wrapf(err, "error getting replica rebuild status")
	}

	data := map[string]*types.RebuildStatus{}
	if err := json.Unmarshal([]byte(output), &data); err != nil {
		return nil, errors.Wrapf(err, "error parsing replica rebuild status")
	}

	return data, nil
}

func (e *Engine) FrontendStart(volumeFrontend types.VolumeFrontend) error {
	frontendName, err := GetEngineProcessFrontend(volumeFrontend)
	if err != nil {
		return err
	}
	if frontendName == "" {
		return fmt.Errorf("cannot start empty frontend")
	}

	if _, err := e.ExecuteEngineBinary("frontend", "start", frontendName); err != nil {
		return errors.Wrapf(err, "error starting frontend %v", frontendName)
	}

	return nil
}

func (e *Engine) FrontendShutdown() error {
	if _, err := e.ExecuteEngineBinary("frontend", "shutdown"); err != nil {
		return errors.Wrapf(err, "error shutting down the frontend")
	}

	return nil
}

func (e *Engine) ReplicaRebuildVerify(url string) error {
	if err := ValidateReplicaURL(url); err != nil {
		return err
	}
	cmd := []string{"verify-rebuild-replica", url}
	if _, err := e.ExecuteEngineBinaryWithoutTimeout([]string{}, cmd...); err != nil {
		return errors.Wrapf(err, "failed to verify rebuilding for the replica from address %s", url)
	}
	return nil
}

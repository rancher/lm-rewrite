package types

import (
	"time"
)

const (
	BackingImageManagerDirectoryName = "backing-images"
	DiskPathInContainer              = "/data/"

	DefaultPort = 8000

	GRPCServiceTimeout     = 3 * time.Minute
	FileValidationInterval = 5 * time.Second
	FileSyncTimeout        = 120

	SendingLimit = 3

	BackingImageStatePending    = "pending"
	BackingImageStateStarting   = "starting"
	BackingImageStateInProgress = "in_progress"
	BackingImageStateReady      = "ready"
	BackingImageStateFailed     = "failed"

	BackingImageTmpFileName = "backing.tmp"
	BackingImageFileName    = "backing"
)

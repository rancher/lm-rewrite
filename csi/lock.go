package csi

import (
	"k8s.io/apimachinery/pkg/util/sets"
	"sync"
)

const OperationTryLockFMT = "%s: try to lock resource %s"
const OperationPendingFMT = "pending operation for resource %v"

// OperationLocks stores a guarded set of all resources with an ongoing operation.
// resources are identified by their id example resources: volumeID, snapshotID, etc.
type OperationLocks struct {
	ops sets.String
	m   sync.Mutex
}

func NewOperationLocks() *OperationLocks {
	return &OperationLocks{
		ops: sets.NewString(),
	}
}

// TryAcquire tries to acquire the lock for operating on a resource returns true on success
// if another operation is already in progress for this resource returns false
func (lock *OperationLocks) TryAcquire(resource string) bool {
	lock.m.Lock()
	defer lock.m.Unlock()
	if lock.ops.Has(resource) {
		return false
	}
	lock.ops.Insert(resource)
	return true
}

func (lock *OperationLocks) Release(resource string) {
	lock.m.Lock()
	defer lock.m.Unlock()
	lock.ops.Delete(resource)
}

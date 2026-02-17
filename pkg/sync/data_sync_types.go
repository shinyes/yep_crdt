package sync

import (
	"sync"

	"github.com/shinyes/yep_crdt/pkg/db"
)

// DataSyncManager handles CRDT merge-based incremental/full sync.
type DataSyncManager struct {
	mu      sync.RWMutex
	db      *db.DB
	nodeID  string
	network NetworkInterface
}

// NewDataSyncManager creates a sync manager.
func NewDataSyncManager(database *db.DB, nodeID string) *DataSyncManager {
	return &DataSyncManager{
		db:     database,
		nodeID: nodeID,
	}
}

// SetNetwork registers network transport.
func (dsm *DataSyncManager) SetNetwork(n NetworkInterface) {
	dsm.mu.Lock()
	defer dsm.mu.Unlock()
	dsm.network = n
}

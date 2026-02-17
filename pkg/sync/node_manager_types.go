package sync

import (
	"sync"
	"time"

	"github.com/shinyes/yep_crdt/pkg/db"
)

// NodeManager coordinates heartbeat, clock, and data sync for one node.
type NodeManager struct {
	mu              sync.RWMutex
	nodes           map[string]*NodeInfo
	offlineSince    map[string]time.Time
	manualGCPending map[string]int64 // coordinator peer ID -> safe timestamp
	fullSyncing     map[string]bool
	localNodeID     string
	db              *db.DB
	config          Config

	heartbeat *HeartbeatMonitor
	dataSync  *DataSyncManager
	clockSync *ClockSync
	network   NetworkInterface
}

const defaultSafeTimestampOffset = 30 * time.Second

// NewNodeManager creates a node manager.
func NewNodeManager(database *db.DB, nodeID string, opts ...Option) *NodeManager {
	config := DefaultConfig()
	for _, opt := range opts {
		opt(&config)
	}

	nm := &NodeManager{
		nodes:           make(map[string]*NodeInfo),
		offlineSince:    make(map[string]time.Time),
		manualGCPending: make(map[string]int64),
		fullSyncing:     make(map[string]bool),
		localNodeID:     nodeID,
		db:              database,
		config:          config,
		network:         &DefaultNetwork{},
	}

	nm.heartbeat = NewHeartbeatMonitor(nm, config.HeartbeatInterval)
	nm.dataSync = NewDataSyncManager(database, nodeID)
	nm.clockSync = NewClockSync(nm, config.ClockThreshold)

	return nm
}

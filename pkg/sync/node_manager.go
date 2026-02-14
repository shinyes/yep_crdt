package sync

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/shinyes/yep_crdt/pkg/db"
)

// NodeManager coordinates heartbeat, GC, clock, and data sync for one node.
type NodeManager struct {
	mu          sync.RWMutex
	nodes       map[string]*NodeInfo
	localNodeID string
	db          *db.DB
	config      Config

	heartbeat *HeartbeatMonitor
	gc        *GCManager
	dataSync  *DataSyncManager
	clockSync *ClockSync
	network   NetworkInterface
}

// NewNodeManager creates a node manager.
func NewNodeManager(database *db.DB, nodeID string, opts ...Option) *NodeManager {
	config := DefaultConfig()
	for _, opt := range opts {
		opt(&config)
	}

	nm := &NodeManager{
		nodes:       make(map[string]*NodeInfo),
		localNodeID: nodeID,
		db:          database,
		config:      config,
		network:     &DefaultNetwork{},
	}

	nm.heartbeat = NewHeartbeatMonitor(nm, config.HeartbeatInterval)
	nm.gc = NewGCManager(nm, config.GCInterval, config.GCTimeOffset)
	nm.dataSync = NewDataSyncManager(database, nodeID)
	nm.clockSync = NewClockSync(nm, config.ClockThreshold)

	return nm
}

// Start starts background heartbeat and GC components.
func (nm *NodeManager) Start(ctx context.Context) {
	log.Printf("node manager starting: local=%s", nm.localNodeID)
	nm.heartbeat.Start(ctx)
	nm.gc.Start(ctx)
	log.Printf("node manager started: local=%s", nm.localNodeID)
}

// Stop stops all background components.
func (nm *NodeManager) Stop() {
	log.Printf("node manager stopping: local=%s", nm.localNodeID)
	nm.heartbeat.Stop()
	nm.gc.Stop()
}

// OnHeartbeat records heartbeat from a peer.
func (nm *NodeManager) OnHeartbeat(nodeID string, clock int64) {
	nm.heartbeat.OnHeartbeat(nodeID, clock)
}

// OnPeerConnected marks a peer online from transport-level connect event.
func (nm *NodeManager) OnPeerConnected(nodeID string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	now := time.Now()
	localClock := nm.db.Clock().Now()
	nodeInfo, exists := nm.nodes[nodeID]
	if !exists {
		nm.nodes[nodeID] = &NodeInfo{
			ID:             nodeID,
			LastHeartbeat:  now,
			IsOnline:       true,
			LastKnownClock: localClock,
			LastSyncTime:   now,
		}
		return
	}

	nodeInfo.LastHeartbeat = now
	nodeInfo.IsOnline = true
	if nodeInfo.LastKnownClock <= 0 {
		nodeInfo.LastKnownClock = localClock
	}
}

// OnPeerDisconnected marks a peer offline from transport-level disconnect event.
func (nm *NodeManager) OnPeerDisconnected(nodeID string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if nodeInfo, exists := nm.nodes[nodeID]; exists {
		nodeInfo.IsOnline = false
	}
}

// MarkPeerSeen updates liveness based on any inbound peer traffic.
// It intentionally avoids clock-sync side effects and full-rejoin handling.
func (nm *NodeManager) MarkPeerSeen(nodeID string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	now := time.Now()
	localClock := nm.db.Clock().Now()
	nodeInfo, exists := nm.nodes[nodeID]
	if !exists {
		nm.nodes[nodeID] = &NodeInfo{
			ID:             nodeID,
			LastHeartbeat:  now,
			IsOnline:       true,
			LastKnownClock: localClock,
			LastSyncTime:   now,
		}
		return
	}

	nodeInfo.LastHeartbeat = now
	nodeInfo.IsOnline = true
	if nodeInfo.LastKnownClock <= 0 {
		nodeInfo.LastKnownClock = localClock
	}
}

// GetNodeInfo returns one node record.
func (nm *NodeManager) GetNodeInfo(nodeID string) (*NodeInfo, bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	nodeInfo, exists := nm.nodes[nodeID]
	return nodeInfo, exists
}

// IsNodeOnline returns whether a node is online.
func (nm *NodeManager) IsNodeOnline(nodeID string) bool {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if nodeInfo, exists := nm.nodes[nodeID]; exists {
		return nodeInfo.IsOnline
	}
	return false
}

// GetOnlineNodes returns all online node IDs.
func (nm *NodeManager) GetOnlineNodes() []string {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	onlineNodes := make([]string, 0)
	for nodeID, nodeInfo := range nm.nodes {
		if nodeInfo.IsOnline {
			onlineNodes = append(onlineNodes, nodeID)
		}
	}
	return onlineNodes
}

// CalculateSafeTimestamp computes GC safe time using online peer clocks.
func (nm *NodeManager) CalculateSafeTimestamp() int64 {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	var minClock int64
	first := true
	for _, nodeInfo := range nm.nodes {
		if !nodeInfo.IsOnline {
			continue
		}
		if nodeInfo.ID == nm.localNodeID {
			continue
		}
		if nodeInfo.LastKnownClock <= 0 {
			continue
		}
		if first || nodeInfo.LastKnownClock < minClock {
			minClock = nodeInfo.LastKnownClock
			first = false
		}
	}

	safetyOffset := nm.config.GCTimeOffset.Milliseconds()
	if safetyOffset <= 0 {
		safetyOffset = int64((30 * time.Second).Milliseconds())
	}

	if first {
		return nm.db.Clock().Now() - safetyOffset
	}
	return minClock - safetyOffset
}

// OnReceiveMerge applies one full-row CRDT payload.
func (nm *NodeManager) OnReceiveMerge(table string, key string, rawData []byte, timestamp int64) error {
	return nm.dataSync.OnReceiveMerge(table, key, rawData, timestamp)
}

// OnReceiveDelta applies one partial-row CRDT payload.
func (nm *NodeManager) OnReceiveDelta(table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return nm.dataSync.OnReceiveDelta(table, key, columns, rawData, timestamp)
}

// UpdateLocalClock merges remote clock into local HLC.
func (nm *NodeManager) UpdateLocalClock(remoteClock int64) {
	nm.db.Clock().Update(remoteClock)
	log.Printf("local clock updated: %d", remoteClock)
}

// RegisterNetwork binds transport implementation.
func (nm *NodeManager) RegisterNetwork(network NetworkInterface) {
	nm.network = network
	nm.dataSync.SetNetwork(network)
	log.Println("network interface registered")
}

// BroadcastHeartbeat sends heartbeat to all peers.
func (nm *NodeManager) BroadcastHeartbeat(clock int64) error {
	if nm.network == nil {
		return nil
	}
	return nm.network.BroadcastHeartbeat(clock)
}

// BroadcastRawData sends one full-row CRDT payload to all peers.
func (nm *NodeManager) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	if nm.network == nil {
		return nil
	}
	return nm.network.BroadcastRawData(table, key, rawData, timestamp)
}

// FetchRawTableData fetches all rows of a table from one remote peer.
func (nm *NodeManager) FetchRawTableData(sourceNodeID, tableName string) ([]RawRowData, error) {
	if nm.network == nil {
		return nil, ErrNoNetwork
	}
	return nm.network.FetchRawTableData(sourceNodeID, tableName)
}

// GetLocalNodeID returns local node ID.
func (nm *NodeManager) GetLocalNodeID() string {
	return nm.localNodeID
}

// FullSync runs full sync from one source node.
func (nm *NodeManager) FullSync(ctx context.Context, sourceNodeID string) (*SyncResult, error) {
	return nm.dataSync.FullSync(ctx, sourceNodeID)
}

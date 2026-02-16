package sync

import (
	"context"
	"errors"
	"fmt"
	"log"
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

// Start starts background components.
func (nm *NodeManager) Start(ctx context.Context) {
	log.Printf("node manager starting: local=%s", nm.localNodeID)
	nm.heartbeat.Start(ctx)
	log.Printf("node manager started: local=%s", nm.localNodeID)
}

// Stop stops background components.
func (nm *NodeManager) Stop() {
	log.Printf("node manager stopping: local=%s", nm.localNodeID)
	nm.heartbeat.Stop()
}

// OnHeartbeat records heartbeat from a peer.
func (nm *NodeManager) OnHeartbeat(nodeID string, clock int64, gcFloor int64) {
	nm.heartbeat.OnHeartbeat(nodeID, clock, gcFloor)
}

// OnPeerConnected marks a peer online from transport-level connect event.
func (nm *NodeManager) OnPeerConnected(nodeID string) {
	now := time.Now()
	localClock := nm.db.Clock().Now()
	var shouldHandleRejoin bool
	var rejoinClock int64
	var offlineDuration time.Duration

	nm.mu.Lock()
	nodeInfo, exists := nm.nodes[nodeID]
	if !exists {
		nm.nodes[nodeID] = &NodeInfo{
			ID:                 nodeID,
			LastHeartbeat:      now,
			IsOnline:           true,
			LastKnownClock:     localClock,
			LastKnownGCFloor:   0,
			IncrementalBlocked: false,
			LastSyncTime:       now,
		}
		nm.mu.Unlock()
		return
	}

	if !nodeInfo.IsOnline {
		shouldHandleRejoin = true
		rejoinClock = nodeInfo.LastKnownClock
		if rejoinClock <= 0 {
			rejoinClock = localClock
		}
		offlineDuration = nm.offlineDurationSinceLocked(nodeID, now, nodeInfo.LastHeartbeat)
	}

	nodeInfo.LastHeartbeat = now
	nodeInfo.IsOnline = true
	if nodeInfo.LastKnownClock <= 0 {
		nodeInfo.LastKnownClock = localClock
	}
	delete(nm.offlineSince, nodeID)
	nm.mu.Unlock()

	if shouldHandleRejoin {
		nm.clockSync.HandleNodeRejoin(nodeID, rejoinClock, offlineDuration)
	}
}

// OnPeerDisconnected marks a peer offline from transport-level disconnect event.
func (nm *NodeManager) OnPeerDisconnected(nodeID string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if nodeInfo, exists := nm.nodes[nodeID]; exists {
		nodeInfo.IsOnline = false
		nm.offlineSince[nodeID] = time.Now()
	}
}

func (nm *NodeManager) offlineDurationSinceLocked(nodeID string, now time.Time, fallback time.Time) time.Duration {
	if offlineAt, ok := nm.offlineSince[nodeID]; ok && !offlineAt.IsZero() {
		if now.After(offlineAt) {
			return now.Sub(offlineAt)
		}
		return 0
	}
	if fallback.IsZero() || !now.After(fallback) {
		return 0
	}
	return now.Sub(fallback)
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
			ID:                 nodeID,
			LastHeartbeat:      now,
			IsOnline:           true,
			LastKnownClock:     localClock,
			LastKnownGCFloor:   0,
			IncrementalBlocked: false,
			LastSyncTime:       now,
		}
		return
	}

	nodeInfo.LastHeartbeat = now
	nodeInfo.IsOnline = true
	if nodeInfo.LastKnownClock <= 0 {
		nodeInfo.LastKnownClock = localClock
	}
	delete(nm.offlineSince, nodeID)
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

// CalculateSafeTimestamp computes GC safe time using all known node clocks.
// This is intentionally conservative: offline nodes are still considered so
// GC does not advance past their last known observation point.
func (nm *NodeManager) CalculateSafeTimestamp() int64 {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	localClock := nm.db.Clock().Now()
	minClock := localClock
	for _, nodeInfo := range nm.nodes {
		if nodeInfo.ID == nm.localNodeID {
			continue
		}
		if nodeInfo.LastKnownClock <= 0 {
			continue
		}
		if nodeInfo.LastKnownClock < minClock {
			minClock = nodeInfo.LastKnownClock
		}
	}

	return minClock - defaultSafeTimestampOffset.Milliseconds()
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

func (nm *NodeManager) markPeerFullSync(nodeID string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if nodeInfo, exists := nm.nodes[nodeID]; exists {
		nodeInfo.LastSyncTime = time.Now()
	}
}

// LocalGCFloor returns persisted local GC floor.
func (nm *NodeManager) LocalGCFloor() int64 {
	return nm.db.GCFloor()
}

// SetLocalGCFloor updates persisted local GC floor and refreshes per-peer incremental gates.
func (nm *NodeManager) SetLocalGCFloor(floor int64) error {
	if err := nm.db.SetGCFloor(floor); err != nil {
		return err
	}
	nm.refreshIncrementalGates()
	return nil
}

// ObservePeerGCFloor records peer GC floor and enforces incremental/full-sync gate.
func (nm *NodeManager) ObservePeerGCFloor(nodeID string, gcFloor int64) {
	if nodeID == "" || gcFloor < 0 {
		return
	}

	nm.mu.Lock()
	nodeInfo, exists := nm.nodes[nodeID]
	if !exists {
		now := time.Now()
		localClock := nm.db.Clock().Now()
		nodeInfo = &NodeInfo{
			ID:                 nodeID,
			LastHeartbeat:      now,
			IsOnline:           true,
			LastKnownClock:     localClock,
			LastKnownGCFloor:   0,
			IncrementalBlocked: false,
			LastSyncTime:       now,
		}
		nm.nodes[nodeID] = nodeInfo
	}
	if gcFloor > nodeInfo.LastKnownGCFloor {
		nodeInfo.LastKnownGCFloor = gcFloor
	}
	nm.mu.Unlock()

	nm.refreshIncrementalGates()
}

// CanUseIncrementalWithPeer reports whether incremental sync is allowed with one peer.
func (nm *NodeManager) CanUseIncrementalWithPeer(nodeID string) bool {
	if nodeID == "" {
		return false
	}

	nm.mu.RLock()
	defer nm.mu.RUnlock()

	nodeInfo, exists := nm.nodes[nodeID]
	if !exists {
		return true
	}
	return !nodeInfo.IncrementalBlocked
}

func (nm *NodeManager) refreshIncrementalGates() {
	type syncTask struct {
		nodeID string
		floor  int64
	}

	localFloor := nm.db.GCFloor()
	tasks := make([]syncTask, 0)

	nm.mu.Lock()
	for nodeID, nodeInfo := range nm.nodes {
		if nodeInfo == nil || nodeID == "" || nodeID == nm.localNodeID {
			continue
		}

		peerFloor := nodeInfo.LastKnownGCFloor
		shouldBlock := (peerFloor > 0 || localFloor > 0) && peerFloor != localFloor
		wasBlocked := nodeInfo.IncrementalBlocked
		nodeInfo.IncrementalBlocked = shouldBlock
		if wasBlocked != shouldBlock {
			log.Printf("[GCFloor] incremental gate changed: peer=%s, blocked=%v, local_floor=%d, peer_floor=%d",
				shortPeerID(nodeID), shouldBlock, localFloor, peerFloor)
		}

		if shouldBlock && peerFloor > localFloor && !nm.fullSyncing[nodeID] {
			nm.fullSyncing[nodeID] = true
			tasks = append(tasks, syncTask{
				nodeID: nodeID,
				floor:  peerFloor,
			})
		}
	}
	nm.mu.Unlock()

	for _, task := range tasks {
		task := task
		go nm.runGCFloorFullSync(task.nodeID, task.floor)
	}
}

func (nm *NodeManager) runGCFloorFullSync(sourceNodeID string, targetFloor int64) {
	log.Printf("[GCFloor] start full sync for lagging node: source=%s, target_floor=%d",
		shortPeerID(sourceNodeID), targetFloor)

	ctx := context.Background()
	result, err := nm.dataSync.FullSync(ctx, sourceNodeID)
	if err != nil {
		log.Printf("[GCFloor] full sync failed: source=%s, target_floor=%d, err=%v",
			shortPeerID(sourceNodeID), targetFloor, err)
		nm.finishGCFloorFullSync(sourceNodeID)
		return
	}

	if err := nm.SetLocalGCFloor(targetFloor); err != nil {
		log.Printf("[GCFloor] persist local gc floor failed: source=%s, target_floor=%d, err=%v",
			shortPeerID(sourceNodeID), targetFloor, err)
		nm.finishGCFloorFullSync(sourceNodeID)
		return
	}

	nm.markPeerFullSync(sourceNodeID)
	log.Printf("[GCFloor] full sync done: source=%s, target_floor=%d, tables=%d, rows=%d, rejected=%d",
		shortPeerID(sourceNodeID), targetFloor, result.TablesSynced, result.RowsSynced, result.RejectedCount)
	nm.finishGCFloorFullSync(sourceNodeID)
}

func (nm *NodeManager) finishGCFloorFullSync(sourceNodeID string) {
	nm.mu.Lock()
	delete(nm.fullSyncing, sourceNodeID)
	nm.mu.Unlock()
	nm.refreshIncrementalGates()
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
	peers := nm.GetOnlineNodes()
	if len(peers) == 0 {
		return nm.network.BroadcastHeartbeat(clock)
	}

	localFloor := nm.LocalGCFloor()
	var sendErrors []error
	for _, peerID := range peers {
		if peerID == "" || peerID == nm.localNodeID {
			continue
		}
		msg := &NetworkMessage{
			Type:      MsgTypeHeartbeat,
			Clock:     clock,
			GCFloor:   localFloor,
			Timestamp: clock,
		}
		if err := nm.network.SendMessage(peerID, msg); err != nil {
			sendErrors = append(sendErrors, fmt.Errorf("peer=%s err=%w", shortPeerID(peerID), err))
		}
	}
	if len(sendErrors) > 0 {
		return errors.Join(sendErrors...)
	}
	return nil
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

package sync

import (
	"log"
	"time"
)

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

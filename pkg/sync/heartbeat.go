package sync

import (
	"context"
	"log"
	"sync"
	"time"
)

// HeartbeatMonitor periodically broadcasts heartbeat for clock sync.
// Peer online/offline state is managed by transport connect/disconnect events.
type HeartbeatMonitor struct {
	nm       *NodeManager
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
}

// NewHeartbeatMonitor creates a heartbeat monitor.
func NewHeartbeatMonitor(nm *NodeManager, interval time.Duration) *HeartbeatMonitor {
	return &HeartbeatMonitor{
		nm:       nm,
		interval: interval,
	}
}

// Start starts heartbeat broadcast loop.
func (hm *HeartbeatMonitor) Start(ctx context.Context) {
	hm.mu.Lock()
	hm.ctx, hm.cancel = context.WithCancel(ctx)
	hm.mu.Unlock()

	ticker := time.NewTicker(hm.interval)
	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-hm.ctx.Done():
				log.Println("heartbeat monitor stopped")
				return
			case <-ticker.C:
				hm.broadcastHeartbeat()
			}
		}
	}()

	log.Printf("heartbeat monitor started: interval=%v", hm.interval)
}

// OnHeartbeat handles inbound heartbeat from one peer.
func (hm *HeartbeatMonitor) OnHeartbeat(nodeID string, clock int64, gcFloor int64) {
	var shouldHandleRejoin bool
	var offlineDuration time.Duration

	hm.nm.mu.Lock()

	now := time.Now()
	nodeInfo, exists := hm.nm.nodes[nodeID]
	if !exists {
		nodeInfo = &NodeInfo{
			ID:                 nodeID,
			IsOnline:           true,
			LastHeartbeat:      now,
			LastKnownClock:     clock,
			LastKnownGCFloor:   gcFloor,
			IncrementalBlocked: false,
			LastSyncTime:       now,
		}
		hm.nm.nodes[nodeID] = nodeInfo
		log.Printf("peer discovered via heartbeat: %s clock=%d gc_floor=%d", nodeID, clock, gcFloor)
	} else {
		wasOffline := !nodeInfo.IsOnline
		if wasOffline {
			shouldHandleRejoin = true
			offlineDuration = hm.nm.offlineDurationSinceLocked(nodeID, now, nodeInfo.LastHeartbeat)
		}
		nodeInfo.LastHeartbeat = now
		nodeInfo.LastKnownClock = clock
		if gcFloor > nodeInfo.LastKnownGCFloor {
			nodeInfo.LastKnownGCFloor = gcFloor
		}
		nodeInfo.IsOnline = true
		delete(hm.nm.offlineSince, nodeID)

		if wasOffline {
			log.Printf("peer rejoined: %s", nodeID)
		}
	}
	hm.nm.mu.Unlock()

	hm.nm.ObservePeerGCFloor(nodeID, gcFloor)

	if shouldHandleRejoin {
		hm.nm.clockSync.HandleNodeRejoin(nodeID, clock, offlineDuration)
	}

	if clock > 0 {
		hm.nm.UpdateLocalClock(clock)
	}
}

func (hm *HeartbeatMonitor) broadcastHeartbeat() {
	if hm.nm == nil {
		return
	}
	if hm.nm.network == nil {
		return
	}

	clock := hm.nm.db.Clock().Now()
	if err := hm.nm.BroadcastHeartbeat(clock); err != nil {
		log.Printf("heartbeat broadcast failed: %v", err)
	}
}

// Stop stops heartbeat monitor.
func (hm *HeartbeatMonitor) Stop() {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if hm.cancel != nil {
		hm.cancel()
		log.Println("heartbeat monitor stopped")
	}
}

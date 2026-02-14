package sync

import (
	"context"
	"log"
	"sync"
	"time"
)

// HeartbeatMonitor tracks peer liveness via periodic heartbeat broadcast/check.
type HeartbeatMonitor struct {
	nm       *NodeManager
	interval time.Duration
	timeout  time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
}

// NewHeartbeatMonitor creates a heartbeat monitor.
func NewHeartbeatMonitor(nm *NodeManager, interval time.Duration, timeout time.Duration) *HeartbeatMonitor {
	return &HeartbeatMonitor{
		nm:       nm,
		interval: interval,
		timeout:  timeout,
	}
}

// Start starts heartbeat loop.
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
				log.Println("🛑 心跳监控已停止")
				return
			case <-ticker.C:
				hm.broadcastHeartbeat()
				hm.checkHeartbeats()
			}
		}
	}()

	log.Printf("✅ 心跳监控已启动: 间隔=%v, 超时=%v", hm.interval, hm.timeout)
}

// OnHeartbeat handles inbound heartbeat from one peer.
func (hm *HeartbeatMonitor) OnHeartbeat(nodeID string, clock int64) {
	hm.nm.mu.Lock()
	defer hm.nm.mu.Unlock()

	now := time.Now()

	nodeInfo, exists := hm.nm.nodes[nodeID]
	if !exists {
		nodeInfo = &NodeInfo{
			ID:             nodeID,
			IsOnline:       true,
			LastHeartbeat:  now,
			LastKnownClock: clock,
			LastSyncTime:   now,
		}
		hm.nm.nodes[nodeID] = nodeInfo
		log.Printf("✨ 新节点加入: %s, 时钟: %d", nodeID, clock)
	} else {
		wasOffline := !nodeInfo.IsOnline
		nodeInfo.LastHeartbeat = now
		nodeInfo.LastKnownClock = clock
		nodeInfo.IsOnline = true

		if wasOffline {
			log.Printf("✅ 节点 %s 重新上线", nodeID)
			hm.nm.clockSync.HandleNodeRejoin(nodeID, clock)
		}
	}

	if clock > 0 {
		hm.nm.UpdateLocalClock(clock)
	}
}

func (hm *HeartbeatMonitor) broadcastHeartbeat() {
	if hm.nm == nil {
		return
	}

	clock := hm.nm.db.Clock().Now()
	if err := hm.nm.BroadcastHeartbeat(clock); err != nil {
		log.Printf("heartbeat broadcast failed: %v", err)
	}
}

// checkHeartbeats checks all peers and marks timeout peers offline.
func (hm *HeartbeatMonitor) checkHeartbeats() {
	hm.nm.mu.Lock()
	defer hm.nm.mu.Unlock()

	now := time.Now()
	newTimeouts := 0

	for nodeID, nodeInfo := range hm.nm.nodes {
		if nodeID == hm.nm.localNodeID {
			continue
		}

		elapsed := now.Sub(nodeInfo.LastHeartbeat)
		if elapsed <= hm.timeout {
			continue
		}

		// Only report once when online->offline transitions.
		if nodeInfo.IsOnline {
			log.Printf("⚠️ 节点 %s 超时 (%v)，标记为离线", nodeID, elapsed)
			newTimeouts++
		}
		nodeInfo.IsOnline = false
	}

	if newTimeouts > 0 {
		log.Printf("📊 检测到 %d 个节点超时", newTimeouts)
	}
}

// Stop stops heartbeat monitor.
func (hm *HeartbeatMonitor) Stop() {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if hm.cancel != nil {
		hm.cancel()
		log.Println("🛑 心跳监控已停止")
	}
}

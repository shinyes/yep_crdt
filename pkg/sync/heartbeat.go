package sync

import (
	"context"
	"log"
	"sync"
	"time"
)

// HeartbeatMonitor 心跳监控器
type HeartbeatMonitor struct {
	nm       *NodeManager
	interval time.Duration
	timeout  time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
}

// NewHeartbeatMonitor 创建心跳监控器
func NewHeartbeatMonitor(nm *NodeManager, interval time.Duration, timeout time.Duration) *HeartbeatMonitor {
	return &HeartbeatMonitor{
		nm:       nm,
		interval: interval,
		timeout:  timeout,
	}
}

// Start 启动心跳监控
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
				hm.checkHeartbeats()
			}
		}
	}()
	
	log.Printf("✅ 心跳监控已启动: 间隔=%v, 超时=%v", hm.interval, hm.timeout)
}

// OnHeartbeat 处理收到的心跳
func (hm *HeartbeatMonitor) OnHeartbeat(nodeID string, clock int64) {
	hm.nm.mu.Lock()
	defer hm.nm.mu.Unlock()
	
	now := time.Now()
	
	// 获取或创建节点信息
	nodeInfo, exists := hm.nm.nodes[nodeID]
	if !exists {
		nodeInfo = &NodeInfo{
			ID:             nodeID,
			IsOnline:        true,
			LastHeartbeat:   now,
			LastKnownClock:  clock,
			LastSyncTime:   now,
		}
		hm.nm.nodes[nodeID] = nodeInfo
		
		log.Printf("✨ 新节点加入: %s, 时钟: %d", nodeID, clock)
	} else {
		// 更新心跳和时钟
		oldOnline := nodeInfo.IsOnline
		nodeInfo.LastHeartbeat = now
		nodeInfo.LastKnownClock = clock
		
		// 如果之前离线，现在上线了
		if !oldOnline {
			log.Printf("✅ 节点 %s 重新上线！", nodeID)
			hm.nm.clockSync.HandleNodeRejoin(nodeID, clock)
		}
		
		nodeInfo.IsOnline = true
	}
	
	// 更新本地时钟
	hm.nm.UpdateLocalClock(clock)
}

// checkHeartbeats 检查所有节点的心跳
func (hm *HeartbeatMonitor) checkHeartbeats() {
	hm.nm.mu.Lock()
	defer hm.nm.mu.Unlock()
	
	now := time.Now()
	timeoutNodes := make([]string, 0)
	
	for nodeID, nodeInfo := range hm.nm.nodes {
		// 跳过本地节点
		if nodeID == hm.nm.localNodeID {
			continue
		}
		
		// 检查是否超时
		elapsed := now.Sub(nodeInfo.LastHeartbeat)
		if elapsed > hm.timeout {
			log.Printf("⚠️ 节点 %s 超时 (%v)，标记为离线", 
				nodeID, elapsed)
			
			nodeInfo.IsOnline = false
			timeoutNodes = append(timeoutNodes, nodeID)
		}
	}
	
	if len(timeoutNodes) > 0 {
		log.Printf("📊 检测到 %d 个节点超时", len(timeoutNodes))
	}
}

// Stop 停止心跳监控
func (hm *HeartbeatMonitor) Stop() {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	
	if hm.cancel != nil {
		hm.cancel()
		log.Println("🛑 心跳监控已停止")
	}
}

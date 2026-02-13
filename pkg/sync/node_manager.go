package sync

import (
	"context"
	"log"
	"sync"

	"github.com/shinyes/yep_crdt/pkg/db"
)

// NodeManager 节点管理器
type NodeManager struct {
	mu          sync.RWMutex
	nodes       map[string]*NodeInfo
	localNodeID string
	db          *db.DB
	config      Config

	// 子组件
	heartbeat *HeartbeatMonitor
	gc        *GCManager
	dataSync  *DataSyncManager
	clockSync *ClockSync
	network   NetworkInterface
}

// NewNodeManager 创建节点管理器
func NewNodeManager(database *db.DB, nodeID string, opts ...Option) *NodeManager {
	// 应用配置
	config := DefaultConfig()
	for _, opt := range opts {
		opt(&config)
	}

	nm := &NodeManager{
		nodes:       make(map[string]*NodeInfo),
		localNodeID: nodeID,
		db:          database,
		config:      config,
		network:     &DefaultNetwork{}, // 使用默认网络实现
	}

	// 初始化子组件
	nm.heartbeat = NewHeartbeatMonitor(nm, config.HeartbeatInterval, config.TimeoutThreshold)
	nm.gc = NewGCManager(nm, config.GCInterval, config.GCTimeOffset)
	nm.dataSync = NewDataSyncManager(database, nodeID)
	nm.clockSync = NewClockSync(nm, config.ClockThreshold)

	return nm
}

// Start 启动节点管理器
func (nm *NodeManager) Start(ctx context.Context) {
	log.Printf("节点管理器启动: 本地节点=%s", nm.localNodeID)

	// 启动心跳监控
	nm.heartbeat.Start(ctx)

	// 启动 GC
	nm.gc.Start(ctx)

	log.Println("节点管理器已启动")
}

// Stop 停止节点管理器
func (nm *NodeManager) Stop() {
	log.Println("停止节点管理器")

	// 停止心跳监控
	nm.heartbeat.Stop()

	// 停止 GC
	nm.gc.Stop()
}

// OnHeartbeat 处理收到的心跳
func (nm *NodeManager) OnHeartbeat(nodeID string, clock int64) {
	nm.heartbeat.OnHeartbeat(nodeID, clock)
}

// GetNodeInfo 获取节点信息
func (nm *NodeManager) GetNodeInfo(nodeID string) (*NodeInfo, bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	nodeInfo, exists := nm.nodes[nodeID]
	return nodeInfo, exists
}

// IsNodeOnline 检查节点是否在线
func (nm *NodeManager) IsNodeOnline(nodeID string) bool {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if nodeInfo, exists := nm.nodes[nodeID]; exists {
		return nodeInfo.IsOnline
	}
	return false
}

// GetOnlineNodes 获取所有在线节点
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

// CalculateSafeTimestamp 计算安全时间戳
func (nm *NodeManager) CalculateSafeTimestamp() int64 {
	nm.mu.RLock()

	// 只考虑在线的节点
	var minClock int64
	first := true

	for _, nodeInfo := range nm.nodes {
		// 只考虑在线的节点
		if !nodeInfo.IsOnline {
			continue
		}

		// 跳过本地节点
		if nodeInfo.ID == nm.localNodeID {
			continue
		}

		if first || nodeInfo.LastKnownClock < minClock {
			minClock = nodeInfo.LastKnownClock
			first = false
		}
	}

	nm.mu.RUnlock()

	if first {
		// 没有其他在线节点，使用保守估计
		// 在锁以外获取本地时钟，避免死锁
		myClock := nm.db.Clock().Now()
		conservativeOffset := int64(30 * 1000) // 30 秒
		return myClock - conservativeOffset
	}

	// 应用安全容差
	safetyMargin := int64(5 * 1000) // 5 秒
	return minClock - safetyMargin
}

// OnReceiveMerge 处理收到的原始 CRDT 字节数据并执行 Merge
func (nm *NodeManager) OnReceiveMerge(table string, key string, rawData []byte, timestamp int64) error {
	return nm.dataSync.OnReceiveMerge(table, key, rawData, timestamp)
}

// UpdateLocalClock 更新本地时钟
func (nm *NodeManager) UpdateLocalClock(remoteClock int64) {
	nm.db.Clock().Update(remoteClock)
	log.Printf("本地时钟已更新: %d", remoteClock)
}

// RegisterNetwork 注册网络接口
func (nm *NodeManager) RegisterNetwork(network NetworkInterface) {
	nm.network = network
	nm.dataSync.SetNetwork(network)
	log.Println("网络接口已注册")
}

// BroadcastHeartbeat 广播心跳
func (nm *NodeManager) BroadcastHeartbeat(clock int64) error {
	if nm.network == nil {
		return nil // 没有网络接口，跳过
	}

	return nm.network.BroadcastHeartbeat(clock)
}

// BroadcastRawData 广播原始 CRDT 字节数据
func (nm *NodeManager) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	if nm.network == nil {
		return nil // 没有网络接口，跳过
	}

	return nm.network.BroadcastRawData(table, key, rawData, timestamp)
}

// FetchRawTableData 获取远程节点的原始表数据
func (nm *NodeManager) FetchRawTableData(sourceNodeID, tableName string) ([]RawRowData, error) {
	if nm.network == nil {
		return nil, ErrNoNetwork
	}

	return nm.network.FetchRawTableData(sourceNodeID, tableName)
}

// GetLocalNodeID 获取本地节点 ID
func (nm *NodeManager) GetLocalNodeID() string {
	return nm.localNodeID
}

// FullSync 从指定节点执行全量同步
func (nm *NodeManager) FullSync(ctx context.Context, sourceNodeID string) (*SyncResult, error) {
	return nm.dataSync.FullSync(ctx, sourceNodeID)
}

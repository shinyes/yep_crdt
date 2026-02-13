package sync

import (
	"context"
	"fmt"
	stdlog "log"
	"sync"

	"github.com/shinyes/yep_crdt/pkg/db"
)

// MultiTenantManager 多租户管理器
// 管理多个租户的网络同步，每个租户有独立的 tenet 频道
type MultiTenantManager struct {
	mu          sync.RWMutex
	tenants     map[string]*TenantNodeManager // tenantID -> TenantNodeManager
	tenetConfig *TenetConfig                  // 共享的 tenet 配置

	// 全局回调
	OnTenantConnected func(tenantID, peerID string)
	OnTenantDisconnected func(tenantID, peerID string)
}

// TenantNodeManager 租户节点管理器
// 每个租户对应一个独立的节点管理器，用于该租户内的数据同步
type TenantNodeManager struct {
	tenantID    string           // 租户 ID (DatabaseID)
	db          *db.DB           // 数据库实例
	nodeMgr     *NodeManager    // 节点管理器
	network     *TenantNetwork  // tenet 网络
	multitenancy *MultiTenantManager
}

// NewMultiTenantManager 创建多租户管理器
func NewMultiTenantManager(tenetConfig *TenetConfig) *MultiTenantManager {
	return &MultiTenantManager{
		tenants:     make(map[string]*TenantNodeManager),
		tenetConfig: tenetConfig,
	}
}

// StartTenant 启动租户网络
// database: 租户数据库实例
func (m *MultiTenantManager) StartTenant(ctx context.Context, database *db.DB) (*TenantNodeManager, error) {
	tenantID := database.DatabaseID

	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查租户是否已存在
	if _, exists := m.tenants[tenantID]; exists {
		return nil, fmt.Errorf("tenant %s already started", tenantID)
	}

	// 创建租户网络（使用租户 ID 作为频道 ID）
	network, err := NewTenantNetwork(tenantID, m.tenetConfig)
	if err != nil {
		return nil, fmt.Errorf("create tenant network failed: %w", err)
	}

	// 启动 tenet 网络
	if err := network.Start(); err != nil {
		return nil, fmt.Errorf("start tenant network failed: %w", err)
	}

	// 创建节点管理器
	nodeMgr := NewNodeManager(database, network.LocalID())
	nodeMgr.network = network

	// 设置消息处理（使用广播处理器接收所有消息）
	network.SetBroadcastHandler(PeerMessageHandler{
		OnReceive: func(peerID string, msg *NetworkMessage) {
			m.handleMessage(tenantID, peerID, msg)
		},
	})

	// 启动节点管理器
	nodeMgr.Start(ctx)

	// 创建租户节点管理器
	tnm := &TenantNodeManager{
		tenantID:    tenantID,
		db:          database,
		nodeMgr:     nodeMgr,
		network:     network,
		multitenancy: m,
	}

	// 触发连接回调
	if m.OnTenantConnected != nil {
		network.tunnel.OnPeerConnected(func(peerID string) {
			m.OnTenantConnected(tenantID, peerID)
		})
		network.tunnel.OnPeerDisconnected(func(peerID string) {
			m.OnTenantDisconnected(tenantID, peerID)
		})
	}

	m.tenants[tenantID] = tnm

	stdlog.Printf("[MultiTenantManager] 租户 %s 已启动, 节点ID: %s", tenantID, network.LocalID())

	return tnm, nil
}

// StopTenant 停止租户网络
func (m *MultiTenantManager) StopTenant(tenantID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tnm, exists := m.tenants[tenantID]
	if !exists {
		return fmt.Errorf("tenant %s not found", tenantID)
	}

	// 停止节点管理器
	tnm.nodeMgr.Stop()

	// 停止租户网络
	tnm.network.Stop()

	// 清理
	delete(m.tenants, tenantID)

	stdlog.Printf("[MultiTenantManager] 租户 %s 已停止", tenantID)

	return nil
}

// GetTenant 获取租户节点管理器
func (m *MultiTenantManager) GetTenant(tenantID string) (*TenantNodeManager, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tnm, exists := m.tenants[tenantID]
	return tnm, exists
}

// ListTenants 列出所有租户
func (m *MultiTenantManager) ListTenants() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tenants := make([]string, 0, len(m.tenants))
	for tenantID := range m.tenants {
		tenants = append(tenants, tenantID)
	}

	return tenants
}

// Connect 连接到一个节点（该节点上的相同租户将收到消息）
func (m *MultiTenantManager) Connect(tenantID string, addr string) error {
	tnm, exists := m.tenants[tenantID]
	if !exists {
		return fmt.Errorf("tenant %s not found", tenantID)
	}

	return tnm.network.Connect(addr)
}

// handleMessage 处理收到的消息
func (m *MultiTenantManager) handleMessage(tenantID, peerID string, msg *NetworkMessage) {
	tnm, exists := m.tenants[tenantID]
	if !exists {
		stdlog.Printf("[MultiTenantManager] 收到未知租户的消息: %s", tenantID)
		return
	}

	switch msg.Type {
	case MsgTypeHeartbeat:
		// 更新节点心跳
		tnm.nodeMgr.OnHeartbeat(peerID, msg.Clock)
		// 更新本地时钟
		if msg.Clock > 0 {
			tnm.nodeMgr.UpdateLocalClock(msg.Clock)
		}

	case MsgTypeData:
		// 处理数据同步
		if msg.Table != "" && msg.Key != "" {
			stdlog.Printf("[MultiTenantManager:%s] 收到数据: table=%s, key=%s, from=%s", tenantID, msg.Table, msg.Key, peerID[:8])
			stdlog.Printf("[MultiTenantManager:%s]   数据时间戳=%d, 本地时钟=%d", tenantID, msg.Timestamp, tnm.db.Clock().Now())
			err := tnm.nodeMgr.OnReceiveData(msg.Table, msg.Key, msg.Data, msg.Timestamp)
			if err != nil {
				stdlog.Printf("[MultiTenantManager:%s] 处理数据失败: %v", tenantID, err)
			} else {
				stdlog.Printf("[MultiTenantManager:%s] 数据处理成功", tenantID)
			}
		}

	case MsgTypeFetchRequest:
		// 处理数据获取请求
		m.handleFetchRequest(tnm, peerID, msg)

	case MsgTypeFetchResponse:
		// 响应已由 TenantNetwork 的 SendWithResponse 处理
	}
}

// handleFetchRequest 处理数据获取请求
func (m *MultiTenantManager) handleFetchRequest(tnm *TenantNodeManager, peerID string, msg *NetworkMessage) {
	if msg.Table == "" {
		return
	}

	// 从数据库获取表数据（简化实现：获取所有表的元数据）
	// 实际实现需要 Table 提供获取所有 key 的方法
	var resultData map[string]map[string]any

	err := tnm.db.View(func(tx *db.Tx) error {
		table := tx.Table(msg.Table)
		if table == nil {
			return fmt.Errorf("table not found: %s", msg.Table)
		}

		// 由于 Table 没有提供 Iterator，我们这里返回一个空结果
		// 实际实现需要在 db.Table 中添加获取所有 key 的方法
		resultData = make(map[string]map[string]any)

		// TODO: 需要在 Table 中添加获取所有数据的方法
		// 目前只能通过其他方式获取数据，例如：
		// - 遍历所有可能的 UUID（不实际）
		// - 添加一个新的 API 方法

		return nil
	})

	if err != nil {
		stdlog.Printf("[MultiTenantManager:%s] 获取表数据失败: %v", tnm.tenantID, err)
		return
	}

	// 发送响应
	responseMsg := &NetworkMessage{
		Type:      MsgTypeFetchResponse,
		RequestID: msg.RequestID,
		Data:      resultData,
	}

	err = tnm.network.Send(peerID, responseMsg)
	if err != nil {
		stdlog.Printf("[MultiTenantManager:%s] 发送响应失败: %v", tnm.tenantID, err)
	}
}

// GetNodeManager 获取租户的节点管理器
func (tnm *TenantNodeManager) GetNodeManager() *NodeManager {
	return tnm.nodeMgr
}

// GetNetwork 获取租户网络
func (tnm *TenantNodeManager) GetNetwork() *TenantNetwork {
	return tnm.network
}

// GetDatabase 获取租户数据库
func (tnm *TenantNodeManager) GetDatabase() *db.DB {
	return tnm.db
}

// TenantID 获取租户 ID
func (tnm *TenantNodeManager) TenantID() string {
	return tnm.tenantID
}

// Connect 连接到其他节点
func (tnm *TenantNodeManager) Connect(addr string) error {
	return tnm.network.Connect(addr)
}

// BroadcastData 广播数据到同租户的所有节点
func (tnm *TenantNodeManager) BroadcastData(table string, key string, data any, timestamp int64) error {
	return tnm.network.BroadcastData(table, key, data, timestamp)
}

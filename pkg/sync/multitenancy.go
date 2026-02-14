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
	OnTenantConnected    func(tenantID, peerID string)
	OnTenantDisconnected func(tenantID, peerID string)
}

// TenantNodeManager 租户节点管理器
// 每个租户对应一个独立的节点管理器，用于该租户内的数据同步
type TenantNodeManager struct {
	tenantID     string         // 租户 ID (DatabaseID)
	db           *db.DB         // 数据库实例
	nodeMgr      *NodeManager   // 节点管理器
	network      *TenantNetwork // tenet 网络
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
	nodeMgr.RegisterNetwork(network)

	// 设置消息处理（使用广播处理器接收所有消息）
	network.SetBroadcastHandler(PeerMessageHandler{
		OnReceive: func(peerID string, msg NetworkMessage) {
			m.handleMessage(tenantID, peerID, msg)
		},
	})

	// 启动节点管理器
	nodeMgr.Start(ctx)

	// 创建租户节点管理器
	tnm := &TenantNodeManager{
		tenantID:     tenantID,
		db:           database,
		nodeMgr:      nodeMgr,
		network:      network,
		multitenancy: m,
	}

	// 通过 TenantNetwork 注册回调，避免覆盖内部回调。
	if m.OnTenantConnected != nil {
		network.AddPeerConnectedHandler(func(peerID string) {
			m.OnTenantConnected(tenantID, peerID)
		})
	}
	if m.OnTenantDisconnected != nil {
		network.AddPeerDisconnectedHandler(func(peerID string) {
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
func (m *MultiTenantManager) handleMessage(tenantID, peerID string, msg NetworkMessage) {
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

	case MsgTypeRawData:
		// 处理原始 CRDT 字节同步
		if msg.Table != "" && msg.Key != "" && msg.RawData != nil {
			stdlog.Printf("[MultiTenantManager:%s] 收到原始数据: table=%s, key=%s, from=%s", tenantID, msg.Table, msg.Key, shortPeerID(peerID))
			err := tnm.nodeMgr.OnReceiveMerge(msg.Table, msg.Key, msg.RawData, msg.Timestamp)
			if err != nil {
				stdlog.Printf("[MultiTenantManager:%s] Merge 数据失败: %v", tenantID, err)
			} else {
				stdlog.Printf("[MultiTenantManager:%s] Merge 数据成功", tenantID)
			}
		}

	case MsgTypeRawDelta:
		if msg.Table != "" && msg.Key != "" && msg.RawData != nil {
			stdlog.Printf("[MultiTenantManager:%s] received delta data: table=%s, key=%s, cols=%v, from=%s",
				tenantID, msg.Table, msg.Key, msg.Columns, shortPeerID(peerID))
			if err := tnm.nodeMgr.OnReceiveDelta(msg.Table, msg.Key, msg.Columns, msg.RawData, msg.Timestamp); err != nil {
				stdlog.Printf("[MultiTenantManager:%s] merge delta failed: %v", tenantID, err)
			}
		}

	case MsgTypeFetchRawRequest:
		// 处理原始数据获取请求
		m.handleFetchRawRequest(tnm, peerID, msg)

	case MsgTypeFetchRawResponse:
		// 响应已由 TenantNetwork 请求等待器处理
	}
}

// handleFetchRawRequest 处理原始数据获取请求
func (m *MultiTenantManager) handleFetchRawRequest(tnm *TenantNodeManager, peerID string, msg NetworkMessage) {
	if msg.Table == "" {
		return
	}

	// 使用 DataSyncManager 导出表的原始数据
	rawRows, err := tnm.nodeMgr.dataSync.ExportTableRawData(msg.Table)
	if err != nil {
		stdlog.Printf("[MultiTenantManager:%s] 导出表数据失败: %v", tnm.tenantID, err)
		return
	}

	// 逐行发送原始数据（用响应消息格式）
	for _, row := range rawRows {
		responseMsg := &NetworkMessage{
			Type:      MsgTypeFetchRawResponse,
			RequestID: msg.RequestID,
			Table:     msg.Table,
			Key:       row.Key,
			RawData:   row.Data,
		}

		err := tnm.network.Send(peerID, responseMsg)
		if err != nil {
			stdlog.Printf("[MultiTenantManager:%s] 发送行数据失败: %v", tnm.tenantID, err)
		}
	}

	doneMsg := &NetworkMessage{
		Type:      MsgTypeFetchRawResponse,
		RequestID: msg.RequestID,
		Table:     msg.Table,
		Key:       fetchRawResponseDoneKey,
	}
	if err := tnm.network.Send(peerID, doneMsg); err != nil {
		stdlog.Printf("[MultiTenantManager:%s] 发送 fetch 结束标记失败: %v", tnm.tenantID, err)
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

// BroadcastRawData 广播原始 CRDT 字节到同租户的所有节点
func (tnm *TenantNodeManager) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	return tnm.network.BroadcastRawData(table, key, rawData, timestamp)
}

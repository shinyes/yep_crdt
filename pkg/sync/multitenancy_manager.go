package sync

import (
	"context"
	"fmt"
	stdlog "log"

	"github.com/shinyes/yep_crdt/pkg/db"
)

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

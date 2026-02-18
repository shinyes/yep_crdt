package sync

import (
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
	chunks       *localFileChunkReceiver
}

// NewMultiTenantManager 创建多租户管理器
func NewMultiTenantManager(tenetConfig *TenetConfig) *MultiTenantManager {
	return &MultiTenantManager{
		tenants:     make(map[string]*TenantNodeManager),
		tenetConfig: tenetConfig,
	}
}

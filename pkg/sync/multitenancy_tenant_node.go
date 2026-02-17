package sync

import "github.com/shinyes/yep_crdt/pkg/db"

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

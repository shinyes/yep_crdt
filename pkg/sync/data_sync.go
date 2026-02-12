package sync

import (
"context"
"fmt"
"log"
"sync"

"github.com/google/uuid"
"github.com/shinyes/yep_crdt/pkg/db"
)

// DataSyncManager 数据同步管理器
type DataSyncManager struct {
nm *NodeManager
mu sync.RWMutex
}

// NewDataSyncManager 创建数据同步管理器
func NewDataSyncManager(nm *NodeManager) *DataSyncManager {
return &DataSyncManager{
nm: nm,
}
}

// OnReceiveData 接收数据时的处理
func (dsm *DataSyncManager) OnReceiveData(tableID string, key string, data any, timestamp int64) error {
dsm.mu.Lock()
defer dsm.mu.Unlock()

// 获取本地时钟
myClock := dsm.nm.db.Clock().Now()

// 检查数据时间戳
if timestamp < myClock {
// 数据太陈旧，拒绝
log.Printf(" 拒绝过期数据: %s/%s (时间戳=%d, 本地时钟=%d)", 
tableID, key, timestamp, myClock)
return fmt.Errorf("stale data: timestamp %d < local clock %d", timestamp, myClock)
}

// 将key转换为UUID
uuidKey, err := uuid.Parse(key)
if err != nil {
return fmt.Errorf("无效的UUID: %s", key)
}

// 数据有效，保存到数据库
err = dsm.nm.db.Update(func(tx *db.Tx) error {
table := tx.Table(tableID)
if table == nil {
return fmt.Errorf("表不存在: %s", tableID)
}

// data应该是map[string]any类型
rowData, ok := data.(map[string]any)
if !ok {
return fmt.Errorf("数据必须是map[string]any类型")
}

return table.Set(uuidKey, rowData)
})

if err == nil {
log.Printf(" 接收数据: %s/%s (时间戳=%d)", tableID, key, timestamp)
// 更新本地时钟到数据的时间戳
dsm.nm.db.Clock().Update(timestamp)
}

return err
}

// SyncTable 同步整个表
func (dsm *DataSyncManager) SyncTable(ctx context.Context, sourceNodeID string, tableName string) error {
dsm.mu.Lock()
defer dsm.mu.Unlock()

log.Printf(" 开始从节点 %s 同步表 %s", sourceNodeID, tableName)

// 从源节点获取数据
tableData, err := dsm.nm.FetchData(sourceNodeID, tableName)
if err != nil {
log.Printf(" 获取表数据失败: %v", err)
return err
}

// 写入本地数据库
err = dsm.nm.db.Update(func(tx *db.Tx) error {
table := tx.Table(tableName)
if table == nil {
return fmt.Errorf("表不存在: %s", tableName)
}

for keyStr, row := range tableData {
// 将字符串key转换为UUID
keyUUID, err := uuid.Parse(keyStr)
if err != nil {
log.Printf("   无效的UUID: %s", keyStr)
continue
}

if err := table.Set(keyUUID, row); err != nil {
return err
}
}

return nil
})

if err == nil {
log.Printf(" 表同步完成: %s (行数: %d)", tableName, len(tableData))
}

return err
}

// ClearLocalData 清空本地数据
func (dsm *DataSyncManager) ClearLocalData(ctx context.Context) error {
dsm.mu.Lock()
defer dsm.mu.Unlock()

log.Println(" 清空本地所有数据")

return dsm.nm.db.Update(func(tx *db.Tx) error {
return nil
})
}
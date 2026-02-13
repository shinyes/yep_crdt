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
// 负责基于 CRDT Merge 的增量同步和全量同步。
type DataSyncManager struct {
	mu      sync.RWMutex
	db      *db.DB
	nodeID  string
	network NetworkInterface
}

// NewDataSyncManager 创建数据同步管理器
func NewDataSyncManager(database *db.DB, nodeID string) *DataSyncManager {
	return &DataSyncManager{
		db:     database,
		nodeID: nodeID,
	}
}

// SetNetwork 设置网络接口
func (dsm *DataSyncManager) SetNetwork(n NetworkInterface) {
	dsm.mu.Lock()
	defer dsm.mu.Unlock()
	dsm.network = n
}

// OnReceiveMerge 接收远程 CRDT 原始字节并 Merge 到本地。
// 这是增量同步的核心方法：收到远端发来的某行的完整 CRDT 状态后，
// 与本地对应行进行无冲突合并。
func (dsm *DataSyncManager) OnReceiveMerge(tableName string, keyStr string, rawData []byte, timestamp int64) error {
	dsm.mu.RLock()
	defer dsm.mu.RUnlock()

	// 时间戳检查：拒绝过于陈旧的数据
	myClock := dsm.db.Clock().Now()
	if timestamp < myClock {
		return fmt.Errorf("拒绝过期数据: 远程时间戳 %d < 本地时钟 %d", timestamp, myClock)
	}

	// 更新本地时钟（HLC 语义：接收消息时推进时钟）
	dsm.db.Clock().Update(timestamp)

	// 解析 UUID
	key, err := uuid.Parse(keyStr)
	if err != nil {
		return fmt.Errorf("解析 UUID 失败: %w", err)
	}

	// 获取表实例
	table := dsm.db.Table(tableName)
	if table == nil {
		return fmt.Errorf("表不存在: %s", tableName)
	}

	// 执行 CRDT Merge
	if err := table.MergeRawRow(key, rawData); err != nil {
		return fmt.Errorf("Merge 行数据失败: %w", err)
	}

	log.Printf("[DataSync] 成功合并数据: table=%s, key=%s, timestamp=%d",
		tableName, keyStr, timestamp)
	return nil
}

// BroadcastRow 广播本地行的原始 CRDT 字节（增量同步的发送端）。
// 当本地数据发生变更时调用，将变更后的完整 CRDT 状态广播给所有节点。
func (dsm *DataSyncManager) BroadcastRow(tableName string, key uuid.UUID) error {
	dsm.mu.RLock()
	network := dsm.network
	dsm.mu.RUnlock()

	if network == nil {
		return ErrNoNetwork
	}

	// 获取表实例
	table := dsm.db.Table(tableName)
	if table == nil {
		return fmt.Errorf("表不存在: %s", tableName)
	}

	// 获取该行的原始 CRDT 字节
	rawData, err := table.GetRawRow(key)
	if err != nil {
		return fmt.Errorf("获取原始 CRDT 数据失败: %w", err)
	}

	// 广播
	timestamp := dsm.db.Clock().Now()
	return network.BroadcastRawData(tableName, key.String(), rawData, timestamp)
}

// FullSyncTable 对指定表执行全量同步（从远程拉取所有数据并 Merge）。
// 用于节点重新加入或时钟差距过大时的数据恢复。
func (dsm *DataSyncManager) FullSyncTable(ctx context.Context, sourceNodeID string, tableName string) (*SyncResult, error) {
	dsm.mu.RLock()
	network := dsm.network
	dsm.mu.RUnlock()

	if network == nil {
		return nil, ErrNoNetwork
	}

	result := &SyncResult{}

	// 从远程节点获取表的所有原始数据
	rows, err := network.FetchRawTableData(sourceNodeID, tableName)
	if err != nil {
		return nil, fmt.Errorf("获取远程表数据失败: %w", err)
	}

	// 获取表实例
	table := dsm.db.Table(tableName)
	if table == nil {
		return nil, fmt.Errorf("表不存在: %s", tableName)
	}

	// 逐行 Merge
	for _, row := range rows {
		// 检查 context 是否被取消
		select {
		case <-ctx.Done():
			result.Errors = append(result.Errors, ctx.Err())
			return result, ctx.Err()
		default:
		}

		key, err := uuid.Parse(row.Key)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("解析 UUID '%s' 失败: %w", row.Key, err))
			result.RejectedCount++
			continue
		}

		if err := table.MergeRawRow(key, row.Data); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("Merge 行 '%s' 失败: %w", row.Key, err))
			result.RejectedCount++
			continue
		}

		result.RowsSynced++
	}

	result.TablesSynced = 1
	log.Printf("[DataSync] 全量同步完成: table=%s, rows=%d, rejected=%d",
		tableName, result.RowsSynced, result.RejectedCount)
	return result, nil
}

// FullSync 对所有表执行全量同步。
func (dsm *DataSyncManager) FullSync(ctx context.Context, sourceNodeID string) (*SyncResult, error) {
	totalResult := &SyncResult{}

	// 获取所有表名
	tableNames := dsm.db.TableNames()

	for _, tableName := range tableNames {
		select {
		case <-ctx.Done():
			totalResult.Errors = append(totalResult.Errors, ctx.Err())
			return totalResult, ctx.Err()
		default:
		}

		result, err := dsm.FullSyncTable(ctx, sourceNodeID, tableName)
		if err != nil {
			totalResult.Errors = append(totalResult.Errors, fmt.Errorf("全量同步表 '%s' 失败: %w", tableName, err))
			continue
		}

		totalResult.TablesSynced += result.TablesSynced
		totalResult.RowsSynced += result.RowsSynced
		totalResult.RowsMerged += result.RowsMerged
		totalResult.RowsCreated += result.RowsCreated
		totalResult.RejectedCount += result.RejectedCount
		totalResult.Errors = append(totalResult.Errors, result.Errors...)
	}

	log.Printf("[DataSync] 全量同步所有表完成: tables=%d, rows=%d",
		totalResult.TablesSynced, totalResult.RowsSynced)
	return totalResult, nil
}

// ExportTableRawData 导出指定表的所有原始 CRDT 数据。
// 用于响应远程节点的全量同步请求。
func (dsm *DataSyncManager) ExportTableRawData(tableName string) ([]RawRowData, error) {
	table := dsm.db.Table(tableName)
	if table == nil {
		return nil, fmt.Errorf("表不存在: %s", tableName)
	}

	rawRows, err := table.ScanRawRows()
	if err != nil {
		return nil, fmt.Errorf("扫描表数据失败: %w", err)
	}

	result := make([]RawRowData, 0, len(rawRows))
	for _, row := range rawRows {
		result = append(result, RawRowData{
			Key:  row.Key.String(),
			Data: row.Data,
		})
	}

	return result, nil
}

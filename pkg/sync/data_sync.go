package sync

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
)

// DataSyncManager handles CRDT merge-based incremental/full sync.
type DataSyncManager struct {
	mu      sync.RWMutex
	db      *db.DB
	nodeID  string
	network NetworkInterface
}

// NewDataSyncManager creates a sync manager.
func NewDataSyncManager(database *db.DB, nodeID string) *DataSyncManager {
	return &DataSyncManager{
		db:     database,
		nodeID: nodeID,
	}
}

// SetNetwork registers network transport.
func (dsm *DataSyncManager) SetNetwork(n NetworkInterface) {
	dsm.mu.Lock()
	defer dsm.mu.Unlock()
	dsm.network = n
}

// OnReceiveMerge applies a full-row raw CRDT state payload.
func (dsm *DataSyncManager) OnReceiveMerge(tableName string, keyStr string, rawData []byte, timestamp int64) error {
	return dsm.applyIncomingRaw(tableName, keyStr, rawData, timestamp, nil)
}

// OnReceiveDelta applies a column-level partial raw CRDT state payload.
func (dsm *DataSyncManager) OnReceiveDelta(tableName string, keyStr string, columns []string, rawData []byte, timestamp int64) error {
	return dsm.applyIncomingRaw(tableName, keyStr, rawData, timestamp, columns)
}

func (dsm *DataSyncManager) applyIncomingRaw(tableName string, keyStr string, rawData []byte, timestamp int64, columns []string) error {
	dsm.mu.RLock()
	defer dsm.mu.RUnlock()

	myClock := dsm.db.Clock().Now()
	if timestamp < myClock {
		return fmt.Errorf("拒绝过期数据: 远程时间戳 %d < 本地时钟 %d", timestamp, myClock)
	}

	dsm.db.Clock().Update(timestamp)

	key, err := uuid.Parse(keyStr)
	if err != nil {
		return fmt.Errorf("解析 UUID 失败: %w", err)
	}

	table := dsm.db.Table(tableName)
	if table == nil {
		return fmt.Errorf("表不存在: %s", tableName)
	}

	if err := table.MergeRawRow(key, rawData); err != nil {
		return fmt.Errorf("Merge 行数据失败: %w", err)
	}

	if len(columns) == 0 {
		log.Printf("[DataSync] merged full row: table=%s, key=%s, ts=%d", tableName, keyStr, timestamp)
	} else {
		log.Printf("[DataSync] merged delta row: table=%s, key=%s, cols=%v, ts=%d", tableName, keyStr, columns, timestamp)
	}
	return nil
}

// BroadcastRow broadcasts the full raw CRDT row state.
func (dsm *DataSyncManager) BroadcastRow(tableName string, key uuid.UUID) error {
	dsm.mu.RLock()
	network := dsm.network
	dsm.mu.RUnlock()

	if network == nil {
		return ErrNoNetwork
	}

	table := dsm.db.Table(tableName)
	if table == nil {
		return fmt.Errorf("表不存在: %s", tableName)
	}

	rawData, err := table.GetRawRow(key)
	if err != nil {
		return fmt.Errorf("获取原始 CRDT 数据失败: %w", err)
	}

	timestamp := dsm.db.Clock().Now()
	return network.BroadcastRawData(tableName, key.String(), rawData, timestamp)
}

// BroadcastRowDelta broadcasts only selected columns of a row.
func (dsm *DataSyncManager) BroadcastRowDelta(tableName string, key uuid.UUID, columns []string) error {
	dsm.mu.RLock()
	network := dsm.network
	dsm.mu.RUnlock()

	if network == nil {
		return ErrNoNetwork
	}

	if len(columns) == 0 {
		return dsm.BroadcastRow(tableName, key)
	}

	table := dsm.db.Table(tableName)
	if table == nil {
		return fmt.Errorf("表不存在: %s", tableName)
	}

	rawData, err := table.GetRawRowColumns(key, columns)
	if err != nil {
		return fmt.Errorf("获取列级 CRDT 数据失败: %w", err)
	}

	timestamp := dsm.db.Clock().Now()
	return network.BroadcastRawDelta(tableName, key.String(), columns, rawData, timestamp)
}

// FullSyncTable performs full sync for one table.
func (dsm *DataSyncManager) FullSyncTable(ctx context.Context, sourceNodeID string, tableName string) (*SyncResult, error) {
	dsm.mu.RLock()
	network := dsm.network
	dsm.mu.RUnlock()

	if network == nil {
		return nil, ErrNoNetwork
	}

	result := &SyncResult{}

	rows, err := network.FetchRawTableData(sourceNodeID, tableName)
	if err != nil {
		return nil, fmt.Errorf("获取远程表数据失败: %w", err)
	}

	table := dsm.db.Table(tableName)
	if table == nil {
		return nil, fmt.Errorf("表不存在: %s", tableName)
	}

	for _, row := range rows {
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
	log.Printf("[DataSync] full sync table done: table=%s, rows=%d, rejected=%d", tableName, result.RowsSynced, result.RejectedCount)
	return result, nil
}

// FullSync performs full sync for all tables.
func (dsm *DataSyncManager) FullSync(ctx context.Context, sourceNodeID string) (*SyncResult, error) {
	totalResult := &SyncResult{}
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

	log.Printf("[DataSync] full sync all done: tables=%d, rows=%d", totalResult.TablesSynced, totalResult.RowsSynced)
	return totalResult, nil
}

// ExportTableRawData exports all raw rows in one table.
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

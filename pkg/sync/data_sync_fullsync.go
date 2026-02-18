package sync

import (
	"context"
	"fmt"
	"log"
)

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
		return nil, fmt.Errorf("fetch remote table data failed: %w", err)
	}

	table := dsm.db.Table(tableName)
	if table == nil {
		return nil, fmt.Errorf("table does not exist: %s", tableName)
	}

	for _, row := range rows {
		select {
		case <-ctx.Done():
			result.Errors = append(result.Errors, ctx.Err())
			return result, ctx.Err()
		default:
		}

		if err := dsm.OnReceiveMergeWithFiles(tableName, row.Key, row.Data, 0, row.LocalFiles); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("merge row '%s' failed: %w", row.Key, err))
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
			totalResult.Errors = append(totalResult.Errors, fmt.Errorf("full sync table '%s' failed: %w", tableName, err))
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
		return nil, fmt.Errorf("table does not exist: %s", tableName)
	}

	rawRows, err := table.ScanRawRows()
	if err != nil {
		return nil, fmt.Errorf("scan table data failed: %w", err)
	}

	result := make([]RawRowData, 0, len(rawRows))
	for _, row := range rawRows {
		localFiles, err := dsm.buildLocalFilePayloadsFromRaw(row.Data)
		if err != nil {
			return nil, fmt.Errorf("collect local files for row '%s' failed: %w", row.Key.String(), err)
		}

		result = append(result, RawRowData{
			Key:        row.Key.String(),
			Data:       row.Data,
			LocalFiles: localFiles,
		})
	}

	return result, nil
}

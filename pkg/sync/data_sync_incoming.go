package sync

import (
	"fmt"
	"log"

	"github.com/google/uuid"
)

// OnReceiveMerge applies a full-row raw CRDT state payload.
func (dsm *DataSyncManager) OnReceiveMerge(tableName string, keyStr string, rawData []byte, timestamp int64) error {
	return dsm.applyIncomingRaw(tableName, keyStr, rawData, timestamp, nil)
}

// OnReceiveDelta applies a column-level partial raw CRDT state payload.
func (dsm *DataSyncManager) OnReceiveDelta(tableName string, keyStr string, columns []string, rawData []byte, timestamp int64) error {
	return dsm.applyIncomingRaw(tableName, keyStr, rawData, timestamp, columns)
}

func (dsm *DataSyncManager) applyIncomingRaw(tableName string, keyStr string, rawData []byte, timestamp int64, columns []string) error {
	// CRDT merge must remain convergent even when transport timestamps are older.
	// We only use incoming timestamp to advance local HLC when possible.
	if timestamp > 0 {
		dsm.db.Clock().Update(timestamp)
	}

	key, err := uuid.Parse(keyStr)
	if err != nil {
		return fmt.Errorf("parse UUID failed: %w", err)
	}

	table := dsm.db.Table(tableName)
	if table == nil {
		return fmt.Errorf("table does not exist: %s", tableName)
	}

	if err := table.MergeRawRow(key, rawData); err != nil {
		return fmt.Errorf("merge row failed: %w", err)
	}

	if len(columns) == 0 {
		log.Printf("[DataSync] merged full row: table=%s, key=%s, ts=%d", tableName, keyStr, timestamp)
	} else {
		log.Printf("[DataSync] merged delta row: table=%s, key=%s, cols=%v, ts=%d", tableName, keyStr, columns, timestamp)
	}
	return nil
}

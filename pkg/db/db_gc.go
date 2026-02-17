package db

import (
	"strconv"

	"github.com/shinyes/yep_crdt/pkg/store"
)

// GC 对数据库中所有 CRDT 执行垃圾回收。
// safeTimestamp 是安全的时间戳，所有在此时间戳之前被删除的数据都可以安全清理。
// 例如：safeTimestamp = db.Now() - 60000 (60 秒前)
func (db *DB) GC(safeTimestamp int64) *GCResult {
	result := &GCResult{}

	db.mu.Lock()
	tableNames := make([]string, 0, len(db.tables))
	for name := range db.tables {
		tableNames = append(tableNames, name)
	}
	db.mu.Unlock()

	result.TablesScanned = len(tableNames)

	for _, tableName := range tableNames {
		table := db.Table(tableName)
		if table == nil {
			continue
		}

		// 获取该表的所有行
		tableResult := table.GC(safeTimestamp)
		result.RowsScanned += tableResult.RowsScanned
		result.TombstonesRemoved += tableResult.TombstonesRemoved
		if len(tableResult.Errors) > 0 {
			result.Errors = append(result.Errors, tableResult.Errors...)
		}
	}

	return result
}

// GCFloor returns the persisted GC floor (max executed safe timestamp).
// If never set, returns 0.
func (db *DB) GCFloor() int64 {
	var floor int64
	if db == nil || db.store == nil {
		return 0
	}

	err := db.store.View(func(txn store.Tx) error {
		val, err := txn.Get([]byte(sysKeyGCFloor))
		if err == store.ErrKeyNotFound {
			floor = 0
			return nil
		}
		if err != nil {
			return err
		}
		parsed, parseErr := strconv.ParseInt(string(val), 10, 64)
		if parseErr != nil {
			return parseErr
		}
		floor = parsed
		return nil
	})
	if err != nil {
		return 0
	}
	return floor
}

// SetGCFloor persists monotonic GC floor.
// The stored floor is max(current floor, floor).
func (db *DB) SetGCFloor(floor int64) error {
	if db == nil || db.store == nil || floor <= 0 {
		return nil
	}

	return db.store.Update(func(txn store.Tx) error {
		currentFloor := int64(0)
		val, err := txn.Get([]byte(sysKeyGCFloor))
		if err != nil && err != store.ErrKeyNotFound {
			return err
		}
		if err == nil {
			parsed, parseErr := strconv.ParseInt(string(val), 10, 64)
			if parseErr != nil {
				return parseErr
			}
			currentFloor = parsed
		}

		if floor <= currentFloor {
			return nil
		}
		return txn.Set([]byte(sysKeyGCFloor), []byte(strconv.FormatInt(floor, 10)), 0)
	})
}

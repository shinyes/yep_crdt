package db

import (
	"time"

	"github.com/shinyes/yep_crdt/pkg/hlc"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// SetSyncEngine 设置同步引擎实例（由 sync 包调用）。
func (db *DB) SetSyncEngine(engine SyncEngine) {
	db.syncEngine = engine
}

// GetSyncEngine 获取同步引擎实例。
func (db *DB) GetSyncEngine() SyncEngine {
	return db.syncEngine
}

// Now 返回当前的混合逻辑时钟 (HLC) 时间戳。
// 这对于生成操作时间戳或计算 SafeTime 非常有用。
func (db *DB) Now() int64 {
	return db.clock.Now()
}

// Clock 返回 DB 内部持有的 Clock 实例。
// 允许外部系统进行更高级的钟同步操作 (如 Update)。
func (db *DB) Clock() *hlc.Clock {
	return db.clock
}

// DefineTable 注册表模式。
func (db *DB) DefineTable(schema *meta.TableSchema) error {
	return db.catalog.AddTable(schema)
}

// Table 返回表句柄。
func (db *DB) Table(name string) *Table {
	db.mu.Lock()
	defer db.mu.Unlock()

	if t, ok := db.tables[name]; ok {
		return t
	}

	schema, ok := db.catalog.GetTable(name)
	if !ok {
		return nil
	}

	t := &Table{
		db:           db,
		schema:       schema,
		indexManager: db.idxMgr,
	}
	db.tables[name] = t
	return t
}

// TableNames 返回所有已注册的表名。
func (db *DB) TableNames() []string {
	return db.catalog.TableNames()
}

// GetStore 返回底层 KV 存储实例。
// 同步模块需要直接访问事务能力。
func (db *DB) GetStore() store.Store {
	return db.store
}

// GCByTimeOffset 根据时间偏移量执行 GC。
// offset 是从当前时间向后的偏移量，例如 60 * time.Second 表示清理 60 秒前的数据。
func (db *DB) GCByTimeOffset(offset time.Duration) *GCResult {
	safeTimestamp := db.clock.Now() - offset.Milliseconds()
	return db.GC(safeTimestamp)
}

package db

import (
	"sync"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/hlc"
	"github.com/shinyes/yep_crdt/pkg/index"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// DB 代表数据库实例 (针对特定租户)。
type DB struct {
	store   store.Store
	catalog *meta.Catalog
	idxMgr  *index.Manager
	clock   *hlc.Clock

	mu     sync.Mutex
	tables map[string]*Table
	NodeID string
}

func Open(s store.Store) *DB {
	c := meta.NewCatalog(s)
	// Try loading existing catalog
	_ = c.Load() // Ignore error for fresh DB (or log it?)

	// Load or generate NodeID
	var nodeID string
	err := s.View(func(txn store.Tx) error {
		val, err := txn.Get([]byte("_sys/node_id"))
		if err == nil {
			nodeID = string(val)
			return nil
		}
		return err
	})

	if nodeID == "" || err == store.ErrKeyNotFound {
		nodeID = "node-" + uuid.NewString()
		// Save it
		_ = s.Update(func(txn store.Tx) error {
			return txn.Set([]byte("_sys/node_id"), []byte(nodeID), 0)
		})
	}

	return &DB{
		store:   s,
		catalog: c,
		idxMgr:  index.NewManager(),
		clock:   hlc.New(),
		tables:  make(map[string]*Table),
		NodeID:  nodeID,
	}
}

func (db *DB) Close() error {
	return db.store.Close()
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

// Tx 代表数据库事务上下文。
type Tx struct {
	db  *DB
	txn store.Tx
}

// Table 返回绑定到当前事务的表句柄。
// 如果表不存在，返回 nil。
func (tx *Tx) Table(name string) *Table {
	// 获取基础表信息
	baseTable := tx.db.Table(name)
	if baseTable == nil {
		return nil
	}

	// 创建一个新的 Table 实例，注入当前事务
	return &Table{
		db:           baseTable.db,
		schema:       baseTable.schema,
		indexManager: baseTable.indexManager,
		tx:           tx.txn,
	}
}

// Update 执行读写事务。
func (db *DB) Update(fn func(*Tx) error) error {
	return db.store.Update(func(txn store.Tx) error {
		return fn(&Tx{db: db, txn: txn})
	})
}

// View 执行只读事务。
func (db *DB) View(fn func(*Tx) error) error {
	return db.store.View(func(txn store.Tx) error {
		return fn(&Tx{db: db, txn: txn})
	})
}

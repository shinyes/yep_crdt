package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/hlc"
	"github.com/shinyes/yep_crdt/pkg/index"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// SyncConfig 同步配置
type SyncConfig struct {
	Password     string // 网络密码（必须）
	ListenPort   int    // 监听端口，0=随机
	ConnectTo    string // 初始连接地址（可选，格式 host:port）
	Debug        bool   // 启用调试日志
	IdentityPath string // tenet identity JSON 路径；为空则使用临时身份
}

// ChangeCallback 数据变更回调
// tableName: 发生变更的表名
// key: 发生变更的行主键
type ChangeCallback func(tableName string, key uuid.UUID)

// ChangeEvent carries detailed row change information for sync.
type ChangeEvent struct {
	TableName string
	Key       uuid.UUID
	Columns   []string
}

// ChangeEventCallback is invoked after a successful local write.
type ChangeEventCallback func(event ChangeEvent)

// SyncEngine 同步引擎接口
// 由 sync 包实现，DB 持有引用以避免循环依赖。
type SyncEngine interface {
	Start(ctx context.Context) error
	Stop()
	Connect(addr string) error
	Peers() []string
	LocalAddr() string
	LocalID() string
	OnDataChanged(tableName string, key uuid.UUID)
}

// DB 代表数据库实例 (针对特定租户)。
type DB struct {
	store   store.Store
	catalog *meta.Catalog
	idxMgr  *index.Manager
	clock   *hlc.Clock

	mu         sync.Mutex
	tables     map[string]*Table
	NodeID     string
	DatabaseID string

	// FileStorageDir 是存储 LocalFileCRDT 文件的根目录。
	// 如果为空，LocalFileCRDT.ReadAll 等操作将失败。
	FileStorageDir string

	// 同步引擎（由 sync 模块注入）
	syncEngine SyncEngine

	// 变更回调（数据写入时通知）
	onChangeMu             sync.RWMutex
	onChangeCallbacks      []ChangeCallback
	onChangeEventCallbacks []ChangeEventCallback
}

type Option func(*DB)

func WithFileStorageDir(dir string) Option {
	return func(db *DB) {
		db.FileStorageDir = dir
	}
}

// Open 打开数据库。
// databaseID 是数据库的唯一标识 (如 "tenant-1")。
// 如果存储中已存在 ID 且与传入的不一致，将 panic。
func Open(s store.Store, databaseID string, opts ...Option) *DB {
	c := meta.NewCatalog(s)
	// Try loading existing catalog
	if err := c.Load(); err != nil {
		// 记录错误但继续执行，可能是首次运行
		fmt.Printf("警告: 加载 catalog 失败: %v\n", err)
	}

	// 1. Check Database ID
	var storedDBID string
	err := s.View(func(txn store.Tx) error {
		val, err := txn.Get([]byte("_sys/database_id"))
		if err == nil {
			storedDBID = string(val)
			return nil
		}
		return err
	})
	if err != nil && err != store.ErrKeyNotFound {
		panic(fmt.Sprintf("读取 Database ID 失败: %v", err))
	}

	if storedDBID != "" {
		if storedDBID != databaseID {
			panic("Database ID mismatch: expected " + storedDBID + ", got " + databaseID)
		}
	} else {
		// First time, store it
		if err := s.Update(func(txn store.Tx) error {
			return txn.Set([]byte("_sys/database_id"), []byte(databaseID), 0)
		}); err != nil {
			panic(fmt.Sprintf("存储 Database ID 失败: %v", err))
		}
	}

	// 2. Load or generate NodeID
	var nodeID string
	err = s.View(func(txn store.Tx) error {
		val, err := txn.Get([]byte("_sys/node_id"))
		if err == nil {
			nodeID = string(val)
			return nil
		}
		return err
	})
	if err != nil && err != store.ErrKeyNotFound {
		panic(fmt.Sprintf("读取 Node ID 失败: %v", err))
	}

	if nodeID == "" || err == store.ErrKeyNotFound {
		nodeID = "node-" + uuid.NewString()
		if err := s.Update(func(txn store.Tx) error {
			return txn.Set([]byte("_sys/node_id"), []byte(nodeID), 0)
		}); err != nil {
			panic(fmt.Sprintf("存储 Node ID 失败: %v", err))
		}
	}

	db := &DB{
		store:      s,
		catalog:    c,
		idxMgr:     index.NewManager(),
		clock:      hlc.New(),
		tables:     make(map[string]*Table),
		NodeID:     nodeID,
		DatabaseID: databaseID,
	}

	for _, opt := range opts {
		opt(db)
	}

	return db
}

// SetFileStorageDir 设置文件存储目录。
func (db *DB) SetFileStorageDir(dir string) {
	db.FileStorageDir = dir
}

func (db *DB) Close() error {
	// 停止同步引擎
	if db.syncEngine != nil {
		db.syncEngine.Stop()
		db.syncEngine = nil
	}
	return db.store.Close()
}

// OnChange 注册数据变更回调。
// 当 Table.Set() 或 Table.Add() 成功写入后触发。
// MergeRawRow() 不会触发（避免远程数据合并时循环广播）。
func (db *DB) OnChange(fn ChangeCallback) {
	db.onChangeMu.Lock()
	defer db.onChangeMu.Unlock()
	db.onChangeCallbacks = append(db.onChangeCallbacks, fn)
}

func (db *DB) OnChangeDetailed(fn ChangeEventCallback) {
	db.onChangeMu.Lock()
	defer db.onChangeMu.Unlock()
	db.onChangeEventCallbacks = append(db.onChangeEventCallbacks, fn)
}

// notifyChange 触发所有变更回调（内部使用）。
func (db *DB) notifyChange(tableName string, key uuid.UUID) {
	db.notifyChangeWithColumns(tableName, key, nil)
}

func (db *DB) notifyChangeWithColumns(tableName string, key uuid.UUID, columns []string) {
	db.onChangeMu.RLock()
	callbacks := append([]ChangeCallback(nil), db.onChangeCallbacks...)
	eventCallbacks := append([]ChangeEventCallback(nil), db.onChangeEventCallbacks...)
	db.onChangeMu.RUnlock()

	for _, fn := range callbacks {
		fn(tableName, key)
	}

	event := ChangeEvent{
		TableName: tableName,
		Key:       key,
		Columns:   append([]string(nil), columns...),
	}
	for _, fn := range eventCallbacks {
		fn(event)
	}
}

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

// GCResult 包含 GC 操作的结果统计。
type GCResult struct {
	TablesScanned     int     // 扫描的表数量
	RowsScanned       int     // 扫描的行数量
	TombstonesRemoved int     // 移除的墓碑数量
	Errors            []error // 遇到的错误
}

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

// GCByTimeOffset 根据时间偏移量执行 GC。
// offset 是从当前时间向后的偏移量，例如 60 * time.Second 表示清理 60 秒前的数据。
func (db *DB) GCByTimeOffset(offset time.Duration) *GCResult {
	safeTimestamp := db.clock.Now() - offset.Milliseconds()
	return db.GC(safeTimestamp)
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

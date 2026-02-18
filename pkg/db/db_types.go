package db

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/hlc"
	"github.com/shinyes/yep_crdt/pkg/index"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

const (
	sysKeyDatabaseID = "_sys/database_id"
	sysKeyNodeID     = "_sys/node_id"
	sysKeyGCFloor    = "_sys/gc_floor"
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

// Tx 代表数据库事务上下文。
type Tx struct {
	db            *DB
	txn           store.Tx
	afterCommit   []func()
	afterRollback []func()
}

// GCResult 包含 GC 操作的结果统计。
type GCResult struct {
	TablesScanned     int     // 扫描的表数量
	RowsScanned       int     // 扫描的行数量
	TombstonesRemoved int     // 移除的墓碑数量
	Errors            []error // 遇到的错误
}

package sync

import (
	"sync"

	"github.com/shinyes/yep_crdt/pkg/db"
)

// NodeFromDataRootOptions configures local filesystem based multi-tenant startup.
type NodeFromDataRootOptions struct {
	DataRoot   string
	ListenPort int
	ConnectTo  string
	Password   string
	Debug      bool
	// IncrementalOnly disables automatic full-sync triggers on rejoin.
	IncrementalOnly bool
	Reset           bool
	IdentityPath    string
	// BadgerValueLogFileSize sets max bytes per Badger vlog file. 0 means store default (128MB).
	BadgerValueLogFileSize int64
	EnsureSchema           func(*db.DB) error
}

// LocalNode represents a started local multi-tenant node.
type LocalNode struct {
	mu        sync.RWMutex
	opWG      sync.WaitGroup
	engine    *MultiEngine
	databases map[string]*db.DB
	tenantIDs []string
	dataRoot  string
	// startup defaults used by helper APIs like RestoreTenant.
	badgerValueLogFileSize int64
	ensureSchema           func(*db.DB) error
	closed                 bool
}

// TenantRestoreOptions configures RestoreTenant behavior.
type TenantRestoreOptions struct {
	TenantID               string
	BackupPath             string
	ReplaceExisting        bool
	MaxPendingWrites       int
	BadgerValueLogFileSize int64 // 0 means use LocalNode startup default.
	EnsureSchema           func(*db.DB) error
}

type tenantDiscovery struct {
	tenantIDs   []string
	tenantPaths map[string]string
}

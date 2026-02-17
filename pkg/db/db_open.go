package db

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/hlc"
	"github.com/shinyes/yep_crdt/pkg/index"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

var ErrDatabaseIDMismatch = errors.New("database id mismatch")

// Open 打开数据库。
// databaseID 是数据库的唯一标识 (如 "tenant-1")。
func Open(s store.Store, databaseID string, opts ...Option) (*DB, error) {
	c := meta.NewCatalog(s)
	if err := c.Load(); err != nil {
		return nil, fmt.Errorf("加载 catalog 失败: %w", err)
	}

	// 1. Check Database ID
	var storedDBID string
	err := s.View(func(txn store.Tx) error {
		val, err := txn.Get([]byte(sysKeyDatabaseID))
		if err == nil {
			storedDBID = string(val)
			return nil
		}
		return err
	})
	if err != nil && err != store.ErrKeyNotFound {
		return nil, fmt.Errorf("读取 Database ID 失败: %w", err)
	}

	if storedDBID != "" {
		if storedDBID != databaseID {
			return nil, fmt.Errorf("%w: expected %s, got %s", ErrDatabaseIDMismatch, storedDBID, databaseID)
		}
	} else {
		// First time, store it
		if err := s.Update(func(txn store.Tx) error {
			return txn.Set([]byte(sysKeyDatabaseID), []byte(databaseID), 0)
		}); err != nil {
			return nil, fmt.Errorf("存储 Database ID 失败: %w", err)
		}
	}

	// 2. Load or generate NodeID
	var nodeID string
	err = s.View(func(txn store.Tx) error {
		val, err := txn.Get([]byte(sysKeyNodeID))
		if err == nil {
			nodeID = string(val)
			return nil
		}
		return err
	})
	if err != nil && err != store.ErrKeyNotFound {
		return nil, fmt.Errorf("读取 Node ID 失败: %w", err)
	}

	if nodeID == "" || err == store.ErrKeyNotFound {
		nodeID = "node-" + uuid.NewString()
		if err := s.Update(func(txn store.Tx) error {
			return txn.Set([]byte(sysKeyNodeID), []byte(nodeID), 0)
		}); err != nil {
			return nil, fmt.Errorf("存储 Node ID 失败: %w", err)
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

	return db, nil
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

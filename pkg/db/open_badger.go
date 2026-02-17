package db

import (
	"fmt"
	"os"
	"strings"

	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// BadgerOpenConfig defines a one-shot bootstrap for a Badger-backed DB.
type BadgerOpenConfig struct {
	Path                   string
	DatabaseID             string
	BadgerValueLogFileSize int64 // 0 means store default.
	BadgerOptions          []store.BadgerOption
	DBOptions              []Option
	Schemas                []*meta.TableSchema
	EnsureSchema           func(*DB) error
}

// OpenBadgerWithConfig creates (if needed) a Badger directory, opens the database,
// applies schemas, and runs optional custom setup.
// Any setup failure closes opened resources.
func OpenBadgerWithConfig(cfg BadgerOpenConfig) (*DB, error) {
	path := strings.TrimSpace(cfg.Path)
	if path == "" {
		return nil, fmt.Errorf("badger path cannot be empty")
	}
	databaseID := strings.TrimSpace(cfg.DatabaseID)
	if databaseID == "" {
		return nil, fmt.Errorf("database id cannot be empty")
	}
	if cfg.BadgerValueLogFileSize < 0 {
		return nil, fmt.Errorf("badger value log file size must be >= 0, got %d", cfg.BadgerValueLogFileSize)
	}

	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}

	badgerOptions := make([]store.BadgerOption, 0, len(cfg.BadgerOptions)+1)
	badgerOptions = append(badgerOptions, cfg.BadgerOptions...)
	if cfg.BadgerValueLogFileSize > 0 {
		badgerOptions = append(badgerOptions, store.WithBadgerValueLogFileSize(cfg.BadgerValueLogFileSize))
	}

	dbOptions := append([]Option(nil), cfg.DBOptions...)

	kv, err := store.NewBadgerStore(path, badgerOptions...)
	if err != nil {
		return nil, err
	}

	database, err := openDatabaseNoPanic(kv, databaseID, dbOptions...)
	if err != nil {
		return nil, err
	}

	for _, schema := range cfg.Schemas {
		if schema == nil {
			continue
		}
		if err := meta.ValidateTableSchemaShape(schema); err != nil {
			_ = database.Close()
			return nil, fmt.Errorf("invalid schema for table %q: %w", schema.Name, err)
		}
		if existing, exists := database.catalog.GetTable(schema.Name); exists {
			if err := meta.ValidateTableSchemaShapeCompatibility(existing, schema); err != nil {
				_ = database.Close()
				return nil, fmt.Errorf("schema conflict for table %q: %w", schema.Name, err)
			}
			continue
		}
		if err := database.DefineTable(schema); err != nil {
			_ = database.Close()
			return nil, err
		}
	}

	if cfg.EnsureSchema != nil {
		if err := cfg.EnsureSchema(database); err != nil {
			_ = database.Close()
			return nil, err
		}
	}

	return database, nil
}

func openDatabaseNoPanic(s store.Store, databaseID string, opts ...Option) (database *DB, err error) {
	defer func() {
		if r := recover(); r != nil {
			_ = s.Close()
			err = fmt.Errorf("open database failed: %v", r)
		}
	}()
	database = Open(s, databaseID, opts...)
	return database, nil
}

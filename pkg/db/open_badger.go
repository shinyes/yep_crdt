package db

import (
	"fmt"
	"os"
	"slices"
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
		if err := validateTableSchemaShape(schema); err != nil {
			_ = database.Close()
			return nil, fmt.Errorf("invalid schema for table %q: %w", schema.Name, err)
		}
		if existing, exists := database.catalog.GetTable(schema.Name); exists {
			if err := validateTableSchemaShapeCompatibility(existing, schema); err != nil {
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

func validateTableSchemaShapeCompatibility(existing *meta.TableSchema, incoming *meta.TableSchema) error {
	existingColumns, err := normalizeColumns(existing.Columns)
	if err != nil {
		return fmt.Errorf("existing columns invalid: %w", err)
	}
	incomingColumns, err := normalizeColumns(incoming.Columns)
	if err != nil {
		return fmt.Errorf("incoming columns invalid: %w", err)
	}
	if len(existingColumns) != len(incomingColumns) {
		return fmt.Errorf("column count mismatch: existing=%d incoming=%d", len(existingColumns), len(incomingColumns))
	}
	for name, existingColumn := range existingColumns {
		incomingColumn, ok := incomingColumns[name]
		if !ok {
			return fmt.Errorf("missing column %q", name)
		}
		if existingColumn.Type != incomingColumn.Type || existingColumn.CrdtType != incomingColumn.CrdtType {
			return fmt.Errorf(
				"column %q mismatch: existing=(type=%s crdt=%s) incoming=(type=%s crdt=%s)",
				name,
				existingColumn.Type,
				existingColumn.CrdtType,
				incomingColumn.Type,
				incomingColumn.CrdtType,
			)
		}
	}

	existingIndexes, err := normalizeIndexes(existing.Indexes)
	if err != nil {
		return fmt.Errorf("existing indexes invalid: %w", err)
	}
	incomingIndexes, err := normalizeIndexes(incoming.Indexes)
	if err != nil {
		return fmt.Errorf("incoming indexes invalid: %w", err)
	}
	if len(existingIndexes) != len(incomingIndexes) {
		return fmt.Errorf("index count mismatch: existing=%d incoming=%d", len(existingIndexes), len(incomingIndexes))
	}
	for name, existingIndex := range existingIndexes {
		incomingIndex, ok := incomingIndexes[name]
		if !ok {
			return fmt.Errorf("missing index %q", name)
		}
		if existingIndex.Unique != incomingIndex.Unique {
			return fmt.Errorf("index %q unique mismatch: existing=%t incoming=%t", name, existingIndex.Unique, incomingIndex.Unique)
		}
		if !slices.Equal(existingIndex.Columns, incomingIndex.Columns) {
			return fmt.Errorf("index %q columns mismatch: existing=%v incoming=%v", name, existingIndex.Columns, incomingIndex.Columns)
		}
	}

	return nil
}

func validateTableSchemaShape(schema *meta.TableSchema) error {
	if schema == nil {
		return fmt.Errorf("schema cannot be nil")
	}
	if strings.TrimSpace(schema.Name) == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if _, err := normalizeColumns(schema.Columns); err != nil {
		return err
	}
	if _, err := normalizeIndexes(schema.Indexes); err != nil {
		return err
	}
	return nil
}

func normalizeColumns(columns []meta.ColumnSchema) (map[string]meta.ColumnSchema, error) {
	normalized := make(map[string]meta.ColumnSchema, len(columns))
	for _, column := range columns {
		name := strings.TrimSpace(column.Name)
		if name == "" {
			return nil, fmt.Errorf("column name cannot be empty")
		}
		if _, exists := normalized[name]; exists {
			return nil, fmt.Errorf("duplicate column name %q", name)
		}
		normalized[name] = meta.ColumnSchema{
			Name:     name,
			Type:     column.Type,
			CrdtType: column.CrdtType,
		}
	}
	return normalized, nil
}

func normalizeIndexes(indexes []meta.IndexSchema) (map[string]meta.IndexSchema, error) {
	normalized := make(map[string]meta.IndexSchema, len(indexes))
	for _, indexSchema := range indexes {
		name := strings.TrimSpace(indexSchema.Name)
		if name == "" {
			return nil, fmt.Errorf("index name cannot be empty")
		}
		if _, exists := normalized[name]; exists {
			return nil, fmt.Errorf("duplicate index name %q", name)
		}
		columns := make([]string, len(indexSchema.Columns))
		for i, column := range indexSchema.Columns {
			column = strings.TrimSpace(column)
			if column == "" {
				return nil, fmt.Errorf("index %q has empty column at position %d", name, i)
			}
			columns[i] = column
		}
		normalized[name] = meta.IndexSchema{
			Name:    name,
			Columns: columns,
			Unique:  indexSchema.Unique,
		}
	}
	return normalized, nil
}

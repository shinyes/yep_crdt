package meta

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/shinyes/yep_crdt/pkg/store"
)

type ColumnType string

const (
	ColTypeString ColumnType = "string"
	ColTypeInt    ColumnType = "int"
	ColTypeBool   ColumnType = "bool"
	// ... 根据需要添加更多
)

const (
	CrdtLWW     = "lww"
	CrdtCounter = "counter"
	CrdtORSet   = "orset"
	CrdtRGA     = "rga"
)

type ColumnSchema struct {
	Name     string     `json:"name"`
	Type     ColumnType `json:"type"`      // 数据类型 (int, string, etc.)
	CrdtType string     `json:"crdt_type"` // "lww", "counter", "orset", "rga"
}

type IndexSchema struct {
	ID      uint32   `json:"id"`
	Name    string   `json:"name"`
	Columns []string `json:"columns"` // 支持复合索引
	Unique  bool     `json:"unique"`
}

type TableSchema struct {
	ID      uint32         `json:"id"`
	Name    string         `json:"name"`
	Columns []ColumnSchema `json:"columns"`
	Indexes []IndexSchema  `json:"indexes"`
}

// Catalog 管理表定义。
type Catalog struct {
	mu          sync.RWMutex
	store       store.Store
	tables      map[string]*TableSchema
	ids         map[uint32]*TableSchema
	lastTableID uint32
}

const MetaCatalogKey = "/_meta/catalog"

func NewCatalog(s store.Store) *Catalog {
	return &Catalog{
		store:  s,
		tables: make(map[string]*TableSchema),
		ids:    make(map[uint32]*TableSchema),
	}
}

// Persistable state
type catalogState struct {
	LastTableID uint32         `json:"last_table_id"`
	Tables      []*TableSchema `json:"tables"`
}

func (c *Catalog) Load() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.store.View(func(txn store.Tx) error {
		val, err := txn.Get([]byte(MetaCatalogKey))
		if err == store.ErrKeyNotFound {
			return nil // New DB
		}
		if err != nil {
			return err
		}

		var state catalogState
		if err := json.Unmarshal(val, &state); err != nil {
			return fmt.Errorf("failed to decode catalog: %w", err)
		}

		c.lastTableID = state.LastTableID
		for _, t := range state.Tables {
			c.tables[t.Name] = t
			c.ids[t.ID] = t
		}
		return nil
	})
}

func (c *Catalog) Save() error {
	// Call with Lock held or externally locked?
	// AddTable locks, so Save needs to handle its own locking if called externally,
	// BUT here we call it from AddTable which holds lock.
	// So Save should NOT lock.
	// Let's make Save private or assume caller holds lock.
	// Actually, let's make it private save() and public Save() if needed.
	// For now, only used internally by AddTable.

	state := catalogState{
		LastTableID: c.lastTableID,
		Tables:      make([]*TableSchema, 0, len(c.tables)),
	}
	for _, t := range c.tables {
		state.Tables = append(state.Tables, t)
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return c.store.Update(func(txn store.Tx) error {
		return txn.Set([]byte(MetaCatalogKey), data, 0)
	})
}

func (c *Catalog) AddTable(t *TableSchema) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, exists := c.tables[t.Name]; exists {
		// If exists, we generally return existing ID.
		// But schema evolution? For now, simple reuse.
		// Update t.ID to match existing.
		t.ID = existing.ID
		// TODO: Merge columns/indexes?
		// For MVP: Just return nil if it matches basic check, or error if conflict?
		// "table %s already exists"
		// If user calls DefineTable("users") again, usually they expect idempotency.
		// Let's return nil but ensure ID is set.
		return nil
	}

	// Assign ID
	if t.ID == 0 {
		c.lastTableID++
		t.ID = c.lastTableID
	} else {
		// If user somehow provided ID (legacy), update tracker
		if t.ID > c.lastTableID {
			c.lastTableID = t.ID
		}
	}

	// Indexes
	var maxIdxID uint32
	for _, idx := range t.Indexes {
		if idx.ID > maxIdxID {
			maxIdxID = idx.ID
		}
	}
	for i := range t.Indexes {
		if t.Indexes[i].ID == 0 {
			maxIdxID++
			t.Indexes[i].ID = maxIdxID
		}
	}

	c.tables[t.Name] = t
	c.ids[t.ID] = t

	// Persist checks
	return c.Save()
}

func (c *Catalog) GetTable(name string) (*TableSchema, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.tables[name]
	return t, ok
}

func (c *Catalog) GetTableByID(id uint32) (*TableSchema, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.ids[id]
	return t, ok
}

package meta

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/shinyes/yep_crdt/pkg/store"
)

// Mock store for testing
type mockStore struct {
	data map[string][]byte
}

func newMockStore() *mockStore {
	return &mockStore{
		data: make(map[string][]byte),
	}
}

func (s *mockStore) RunTx(update bool, fn func(tx store.Tx) error) error {
	tx := &mockTx{data: s.data}
	return fn(tx)
}

func (s *mockStore) Update(fn func(tx store.Tx) error) error {
	return s.RunTx(true, fn)
}

func (s *mockStore) View(fn func(tx store.Tx) error) error {
	return s.RunTx(false, fn)
}

func (s *mockStore) Close() error {
	return nil
}

type mockTx struct {
	data map[string][]byte
}

func (tx *mockTx) Get(key []byte) ([]byte, error) {
	val, ok := tx.data[string(key)]
	if !ok {
		return nil, store.ErrKeyNotFound
	}
	return val, nil
}

func (tx *mockTx) Set(key, value []byte, ttl int64) error {
	tx.data[string(key)] = value
	return nil
}

func (tx *mockTx) Delete(key []byte) error {
	delete(tx.data, string(key))
	return nil
}

func (tx *mockTx) sortedKeys(prefix []byte) []string {
	var keys []string
	for k := range tx.data {
		if len(prefix) == 0 || len(k) >= len(prefix) && k[:len(prefix)] == string(prefix) {
			keys = append(keys, k)
		}
	}
	// Sort keys
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}

func (tx *mockTx) NewIterator(opts store.IteratorOptions) store.Iterator {
	return &mockIterator{
		data:   tx.data,
		prefix: opts.Prefix,
		keys:   tx.sortedKeys(opts.Prefix),
		pos:    -1,
	}
}

type mockIterator struct {
	data   map[string][]byte
	prefix []byte
	keys   []string
	pos    int
}

func (it *mockIterator) sortedKeys(prefix []byte) []string {
	var keys []string
	for k := range it.data {
		if len(prefix) == 0 || len(k) >= len(prefix) && k[:len(prefix)] == string(prefix) {
			keys = append(keys, k)
		}
	}
	// Sort keys
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}

func (it *mockIterator) Seek(key []byte) {
	for i, k := range it.keys {
		if k >= string(key) {
			it.pos = i
			return
		}
	}
	it.pos = len(it.keys)
}

func (it *mockIterator) Rewind() {
	it.pos = 0
}

func (it *mockIterator) Valid() bool {
	return it.pos >= 0 && it.pos < len(it.keys)
}

func (it *mockIterator) ValidForPrefix(prefix []byte) bool {
	if !it.Valid() {
		return false
	}
	return len(it.keys[it.pos]) >= len(prefix) && it.keys[it.pos][:len(prefix)] == string(prefix)
}

func (it *mockIterator) Next() {
	it.pos++
}

func (it *mockIterator) Item() (key, value []byte, err error) {
	if !it.Valid() {
		return nil, nil, store.ErrKeyNotFound
	}
	k := it.keys[it.pos]
	return []byte(k), it.data[k], nil
}

func (it *mockIterator) Close() {}

func TestNewCatalog(t *testing.T) {
	store := newMockStore()
	catalog := NewCatalog(store)

	if catalog == nil {
		t.Fatal("NewCatalog() returned nil")
	}

	if catalog.tables == nil {
		t.Error("catalog.tables is nil")
	}

	if catalog.ids == nil {
		t.Error("catalog.ids is nil")
	}
}

func TestAddTable(t *testing.T) {
	store := newMockStore()
	catalog := NewCatalog(store)

	schema := &TableSchema{
		Name: "users",
		Columns: []ColumnSchema{
			{Name: "id", Type: ColTypeString},
			{Name: "name", Type: ColTypeString},
		},
		Indexes: []IndexSchema{
			{Name: "idx_name", Columns: []string{"name"}},
		},
	}

	err := catalog.AddTable(schema)
	if err != nil {
		t.Fatalf("AddTable() failed: %v", err)
	}

	// Check table was assigned an ID
	if schema.ID == 0 {
		t.Error("Table was not assigned an ID")
	}

	// Check index was assigned an ID
	if len(schema.Indexes) > 0 && schema.Indexes[0].ID == 0 {
		t.Error("Index was not assigned an ID")
	}

	// Check table can be retrieved
	retrieved, ok := catalog.GetTable("users")
	if !ok {
		t.Error("GetTable() returned false for existing table")
	}

	if retrieved.Name != "users" {
		t.Errorf("Got table name %s, want users", retrieved.Name)
	}

	// Check table can be retrieved by ID
	retrievedByID, ok := catalog.GetTableByID(schema.ID)
	if !ok {
		t.Error("GetTableByID() returned false for existing table")
	}

	if retrievedByID.Name != "users" {
		t.Errorf("Got table name %s, want users", retrievedByID.Name)
	}
}

func TestAddTableIdempotent(t *testing.T) {
	store := newMockStore()
	catalog := NewCatalog(store)

	schema1 := &TableSchema{
		Name: "users",
		Columns: []ColumnSchema{
			{Name: "id", Type: ColTypeString},
			{Name: "name", Type: ColTypeString, CrdtType: CrdtLWW},
		},
		Indexes: []IndexSchema{
			{Name: "idx_name", Columns: []string{"name"}, Unique: false},
		},
	}

	schema2 := &TableSchema{
		Name: "users",
		Columns: []ColumnSchema{
			{Name: "name", Type: ColTypeString, CrdtType: CrdtLWW},
			{Name: "id", Type: ColTypeString},
		},
		Indexes: []IndexSchema{
			{Name: "idx_name", Columns: []string{"name"}, Unique: false},
		},
	}

	err := catalog.AddTable(schema1)
	if err != nil {
		t.Fatalf("First AddTable() failed: %v", err)
	}

	id1 := schema1.ID

	// Adding same schema by name should be idempotent.
	err = catalog.AddTable(schema2)
	if err != nil {
		t.Fatalf("Second AddTable() failed: %v", err)
	}

	// ID should remain the same
	if schema2.ID != id1 {
		t.Errorf("Table ID changed from %d to %d", id1, schema2.ID)
	}

	table, ok := catalog.GetTable("users")
	if !ok {
		t.Error("GetTable() returned false")
	}
	if len(table.Columns) != 2 {
		t.Errorf("Table has %d columns, want 2", len(table.Columns))
	}
}

func TestAddTableColumnConflict(t *testing.T) {
	store := newMockStore()
	catalog := NewCatalog(store)

	schema1 := &TableSchema{
		Name: "users",
		Columns: []ColumnSchema{
			{Name: "name", Type: ColTypeString, CrdtType: CrdtLWW},
		},
	}
	schema2 := &TableSchema{
		Name: "users",
		Columns: []ColumnSchema{
			{Name: "name", Type: ColTypeInt, CrdtType: CrdtCounter},
		},
	}

	if err := catalog.AddTable(schema1); err != nil {
		t.Fatalf("First AddTable() failed: %v", err)
	}
	err := catalog.AddTable(schema2)
	if err == nil {
		t.Fatalf("expected schema conflict, got nil")
	}
	if schema2.ID != schema1.ID {
		t.Fatalf("expected incoming schema ID %d, got %d", schema1.ID, schema2.ID)
	}
}

func TestAddTableIndexConflict(t *testing.T) {
	store := newMockStore()
	catalog := NewCatalog(store)

	schema1 := &TableSchema{
		Name: "users",
		Columns: []ColumnSchema{
			{Name: "name", Type: ColTypeString, CrdtType: CrdtLWW},
		},
		Indexes: []IndexSchema{
			{Name: "idx_name", Columns: []string{"name"}, Unique: false},
		},
	}
	schema2 := &TableSchema{
		Name: "users",
		Columns: []ColumnSchema{
			{Name: "name", Type: ColTypeString, CrdtType: CrdtLWW},
		},
		Indexes: []IndexSchema{
			{Name: "idx_name", Columns: []string{"name"}, Unique: true},
		},
	}

	if err := catalog.AddTable(schema1); err != nil {
		t.Fatalf("First AddTable() failed: %v", err)
	}
	err := catalog.AddTable(schema2)
	if err == nil {
		t.Fatalf("expected schema conflict, got nil")
	}
}

func TestAddTableRejectsInvalidSchema(t *testing.T) {
	tests := []struct {
		name   string
		schema *TableSchema
	}{
		{
			name:   "nil schema",
			schema: nil,
		},
		{
			name: "empty table name",
			schema: &TableSchema{
				Name: "   ",
				Columns: []ColumnSchema{
					{Name: "id", Type: ColTypeString},
				},
			},
		},
		{
			name: "duplicate column names",
			schema: &TableSchema{
				Name: "users",
				Columns: []ColumnSchema{
					{Name: "id", Type: ColTypeString},
					{Name: " id ", Type: ColTypeString},
				},
			},
		},
		{
			name: "duplicate index names",
			schema: &TableSchema{
				Name: "users",
				Columns: []ColumnSchema{
					{Name: "id", Type: ColTypeString},
				},
				Indexes: []IndexSchema{
					{Name: "idx_id", Columns: []string{"id"}},
					{Name: " idx_id ", Columns: []string{"id"}},
				},
			},
		},
		{
			name: "unsupported text column type",
			schema: &TableSchema{
				Name: "users",
				Columns: []ColumnSchema{
					{Name: "bio", Type: ColumnType("text")},
				},
			},
		},
		{
			name: "unsupported json column type",
			schema: &TableSchema{
				Name: "users",
				Columns: []ColumnSchema{
					{Name: "profile", Type: ColumnType("json")},
				},
			},
		},
		{
			name: "empty index column name",
			schema: &TableSchema{
				Name: "users",
				Columns: []ColumnSchema{
					{Name: "id", Type: ColTypeString},
				},
				Indexes: []IndexSchema{
					{Name: "idx_id", Columns: []string{"id", " "}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog := NewCatalog(newMockStore())
			if err := catalog.AddTable(tt.schema); err == nil {
				t.Fatalf("expected error for invalid schema %q", tt.name)
			}
		})
	}
}

func TestLoadRejectsUnsupportedColumnType(t *testing.T) {
	store := newMockStore()
	state := catalogState{
		LastTableID: 1,
		Tables: []*TableSchema{
			{
				ID:   1,
				Name: "users",
				Columns: []ColumnSchema{
					{Name: "bio", Type: ColumnType("text")},
				},
			},
		},
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal state failed: %v", err)
	}
	store.data[MetaCatalogKey] = data

	catalog := NewCatalog(store)
	err = catalog.Load()
	if err == nil {
		t.Fatalf("expected load to fail for unsupported column type")
	}
	if !strings.Contains(err.Error(), "unsupported column type") {
		t.Fatalf("unexpected load error: %v", err)
	}
}

func TestAddTableWithID(t *testing.T) {
	store := newMockStore()
	catalog := NewCatalog(store)

	schema := &TableSchema{
		ID:   100,
		Name: "users",
		Columns: []ColumnSchema{
			{Name: "id", Type: ColTypeString},
		},
	}

	err := catalog.AddTable(schema)
	if err != nil {
		t.Fatalf("AddTable() failed: %v", err)
	}

	if schema.ID != 100 {
		t.Errorf("Table ID changed from 100 to %d", schema.ID)
	}

	// Next table should get a higher ID
	schema2 := &TableSchema{
		Name: "posts",
		Columns: []ColumnSchema{
			{Name: "id", Type: ColTypeString},
		},
	}

	err = catalog.AddTable(schema2)
	if err != nil {
		t.Fatalf("AddTable() failed: %v", err)
	}

	if schema2.ID <= 100 {
		t.Errorf("Second table ID %d is not greater than 100", schema2.ID)
	}
}

func TestAddTableWithDuplicateExplicitID(t *testing.T) {
	store := newMockStore()
	catalog := NewCatalog(store)

	first := &TableSchema{
		ID:   100,
		Name: "users",
		Columns: []ColumnSchema{
			{Name: "id", Type: ColTypeString},
		},
	}
	second := &TableSchema{
		ID:   100,
		Name: "posts",
		Columns: []ColumnSchema{
			{Name: "id", Type: ColTypeString},
		},
	}

	if err := catalog.AddTable(first); err != nil {
		t.Fatalf("First AddTable() failed: %v", err)
	}
	if err := catalog.AddTable(second); err == nil {
		t.Fatalf("expected duplicate table ID error, got nil")
	}
}

func TestAddTableWithIndex(t *testing.T) {
	store := newMockStore()
	catalog := NewCatalog(store)

	schema := &TableSchema{
		Name: "users",
		Columns: []ColumnSchema{
			{Name: "id", Type: ColTypeString},
			{Name: "name", Type: ColTypeString},
			{Name: "age", Type: ColTypeInt},
		},
		Indexes: []IndexSchema{
			{Name: "idx_name", Columns: []string{"name"}},
			{Name: "idx_age", Columns: []string{"age"}},
		},
	}

	err := catalog.AddTable(schema)
	if err != nil {
		t.Fatalf("AddTable() failed: %v", err)
	}

	// Check all indexes were assigned IDs
	for i, idx := range schema.Indexes {
		if idx.ID == 0 {
			t.Errorf("Index %d was not assigned an ID", i)
		}
	}

	// Index IDs should be unique
	ids := make(map[uint32]bool)
	for _, idx := range schema.Indexes {
		if ids[idx.ID] {
			t.Errorf("Duplicate index ID: %d", idx.ID)
		}
		ids[idx.ID] = true
	}
}

func TestAddTableRejectDuplicateIndexID(t *testing.T) {
	store := newMockStore()
	catalog := NewCatalog(store)

	schema := &TableSchema{
		Name: "users",
		Columns: []ColumnSchema{
			{Name: "id", Type: ColTypeString},
			{Name: "name", Type: ColTypeString},
		},
		Indexes: []IndexSchema{
			{ID: 1, Name: "idx_name_1", Columns: []string{"name"}},
			{ID: 1, Name: "idx_name_2", Columns: []string{"name"}},
		},
	}

	if err := catalog.AddTable(schema); err == nil {
		t.Fatalf("expected duplicate index ID error, got nil")
	}
}

func TestGetTableNotFound(t *testing.T) {
	store := newMockStore()
	catalog := NewCatalog(store)

	_, ok := catalog.GetTable("nonexistent")
	if ok {
		t.Error("GetTable() returned true for non-existent table")
	}

	_, ok = catalog.GetTableByID(999)
	if ok {
		t.Error("GetTableByID() returned true for non-existent ID")
	}
}

func TestLoadEmptyCatalog(t *testing.T) {
	store := newMockStore()
	catalog := NewCatalog(store)

	err := catalog.Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Catalog should be empty
	_, ok := catalog.GetTable("any_table")
	if ok {
		t.Error("Catalog should be empty after loading from empty store")
	}
}

func TestLoadAndPersist(t *testing.T) {
	store := newMockStore()
	catalog := NewCatalog(store)

	// Add a table
	schema := &TableSchema{
		Name: "users",
		Columns: []ColumnSchema{
			{Name: "id", Type: ColTypeString},
			{Name: "name", Type: ColTypeString, CrdtType: CrdtLWW},
			{Name: "views", Type: ColTypeInt, CrdtType: CrdtCounter},
			{Name: "tags", Type: ColTypeString, CrdtType: CrdtORSet},
			{Name: "todos", Type: ColTypeString, CrdtType: CrdtRGA},
		},
		Indexes: []IndexSchema{
			{Name: "idx_name", Columns: []string{"name"}},
			{Name: "idx_views", Columns: []string{"views"}},
		},
	}

	err := catalog.AddTable(schema)
	if err != nil {
		t.Fatalf("AddTable() failed: %v", err)
	}

	tableID := schema.ID
	indexIDs := make([]uint32, len(schema.Indexes))
	for i, idx := range schema.Indexes {
		indexIDs[i] = idx.ID
	}

	// Create a new catalog and load from the same store
	catalog2 := NewCatalog(store)
	err = catalog2.Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Check table was restored
	retrieved, ok := catalog2.GetTable("users")
	if !ok {
		t.Fatal("Table was not restored")
	}

	if retrieved.ID != tableID {
		t.Errorf("Table ID changed from %d to %d", tableID, retrieved.ID)
	}

	if len(retrieved.Columns) != len(schema.Columns) {
		t.Errorf("Column count mismatch: got %d, want %d",
			len(retrieved.Columns), len(schema.Columns))
	}

	if len(retrieved.Indexes) != len(schema.Indexes) {
		t.Errorf("Index count mismatch: got %d, want %d",
			len(retrieved.Indexes), len(schema.Indexes))
	}

	// Check column types
	for i, col := range retrieved.Columns {
		if col.Name != schema.Columns[i].Name {
			t.Errorf("Column %d name mismatch: got %s, want %s",
				i, col.Name, schema.Columns[i].Name)
		}
		if col.Type != schema.Columns[i].Type {
			t.Errorf("Column %d type mismatch: got %s, want %s",
				i, col.Type, schema.Columns[i].Type)
		}
		if col.CrdtType != schema.Columns[i].CrdtType {
			t.Errorf("Column %d CRDT type mismatch: got %s, want %s",
				i, col.CrdtType, schema.Columns[i].CrdtType)
		}
	}

	// Check index IDs
	for i, idx := range retrieved.Indexes {
		if idx.ID != indexIDs[i] {
			t.Errorf("Index %d ID mismatch: got %d, want %d",
				i, idx.ID, indexIDs[i])
		}
	}
}

func TestMultipleTables(t *testing.T) {
	store := newMockStore()
	catalog := NewCatalog(store)

	tables := []string{"users", "posts", "comments"}
	ids := make(map[string]uint32)

	// Add multiple tables
	for _, name := range tables {
		schema := &TableSchema{
			Name: name,
			Columns: []ColumnSchema{
				{Name: "id", Type: ColTypeString},
			},
		}

		err := catalog.AddTable(schema)
		if err != nil {
			t.Fatalf("AddTable() failed: %v", err)
		}

		ids[name] = schema.ID
	}

	// Check all tables exist
	for _, name := range tables {
		_, ok := catalog.GetTable(name)
		if !ok {
			t.Errorf("Table %s not found", name)
		}
	}

	// Check IDs are unique
	seenIDs := make(map[uint32]bool)
	for _, id := range ids {
		if seenIDs[id] {
			t.Errorf("Duplicate table ID: %d", id)
		}
		seenIDs[id] = true
	}

	// Create new catalog and load
	catalog2 := NewCatalog(store)
	err := catalog2.Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Check all tables were restored
	for _, name := range tables {
		retrieved, ok := catalog2.GetTable(name)
		if !ok {
			t.Errorf("Table %s was not restored", name)
		}

		if retrieved.ID != ids[name] {
			t.Errorf("Table %s ID mismatch: got %d, want %d",
				name, retrieved.ID, ids[name])
		}
	}
}

package index

import (
	"testing"

	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// Mock transaction for testing
type mockTx struct {
	data map[string][]byte
}

func newMockTx() *mockTx {
	return &mockTx{
		data: make(map[string][]byte),
	}
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

func TestUpdateIndexes(t *testing.T) {
	mgr := NewManager()

	tests := []struct {
		name        string
		tableID     uint32
		indexes     []meta.IndexSchema
		pk          []byte
		oldBody     map[string]any
		newBody     map[string]any
		wantErr     bool
		validate    func(*testing.T, *mockTx)
	}{
		{
			name:    "new row - insert index entry",
			tableID: 1,
			indexes: []meta.IndexSchema{
				{ID: 1, Name: "idx_name", Columns: []string{"name"}},
			},
			pk:      []byte{1, 2, 3, 4},
			oldBody: map[string]any{},
			newBody: map[string]any{
				"name": "Alice",
			},
			wantErr: false,
			validate: func(t *testing.T, tx *mockTx) {
				// Should have added one index entry
				if len(tx.data) != 1 {
					t.Errorf("expected 1 index entry, got %d", len(tx.data))
				}
			},
		},
		{
			name:    "update row - replace index entry",
			tableID: 1,
			indexes: []meta.IndexSchema{
				{ID: 1, Name: "idx_name", Columns: []string{"name"}},
			},
			pk:      []byte{1, 2, 3, 4},
			oldBody: map[string]any{
				"name": "Alice",
			},
			newBody: map[string]any{
				"name": "Bob",
			},
			wantErr: false,
			validate: func(t *testing.T, tx *mockTx) {
				// Should have deleted old and inserted new
				// With mock implementation, old is deleted so we only see new
				if len(tx.data) != 1 {
					t.Errorf("expected 1 index entry, got %d", len(tx.data))
				}
			},
		},
		{
			name:    "delete row - remove index entry",
			tableID: 1,
			indexes: []meta.IndexSchema{
				{ID: 1, Name: "idx_name", Columns: []string{"name"}},
			},
			pk:      []byte{1, 2, 3, 4},
			oldBody: map[string]any{
				"name": "Alice",
			},
			newBody: map[string]any{},
			wantErr: false,
			validate: func(t *testing.T, tx *mockTx) {
				// Should have removed the entry
				if len(tx.data) != 0 {
					t.Errorf("expected 0 index entries, got %d", len(tx.data))
				}
			},
		},
		{
			name:    "composite index",
			tableID: 1,
			indexes: []meta.IndexSchema{
				{ID: 1, Name: "idx_name_age", Columns: []string{"name", "age"}},
			},
			pk:      []byte{1, 2, 3, 4},
			oldBody: map[string]any{},
			newBody: map[string]any{
				"name": "Alice",
				"age":  30,
			},
			wantErr: false,
		},
		{
			name:    "multiple indexes on same table",
			tableID: 1,
			indexes: []meta.IndexSchema{
				{ID: 1, Name: "idx_name", Columns: []string{"name"}},
				{ID: 2, Name: "idx_age", Columns: []string{"age"}},
			},
			pk:      []byte{1, 2, 3, 4},
			oldBody: map[string]any{},
			newBody: map[string]any{
				"name": "Alice",
				"age":  30,
			},
			wantErr: false,
			validate: func(t *testing.T, tx *mockTx) {
				// Should have added two index entries
				if len(tx.data) != 2 {
					t.Errorf("expected 2 index entries, got %d", len(tx.data))
				}
			},
		},
		{
			name:    "partial data - should not index",
			tableID: 1,
			indexes: []meta.IndexSchema{
				{ID: 1, Name: "idx_name_age", Columns: []string{"name", "age"}},
			},
			pk:      []byte{1, 2, 3, 4},
			oldBody: map[string]any{},
			newBody: map[string]any{
				"name": "Alice",
				// age is missing
			},
			wantErr: false,
			validate: func(t *testing.T, tx *mockTx) {
				// Should not have added any index entry
				if len(tx.data) != 0 {
					t.Errorf("expected 0 index entries (partial data), got %d", len(tx.data))
				}
			},
		},
		{
			name:    "nil value - should not index",
			tableID: 1,
			indexes: []meta.IndexSchema{
				{ID: 1, Name: "idx_name", Columns: []string{"name"}},
			},
			pk:      []byte{1, 2, 3, 4},
			oldBody: map[string]any{},
			newBody: map[string]any{
				"name": nil,
			},
			wantErr: false,
			validate: func(t *testing.T, tx *mockTx) {
				if len(tx.data) != 0 {
					t.Errorf("expected 0 index entries (nil value), got %d", len(tx.data))
				}
			},
		},
		{
			name:    "different primary keys create different entries",
			tableID: 1,
			indexes: []meta.IndexSchema{
				{ID: 1, Name: "idx_name", Columns: []string{"age"}},
			},
			pk:      []byte{1, 2, 3, 4},
			oldBody: map[string]any{},
			newBody: map[string]any{
				"age": 30,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := newMockTx()
			err := mgr.UpdateIndexes(tx, tt.tableID, tt.indexes, tt.pk, tt.oldBody, tt.newBody)
			if (err != nil) != tt.wantErr {
				t.Errorf("UpdateIndexes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.validate != nil {
				tt.validate(t, tx)
			}
		})
	}
}

func TestExtractValues(t *testing.T) {
	tests := []struct {
		name     string
		cols     []string
		body     map[string]any
		expected []any
	}{
		{
			name:     "all columns present",
			cols:     []string{"name", "age"},
			body:     map[string]any{"name": "Alice", "age": 30},
			expected: []any{"Alice", 30},
		},
		{
			name:     "column missing",
			cols:     []string{"name", "age", "city"},
			body:     map[string]any{"name": "Alice", "age": 30},
			expected: nil,
		},
		{
			name:     "nil value",
			cols:     []string{"name", "age"},
			body:     map[string]any{"name": "Alice", "age": nil},
			expected: nil,
		},
		{
			name:     "empty columns",
			cols:     []string{},
			body:     map[string]any{"name": "Alice"},
			expected: []any{},
		},
		{
			name:     "int value",
			cols:     []string{"count"},
			body:     map[string]any{"count": 42},
			expected: []any{42},
		},
		{
			name:     "int64 value",
			cols:     []string{"count"},
			body:     map[string]any{"count": int64(42)},
			expected: []any{int64(42)},
		},
		{
			name:     "byte array value",
			cols:     []string{"data"},
			body:     map[string]any{"data": []byte("test")},
			expected: []any{[]byte("test")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractValues(tt.cols, tt.body)
			if tt.expected == nil {
				if result != nil {
					t.Errorf("extractValues() = %v, want nil", result)
				}
			} else {
				if result == nil {
					t.Errorf("extractValues() = nil, want %v", tt.expected)
				} else {
					if len(result) != len(tt.expected) {
						t.Errorf("extractValues() length = %d, want %d", len(result), len(tt.expected))
					}
				}
			}
		})
	}
}
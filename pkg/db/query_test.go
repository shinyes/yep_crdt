package db

import (
	"os"
	"testing"

	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestQueryPlanner(t *testing.T) {
	// Setup
	dbPath := "./tmp/test_query_planner"
	os.RemoveAll(dbPath)
	os.MkdirAll(dbPath, 0755)

	s, _ := store.NewBadgerStore(dbPath)
	defer s.Close()
	defer os.RemoveAll(dbPath)

	myDB := Open(s)

	// Define Schema with Composite Index
	err := myDB.DefineTable(&meta.TableSchema{
		ID:   1,
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "region", Type: meta.ColTypeString},
			{Name: "age", Type: meta.ColTypeInt},
			{Name: "name", Type: meta.ColTypeString},
		},
		Indexes: []meta.IndexSchema{
			{ID: 1, Name: "idx_region_age", Columns: []string{"region", "age"}, Unique: false},
			{ID: 2, Name: "idx_age", Columns: []string{"age"}, Unique: false},
		},
	})
	if err != nil {
		t.Fatalf("DefineTable failed: %v", err)
	}

	table := myDB.Table("users")

	// Insert Data
	data := []struct {
		Key  string
		Data map[string]interface{}
	}{
		{"u1", map[string]interface{}{"region": "US", "age": 30, "name": "Alice"}},   // Matches
		{"u2", map[string]interface{}{"region": "US", "age": 15, "name": "Bob"}},     // Region match, Age fail
		{"u3", map[string]interface{}{"region": "EU", "age": 30, "name": "Charlie"}}, // Region fail
		{"u4", map[string]interface{}{"region": "US", "age": 40, "name": "David"}},   // Matches
	}

	for _, d := range data {
		table.Set(d.Key, d.Data)
	}

	// Query: Region="US" AND Age > 20
	// Should use idx_region_age because "region" is Eq, "age" is Range. Score = 2.
	// idx_age would have Score = 1 (Age > 20).

	results, err := table.Where("region", OpEq, "US").And("age", OpGt, 20).Find()
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// Verify results
	names := make(map[string]bool)
	for _, r := range results {
		// Value might be []byte or string depending on encoding.
		// In our simple MVP, table.Set stores what is given.
		// But wait, `crdt.FromBytesMap` decodes LWWRegister value.
		// `encodeValue` in table.go uses fmt.Sprintf("%v", v) -> []byte.
		// So the value in CRDT is []byte.
		val := r["name"]
		var nameStr string
		switch v := val.(type) {
		case []byte:
			nameStr = string(v)
		case string:
			nameStr = v
		}
		names[nameStr] = true
	}

	if !names["Alice"] || !names["David"] {
		t.Errorf("Expected Alice and David, got: %v", names)
	}
}

package db

import (
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestMergeRawRow_NewRowBuildsIndexes(t *testing.T) {
	root := t.TempDir()

	srcStore, err := store.NewBadgerStore(filepath.Join(root, "src-db"))
	if err != nil {
		t.Fatalf("create source store failed: %v", err)
	}
	defer srcStore.Close()
	srcDB, err := Open(srcStore, "src-node")
	if err != nil {
		t.Fatalf("open source db failed: %v", err)
	}
	defer srcDB.Close()

	dstStore, err := store.NewBadgerStore(filepath.Join(root, "dst-db"))
	if err != nil {
		t.Fatalf("create destination store failed: %v", err)
	}
	defer dstStore.Close()
	dstDB, err := Open(dstStore, "dst-node")
	if err != nil {
		t.Fatalf("open destination db failed: %v", err)
	}
	defer dstDB.Close()

	schema := &meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
		Indexes: []meta.IndexSchema{
			{ID: 1, Name: "idx_name", Columns: []string{"name"}},
		},
	}
	if err := srcDB.DefineTable(schema); err != nil {
		t.Fatalf("define source schema failed: %v", err)
	}
	if err := dstDB.DefineTable(schema); err != nil {
		t.Fatalf("define destination schema failed: %v", err)
	}

	key, _ := uuid.NewV7()
	if err := srcDB.Update(func(tx *Tx) error {
		return tx.Table("users").Set(key, map[string]any{"name": "alice"})
	}); err != nil {
		t.Fatalf("seed source row failed: %v", err)
	}

	raw, err := srcDB.Table("users").GetRawRow(key)
	if err != nil {
		t.Fatalf("get source raw row failed: %v", err)
	}

	if err := dstDB.Table("users").MergeRawRow(key, raw); err != nil {
		t.Fatalf("merge raw row failed: %v", err)
	}

	row, err := dstDB.Table("users").Get(key)
	if err != nil {
		t.Fatalf("get merged row failed: %v", err)
	}
	if got := row["name"]; got != "alice" {
		t.Fatalf("merged row mismatch: got=%v want=%s", got, "alice")
	}

	rows, err := dstDB.Table("users").Where("name", OpEq, "alice").Find()
	if err != nil {
		t.Fatalf("index query failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("index query should return merged row, got=%d", len(rows))
	}
	if got := rows[0]["name"]; got != "alice" {
		t.Fatalf("index query row mismatch: got=%v want=%s", got, "alice")
	}
}

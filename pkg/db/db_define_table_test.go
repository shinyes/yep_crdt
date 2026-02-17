package db

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestDefineTableSchemaConflict(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "define-table-conflict")
	kv, err := store.NewBadgerStore(dbPath)
	if err != nil {
		t.Fatalf("NewBadgerStore() failed: %v", err)
	}

	database, err := Open(kv, "tenant-define-table-conflict")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer func() {
		_ = database.Close()
	}()

	if err := database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}); err != nil {
		t.Fatalf("first DefineTable() failed: %v", err)
	}

	err = database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeInt, CrdtType: meta.CrdtCounter},
		},
	})
	if err == nil {
		t.Fatalf("expected DefineTable schema conflict, got nil")
	}
	if !strings.Contains(err.Error(), "schema conflict") {
		t.Fatalf("expected schema conflict error, got: %v", err)
	}
}

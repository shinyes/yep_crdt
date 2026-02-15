package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/shinyes/yep_crdt/pkg/meta"
)

func TestOpenBadgerWithConfig_AppliesEnsureSchema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tenant-a")
	schemaCalled := false

	database, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       dbPath,
		DatabaseID: "tenant-a",
		EnsureSchema: func(d *DB) error {
			schemaCalled = true
			return d.DefineTable(&meta.TableSchema{
				Name: "notes",
				Columns: []meta.ColumnSchema{
					{Name: "title", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
				},
			})
		},
	})
	if err != nil {
		t.Fatalf("OpenBadgerWithConfig failed: %v", err)
	}
	defer database.Close()

	if !schemaCalled {
		t.Fatalf("ensureSchema was not called")
	}

	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("expected db path to exist: %v", err)
	}

	if database.Table("notes") == nil {
		t.Fatalf("expected table notes to be available after ensureSchema")
	}
}

func TestOpenBadgerWithConfig_EnsureSchemaErrorClosesDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tenant-b")
	expectedErr := errors.New("schema setup failed")

	_, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       dbPath,
		DatabaseID: "tenant-b",
		EnsureSchema: func(*DB) error {
			return expectedErr
		},
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected ensureSchema error, got %v", err)
	}

	// If failed setup didn't close the store, this reopen may fail on lock.
	database, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       dbPath,
		DatabaseID: "tenant-b",
	})
	if err != nil {
		t.Fatalf("expected reopen to succeed after ensureSchema failure: %v", err)
	}
	defer database.Close()
}

func TestOpenBadgerWithConfig_AppliesSchemas(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tenant-c")
	ensureCalled := false

	database, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       dbPath,
		DatabaseID: "tenant-c",
		Schemas: []*meta.TableSchema{
			{
				Name: "users",
				Columns: []meta.ColumnSchema{
					{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
				},
			},
		},
		EnsureSchema: func(d *DB) error {
			ensureCalled = true
			return d.DefineTable(&meta.TableSchema{
				Name: "notes",
				Columns: []meta.ColumnSchema{
					{Name: "title", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
				},
			})
		},
	})
	if err != nil {
		t.Fatalf("OpenBadgerWithConfig failed: %v", err)
	}
	defer database.Close()

	if !ensureCalled {
		t.Fatalf("expected EnsureSchema hook to run")
	}
	if database.Table("users") == nil {
		t.Fatalf("expected users table from Schemas")
	}
	if database.Table("notes") == nil {
		t.Fatalf("expected notes table from EnsureSchema")
	}
}

func TestOpenBadgerWithConfig_Validation(t *testing.T) {
	if _, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       "",
		DatabaseID: "tenant-c",
	}); err == nil {
		t.Fatalf("expected empty path to fail")
	}
	if _, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       t.TempDir(),
		DatabaseID: "",
	}); err == nil {
		t.Fatalf("expected empty database id to fail")
	}

	if _, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:                   filepath.Join(t.TempDir(), "tenant-c"),
		DatabaseID:             "tenant-c",
		BadgerValueLogFileSize: -1,
	}); err == nil {
		t.Fatalf("expected invalid badger value log file size to fail")
	}
}

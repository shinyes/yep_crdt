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
	if _, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:                   filepath.Join(t.TempDir(), "tenant-c"),
		DatabaseID:             "tenant-c",
		BadgerValueLogFileSize: 512 * 1024,
	}); err == nil {
		t.Fatalf("expected too-small badger value log file size to fail")
	}
	if _, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:                   filepath.Join(t.TempDir(), "tenant-c"),
		DatabaseID:             "tenant-c",
		BadgerValueLogFileSize: 2 << 30,
	}); err == nil {
		t.Fatalf("expected too-large badger value log file size to fail")
	}

	if _, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       filepath.Join(t.TempDir(), "tenant-c"),
		DatabaseID: "tenant-c",
		Schemas: []*meta.TableSchema{
			{
				Name: "users",
				Columns: []meta.ColumnSchema{
					{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
					{Name: "name", Type: meta.ColTypeInt, CrdtType: meta.CrdtCounter},
				},
			},
		},
	}); err == nil {
		t.Fatalf("expected duplicate schema column to fail")
	}
}

func TestOpenBadgerWithConfig_DatabaseIDMismatchReturnsError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tenant-id-mismatch")

	database, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       dbPath,
		DatabaseID: "tenant-a",
	})
	if err != nil {
		t.Fatalf("first open failed: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}

	if _, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       dbPath,
		DatabaseID: "tenant-b",
	}); err == nil {
		t.Fatalf("expected database id mismatch to return error")
	}

	// Ensure mismatch path does not leak open handles/locks.
	database, err = OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       dbPath,
		DatabaseID: "tenant-a",
	})
	if err != nil {
		t.Fatalf("reopen with original database id should succeed: %v", err)
	}
	defer database.Close()
}

func TestOpenBadgerWithConfig_SchemaConflictReturnsError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tenant-schema-conflict")

	database, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       dbPath,
		DatabaseID: "tenant-z",
		Schemas: []*meta.TableSchema{
			{
				Name: "users",
				Columns: []meta.ColumnSchema{
					{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("first open failed: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}

	if _, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       dbPath,
		DatabaseID: "tenant-z",
		Schemas: []*meta.TableSchema{
			{
				Name: "users",
				Columns: []meta.ColumnSchema{
					{Name: "name", Type: meta.ColTypeInt, CrdtType: meta.CrdtCounter},
				},
			},
		},
	}); err == nil {
		t.Fatalf("expected schema conflict to fail")
	}

	// Ensure conflict path closes resources.
	database, err = OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       dbPath,
		DatabaseID: "tenant-z",
		Schemas: []*meta.TableSchema{
			{
				Name: "users",
				Columns: []meta.ColumnSchema{
					{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("reopen with matching schema should succeed: %v", err)
	}
	defer database.Close()
}

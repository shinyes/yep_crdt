package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestUpdateRollback_DoesNotLeakFileImportOrCallbacks(t *testing.T) {
	database, root, cleanup := openDBForTxConsistency(t)
	defer cleanup()

	srcPath := filepath.Join(root, "src.txt")
	if err := os.WriteFile(srcPath, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write source file failed: %v", err)
	}

	callbackCount := 0
	database.OnChangeDetailed(func(event ChangeEvent) {
		callbackCount++
	})

	key, _ := uuid.NewV7()
	err := database.Update(func(tx *Tx) error {
		if err := tx.Table("docs").Set(key, map[string]any{
			"name": "rollback-test",
			"file": FileImport{
				LocalPath:    srcPath,
				RelativePath: "docs/rollback.txt",
			},
		}); err != nil {
			return err
		}
		if callbackCount != 0 {
			return fmt.Errorf("callback should not run before commit")
		}
		return errors.New("force rollback")
	})
	if err == nil {
		t.Fatal("expected rollback error")
	}

	if _, getErr := database.Table("docs").Get(key); getErr == nil {
		t.Fatal("row should not exist after rollback")
	}

	importedPath := filepath.Join(database.FileStorageDir, "docs", "rollback.txt")
	if _, statErr := os.Stat(importedPath); !os.IsNotExist(statErr) {
		t.Fatalf("imported file should not exist after rollback: %v", statErr)
	}

	if callbackCount != 0 {
		t.Fatalf("callback should not run on rollback, got=%d", callbackCount)
	}
}

func TestUpdateCommit_EmitsCallbacksAfterCommit(t *testing.T) {
	database, root, cleanup := openDBForTxConsistency(t)
	defer cleanup()

	srcPath := filepath.Join(root, "src-commit.txt")
	if err := os.WriteFile(srcPath, []byte("payload-commit"), 0o644); err != nil {
		t.Fatalf("write source file failed: %v", err)
	}

	callbackCount := 0
	database.OnChangeDetailed(func(event ChangeEvent) {
		callbackCount++
	})

	key, _ := uuid.NewV7()
	if err := database.Update(func(tx *Tx) error {
		if err := tx.Table("docs").Set(key, map[string]any{
			"name": "commit-test",
			"file": FileImport{
				LocalPath:    srcPath,
				RelativePath: "docs/commit.txt",
			},
		}); err != nil {
			return err
		}
		if callbackCount != 0 {
			return fmt.Errorf("callback should not run before commit")
		}
		return nil
	}); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	if callbackCount != 1 {
		t.Fatalf("expected 1 callback after commit, got=%d", callbackCount)
	}

	if _, getErr := database.Table("docs").Get(key); getErr != nil {
		t.Fatalf("row should exist after commit: %v", getErr)
	}
	importedPath := filepath.Join(database.FileStorageDir, "docs", "commit.txt")
	if _, statErr := os.Stat(importedPath); statErr != nil {
		t.Fatalf("imported file should exist after commit: %v", statErr)
	}
}

func TestMutationOperations_RejectNonV7UUID(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database, err := Open(s, "non-v7-ops")
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	defer database.Close()

	if err := database.DefineTable(&meta.TableSchema{
		Name: "items",
		Columns: []meta.ColumnSchema{
			{Name: "counter", Type: meta.ColTypeInt, CrdtType: meta.CrdtCounter},
			{Name: "text", Type: meta.ColTypeString, CrdtType: meta.CrdtRGA},
		},
	}); err != nil {
		t.Fatalf("define table failed: %v", err)
	}

	key := uuid.New() // v4
	table := database.Table("items")
	if err := table.Set(key, map[string]any{
		"counter": int64(1),
		"text":    "a",
	}); err == nil {
		t.Fatal("set should reject non-v7 uuid")
	}

	if err := table.Add(key, "counter", int64(1)); err == nil {
		t.Fatal("counter add should reject non-v7 uuid")
	}
	if err := table.Remove(key, "counter", int64(1)); err == nil {
		t.Fatal("counter remove should reject non-v7 uuid")
	}

	if err := table.Add(key, "text", "b"); err == nil {
		t.Fatal("rga add should reject non-v7 uuid")
	}
	if err := table.InsertAfter(key, "text", "a", "c"); err == nil {
		t.Fatal("insert after should reject non-v7 uuid")
	}
	if err := table.InsertAt(key, "text", 0, "z"); err == nil {
		t.Fatal("insert at should reject non-v7 uuid")
	}
	if err := table.RemoveAt(key, "text", 0); err == nil {
		t.Fatal("remove at should reject non-v7 uuid")
	}
}

func openDBForTxConsistency(t *testing.T) (*DB, string, func()) {
	t.Helper()

	root := t.TempDir()
	s, err := store.NewBadgerStore(filepath.Join(root, "db"))
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}

	database, err := Open(s, "tx-consistency")
	if err != nil {
		_ = s.Close()
		t.Fatalf("open db failed: %v", err)
	}

	fileDir := filepath.Join(root, "files")
	if err := os.MkdirAll(fileDir, 0o755); err != nil {
		_ = database.Close()
		_ = s.Close()
		t.Fatalf("create file dir failed: %v", err)
	}
	database.SetFileStorageDir(fileDir)

	if err := database.DefineTable(&meta.TableSchema{
		Name: "docs",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "file", Type: meta.ColTypeString, CrdtType: meta.CrdtLocalFile},
		},
	}); err != nil {
		_ = database.Close()
		_ = s.Close()
		t.Fatalf("define table failed: %v", err)
	}

	cleanup := func() {
		_ = database.Close()
		_ = s.Close()
	}
	return database, root, cleanup
}

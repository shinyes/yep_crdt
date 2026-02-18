package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/meta"
)

func TestDB_BackupToLocalAndRestoreBadger(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "tenant-a")

	database, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       sourcePath,
		DatabaseID: "tenant-a",
		Schemas: []*meta.TableSchema{
			{
				Name: "notes",
				Columns: []meta.ColumnSchema{
					{Name: "title", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}

	noteID := uuid.New()
	if err := database.Table("notes").Set(noteID, map[string]any{
		"title": "backup-roundtrip",
	}); err != nil {
		_ = database.Close()
		t.Fatalf("set row failed: %v", err)
	}

	backupPath := filepath.Join(t.TempDir(), "tenant-a.badgerbak")
	if _, err := database.BackupToLocal(backupPath); err != nil {
		_ = database.Close()
		t.Fatalf("backup failed: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close source db failed: %v", err)
	}

	restorePath := filepath.Join(t.TempDir(), "restore-root", "tenant-a")
	restoredDB, err := RestoreBadgerFromLocalBackup(BadgerRestoreConfig{
		BackupPath: backupPath,
		Path:       restorePath,
		DatabaseID: "tenant-a",
	})
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	defer restoredDB.Close()

	row, err := restoredDB.Table("notes").Get(noteID)
	if err != nil {
		t.Fatalf("read restored row failed: %v", err)
	}
	if got := row["title"]; got != "backup-roundtrip" {
		t.Fatalf("restored row mismatch: got=%v, want=%s", got, "backup-roundtrip")
	}
}

func TestRestoreBadgerFromLocalBackup_WritesUnderParentDir(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "tenant-b")

	database, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       sourcePath,
		DatabaseID: "tenant-b",
		Schemas: []*meta.TableSchema{
			{
				Name: "notes",
				Columns: []meta.ColumnSchema{
					{Name: "title", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}

	id := uuid.New()
	if err := database.Table("notes").Set(id, map[string]any{
		"title": "parent-dir-check",
	}); err != nil {
		_ = database.Close()
		t.Fatalf("set row failed: %v", err)
	}

	backupPath := filepath.Join(t.TempDir(), "tenant-b.badgerbak")
	if _, err := database.BackupToLocal(backupPath); err != nil {
		_ = database.Close()
		t.Fatalf("backup failed: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close source db failed: %v", err)
	}

	restoreParent := filepath.Join(t.TempDir(), "restore-parent")
	restorePath := filepath.Join(restoreParent, "tenant-b")
	restoredDB, err := RestoreBadgerFromLocalBackup(BadgerRestoreConfig{
		BackupPath: backupPath,
		Path:       restorePath,
		DatabaseID: "tenant-b",
	})
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	defer restoredDB.Close()

	if _, err := os.Stat(filepath.Join(restorePath, "MANIFEST")); err != nil {
		t.Fatalf("expected manifest under restored db path: %v", err)
	}
	if _, err := os.Stat(restoreParent); err != nil {
		t.Fatalf("expected restore parent path to be created: %v", err)
	}
}

func TestRestoreBadgerFromLocalBackup_RejectNonEmptyTargetWithoutReplace(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "tenant-c")

	database, err := OpenBadgerWithConfig(BadgerOpenConfig{
		Path:       sourcePath,
		DatabaseID: "tenant-c",
		Schemas: []*meta.TableSchema{
			{
				Name: "notes",
				Columns: []meta.ColumnSchema{
					{Name: "title", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	backupPath := filepath.Join(t.TempDir(), "tenant-c.badgerbak")
	if _, err := database.BackupToLocal(backupPath); err != nil {
		_ = database.Close()
		t.Fatalf("backup failed: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close source db failed: %v", err)
	}

	restorePath := filepath.Join(t.TempDir(), "restore-target", "tenant-c")
	if err := os.MkdirAll(restorePath, 0o755); err != nil {
		t.Fatalf("prepare restore path failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(restorePath, "placeholder.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("prepare restore placeholder failed: %v", err)
	}

	if _, err := RestoreBadgerFromLocalBackup(BadgerRestoreConfig{
		BackupPath: backupPath,
		Path:       restorePath,
		DatabaseID: "tenant-c",
	}); err == nil {
		t.Fatalf("expected restore to fail on non-empty target path")
	}
}

func TestReplaceFileWithBackup_OverwriteExisting(t *testing.T) {
	dir := t.TempDir()
	destPath := filepath.Join(dir, "backup.badgerbak")
	tmpPath := filepath.Join(dir, "backup.badgerbak.tmp")

	if err := os.WriteFile(destPath, []byte("old"), 0o644); err != nil {
		t.Fatalf("write old file failed: %v", err)
	}
	if err := os.WriteFile(tmpPath, []byte("new"), 0o644); err != nil {
		t.Fatalf("write tmp file failed: %v", err)
	}

	if err := replaceFileWithBackup(destPath, tmpPath); err != nil {
		t.Fatalf("replace file failed: %v", err)
	}

	got, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("read replaced file failed: %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("unexpected replaced content: got=%q", string(got))
	}

	if _, err := os.Stat(destPath + ".old"); !os.IsNotExist(err) {
		t.Fatalf("expected backup file cleanup, got stat err=%v", err)
	}
}

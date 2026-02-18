package sync

import (
	"archive/zip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
)

func TestLocalNode_BackupTenantAndRestoreTenant(t *testing.T) {
	dataRoot := t.TempDir()
	tenantID := "tenant-a"
	tenantPath := filepath.Join(dataRoot, tenantID)

	database, err := db.OpenBadgerWithConfig(db.BadgerOpenConfig{
		Path:       tenantPath,
		DatabaseID: tenantID,
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
	if err := database.Table("notes").Set(id, map[string]any{"title": "from-backup"}); err != nil {
		_ = database.Close()
		t.Fatalf("set row failed: %v", err)
	}

	node := &LocalNode{
		databases: map[string]*db.DB{
			tenantID: database,
		},
		tenantIDs: []string{tenantID},
		dataRoot:  dataRoot,
	}

	backupPath := filepath.Join(t.TempDir(), "tenant-a.badgerbak")
	if _, err := node.BackupTenant(tenantID, backupPath); err != nil {
		_ = database.Close()
		t.Fatalf("backup tenant failed: %v", err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("close source db failed: %v", err)
	}
	node.closed = true

	if err := node.RestoreTenant(TenantRestoreOptions{
		TenantID:        tenantID,
		BackupPath:      backupPath,
		ReplaceExisting: true,
	}); err != nil {
		t.Fatalf("restore tenant failed: %v", err)
	}

	restoredDB, err := db.OpenBadgerWithConfig(db.BadgerOpenConfig{
		Path:       tenantPath,
		DatabaseID: tenantID,
	})
	if err != nil {
		t.Fatalf("open restored db failed: %v", err)
	}
	defer restoredDB.Close()

	row, err := restoredDB.Table("notes").Get(id)
	if err != nil {
		t.Fatalf("read restored row failed: %v", err)
	}
	if got := row["title"]; got != "from-backup" {
		t.Fatalf("restored title mismatch: got=%v, want=%s", got, "from-backup")
	}
}

func TestLocalNode_RestoreTenantRejectsRunningTenant(t *testing.T) {
	dataRoot := t.TempDir()
	tenantID := "tenant-b"
	tenantPath := filepath.Join(dataRoot, tenantID)

	database, err := db.OpenBadgerWithConfig(db.BadgerOpenConfig{
		Path:       tenantPath,
		DatabaseID: tenantID,
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
	defer database.Close()

	backupPath := filepath.Join(t.TempDir(), "tenant-b.badgerbak")
	if _, err := database.BackupToLocal(backupPath); err != nil {
		t.Fatalf("backup failed: %v", err)
	}

	node := &LocalNode{
		databases: map[string]*db.DB{
			tenantID: database,
		},
		tenantIDs: []string{tenantID},
		dataRoot:  dataRoot,
	}

	err = node.RestoreTenant(TenantRestoreOptions{
		TenantID:        tenantID,
		BackupPath:      backupPath,
		ReplaceExisting: true,
	})
	if err == nil {
		t.Fatalf("expected restore to fail when tenant is running")
	}
	if !strings.Contains(err.Error(), "close node before restore") {
		t.Fatalf("unexpected restore error: %v", err)
	}
}

func TestLocalNode_BackupTenantRejectsUnknownTenant(t *testing.T) {
	node := &LocalNode{
		databases: map[string]*db.DB{},
	}
	if _, err := node.BackupTenant("unknown", filepath.Join(t.TempDir(), "unknown.badgerbak")); err == nil {
		t.Fatalf("expected backup unknown tenant to fail")
	}
}

func TestLocalNode_BackupAllTenants(t *testing.T) {
	dataRoot := t.TempDir()
	tenantTitles := map[string]string{
		"tenant-a": "title-a",
		"tenant-b": "title-b",
	}
	rowIDs := make(map[string]uuid.UUID, len(tenantTitles))

	node := &LocalNode{
		databases: map[string]*db.DB{},
		tenantIDs: make([]string, 0, len(tenantTitles)),
		dataRoot:  dataRoot,
	}

	for tenantID, title := range tenantTitles {
		database, err := db.OpenBadgerWithConfig(db.BadgerOpenConfig{
			Path:       filepath.Join(dataRoot, tenantID),
			DatabaseID: tenantID,
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
			t.Fatalf("open tenant db %s failed: %v", tenantID, err)
		}
		defer database.Close()

		id := uuid.New()
		rowIDs[tenantID] = id
		if err := database.Table("notes").Set(id, map[string]any{"title": title}); err != nil {
			t.Fatalf("set row for tenant %s failed: %v", tenantID, err)
		}

		node.databases[tenantID] = database
		node.tenantIDs = append(node.tenantIDs, tenantID)
	}

	archivePath := filepath.Join(t.TempDir(), "all-tenants.zip")
	sinceByTenant, err := node.BackupAllTenants(archivePath)
	if err != nil {
		t.Fatalf("backup all tenants failed: %v", err)
	}
	if len(sinceByTenant) != len(tenantTitles) {
		t.Fatalf("unexpected since map size: got=%d, want=%d", len(sinceByTenant), len(tenantTitles))
	}

	zipReader, err := zip.OpenReader(archivePath)
	if err != nil {
		t.Fatalf("open archive failed: %v", err)
	}
	defer zipReader.Close()

	var manifest localNodeArchiveManifest
	entryByName := make(map[string]*zip.File, len(zipReader.File))
	for i := range zipReader.File {
		zf := zipReader.File[i]
		entryByName[zf.Name] = zf
		if zf.Name != "manifest.json" {
			continue
		}
		rc, err := zf.Open()
		if err != nil {
			t.Fatalf("open manifest failed: %v", err)
		}
		manifestBytes, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("read manifest failed: %v", err)
		}
		if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
			t.Fatalf("decode manifest failed: %v", err)
		}
	}

	if manifest.Version != 1 {
		t.Fatalf("unexpected manifest version: %d", manifest.Version)
	}
	if len(manifest.Tenants) != len(tenantTitles) {
		t.Fatalf("unexpected manifest tenant count: got=%d, want=%d", len(manifest.Tenants), len(tenantTitles))
	}

	restoreRoot := filepath.Join(t.TempDir(), "restore-all")
	for _, tenant := range manifest.Tenants {
		entry, ok := entryByName[tenant.File]
		if !ok {
			t.Fatalf("tenant backup entry missing: %s", tenant.File)
		}

		rc, err := entry.Open()
		if err != nil {
			t.Fatalf("open tenant archive entry failed: %v", err)
		}
		backupBytes, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("read tenant archive entry failed: %v", err)
		}

		extractedBackupPath := filepath.Join(t.TempDir(), tenant.TenantID+".badgerbak")
		if err := os.WriteFile(extractedBackupPath, backupBytes, 0o644); err != nil {
			t.Fatalf("write extracted tenant backup failed: %v", err)
		}

		restoredDB, err := db.RestoreBadgerFromLocalBackup(db.BadgerRestoreConfig{
			BackupPath: extractedBackupPath,
			Path:       filepath.Join(restoreRoot, tenant.TenantID),
			DatabaseID: tenant.TenantID,
		})
		if err != nil {
			t.Fatalf("restore tenant %s from combined archive failed: %v", tenant.TenantID, err)
		}

		row, err := restoredDB.Table("notes").Get(rowIDs[tenant.TenantID])
		_ = restoredDB.Close()
		if err != nil {
			t.Fatalf("read restored tenant row failed: %v", err)
		}
		if got, want := row["title"], tenantTitles[tenant.TenantID]; got != want {
			t.Fatalf("restored title mismatch for %s: got=%v, want=%s", tenant.TenantID, got, want)
		}
	}
}

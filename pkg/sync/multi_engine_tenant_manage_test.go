package sync

import (
	"path/filepath"
	"testing"

	"github.com/shinyes/yep_crdt/pkg/db"
)

func TestMultiEngine_AddRemoveTenant(t *testing.T) {
	root := t.TempDir()

	tenantA, err := db.OpenBadgerWithConfig(db.BadgerOpenConfig{
		Path:       filepath.Join(root, "tenant-a"),
		DatabaseID: "tenant-a",
	})
	if err != nil {
		t.Fatalf("open tenant-a failed: %v", err)
	}
	tenantB, err := db.OpenBadgerWithConfig(db.BadgerOpenConfig{
		Path:       filepath.Join(root, "tenant-b"),
		DatabaseID: "tenant-b",
	})
	if err != nil {
		_ = tenantA.Close()
		t.Fatalf("open tenant-b failed: %v", err)
	}

	engine, err := EnableMultiTenantSync([]*db.DB{tenantA}, db.SyncConfig{
		Password:   "test-password",
		ListenPort: 0,
	})
	if err != nil {
		_ = tenantA.Close()
		_ = tenantB.Close()
		t.Fatalf("start multi engine failed: %v", err)
	}
	defer engine.Stop()
	defer tenantA.Close()
	defer tenantB.Close()

	if err := engine.AddTenant(tenantB); err != nil {
		t.Fatalf("add tenant-b failed: %v", err)
	}

	tenantIDs := engine.TenantIDs()
	if len(tenantIDs) != 2 || tenantIDs[0] != "tenant-a" || tenantIDs[1] != "tenant-b" {
		t.Fatalf("unexpected tenant list after add: %v", tenantIDs)
	}
	if _, ok := engine.TenantDatabase("tenant-b"); !ok {
		t.Fatalf("tenant-b runtime should exist after add")
	}

	if err := engine.AddTenant(tenantB); err == nil {
		t.Fatalf("expected duplicate add to fail")
	}

	if err := engine.RemoveTenant("tenant-b"); err != nil {
		t.Fatalf("remove tenant-b failed: %v", err)
	}
	tenantIDs = engine.TenantIDs()
	if len(tenantIDs) != 1 || tenantIDs[0] != "tenant-a" {
		t.Fatalf("unexpected tenant list after remove: %v", tenantIDs)
	}
	if _, ok := engine.TenantDatabase("tenant-b"); ok {
		t.Fatalf("tenant-b runtime should be removed")
	}

	if err := engine.RemoveTenant("tenant-b"); err == nil {
		t.Fatalf("expected removing unknown tenant to fail")
	}
}

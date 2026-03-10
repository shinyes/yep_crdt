package sync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLoadTenantRegistry_NormalizesAndDeduplicates(t *testing.T) {
	registryPath := filepath.Join(t.TempDir(), localNodeTenantRegistryFile)
	payload := localNodeTenantRegistry{
		Version: 1,
		Tenants: []string{" tenant-b ", "tenant-a", "tenant-a", "../invalid"},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal registry failed: %v", err)
	}
	if err := os.WriteFile(registryPath, data, 0o600); err != nil {
		t.Fatalf("write registry failed: %v", err)
	}

	tenantIDs, err := loadTenantRegistry(registryPath)
	if err != nil {
		t.Fatalf("load registry failed: %v", err)
	}
	expected := []string{"tenant-a", "tenant-b"}
	if !reflect.DeepEqual(tenantIDs, expected) {
		t.Fatalf("unexpected tenant IDs: got=%v want=%v", tenantIDs, expected)
	}
}

func TestLocalNodePersistTenantRegistry_WritesSortedList(t *testing.T) {
	registryPath := filepath.Join(t.TempDir(), localNodeTenantRegistryFile)
	node := &LocalNode{
		tenantIDs:          []string{"tenant-c", "tenant-a", "tenant-b"},
		tenantRegistryPath: registryPath,
	}

	if err := node.persistTenantRegistry(); err != nil {
		t.Fatalf("persist tenant registry failed: %v", err)
	}

	tenantIDs, err := loadTenantRegistry(registryPath)
	if err != nil {
		t.Fatalf("reload registry failed: %v", err)
	}
	expected := []string{"tenant-a", "tenant-b", "tenant-c"}
	if !reflect.DeepEqual(tenantIDs, expected) {
		t.Fatalf("unexpected tenant IDs after persist: got=%v want=%v", tenantIDs, expected)
	}
}

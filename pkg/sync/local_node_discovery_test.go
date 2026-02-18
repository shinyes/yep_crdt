package sync

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverTenantLocations_NormalizesTenantIDs(t *testing.T) {
	root := t.TempDir()
	listenPort := 19777

	mustCreateMockBadgerDir(t, filepath.Join(root, " tenant-a"))
	mustCreateMockBadgerDir(t, filepath.Join(root, fmt.Sprintf("tenant-b_%d", listenPort)))
	mustCreateMockBadgerDir(t, filepath.Join(root, fmt.Sprintf(".._%d", listenPort)))

	discovery, err := discoverTenantLocations(root, listenPort)
	if err != nil {
		t.Fatalf("discover tenants failed: %v", err)
	}

	if _, exists := discovery.tenantPaths["tenant-a"]; !exists {
		t.Fatalf("expected normalized tenant id tenant-a, got=%v", discovery.tenantPaths)
	}
	if _, exists := discovery.tenantPaths["tenant-b"]; !exists {
		t.Fatalf("expected tenant id tenant-b from tenant_port layout, got=%v", discovery.tenantPaths)
	}
	if _, exists := discovery.tenantPaths[".."]; exists {
		t.Fatalf("unexpected invalid tenant id discovered: %v", discovery.tenantPaths)
	}
	if _, exists := discovery.tenantPaths[" tenant-a"]; exists {
		t.Fatalf("raw unnormalized tenant id should not appear: %v", discovery.tenantPaths)
	}
}

func mustCreateMockBadgerDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create dir failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "MANIFEST"), []byte("mock"), 0o644); err != nil {
		t.Fatalf("create manifest failed: %v", err)
	}
}

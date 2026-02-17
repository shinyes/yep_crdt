package sync

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/shinyes/yep_crdt/pkg/db"
)

func discoverTenantLocations(root string, listenPort int) (tenantDiscovery, error) {
	fallback := tenantDiscovery{
		tenantIDs:   nil,
		tenantPaths: map[string]string{},
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return fallback, nil
		}
		return fallback, err
	}

	byPort := make(map[string]string, len(entries))
	suffix := fmt.Sprintf("_%d", listenPort)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		tenantID := strings.TrimSuffix(name, suffix)
		if tenantID == "" {
			continue
		}
		path := filepath.Join(root, name)
		if !looksLikeBadgerDir(path) {
			continue
		}
		byPort[tenantID] = path
	}

	byTenant := make(map[string]string, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == "_tenet_identity" || strings.HasPrefix(name, ".") {
			continue
		}
		path := filepath.Join(root, name)
		if !looksLikeBadgerDir(path) {
			continue
		}
		byTenant[name] = path
	}

	if len(byPort) == 0 && len(byTenant) == 0 {
		return fallback, nil
	}

	tenantPaths := make(map[string]string, len(byPort)+len(byTenant))
	for tenantID, path := range byPort {
		tenantPaths[tenantID] = path
	}
	// Merge tenant-only layout; if tenant exists in both, keep tenant_port layout.
	for tenantID, path := range byTenant {
		if _, exists := tenantPaths[tenantID]; exists {
			continue
		}
		tenantPaths[tenantID] = path
	}

	return tenantDiscovery{
		tenantIDs:   sortedTenantIDs(tenantPaths),
		tenantPaths: tenantPaths,
	}, nil
}

func sortedTenantIDs(paths map[string]string) []string {
	tenantIDs := make([]string, 0, len(paths))
	for tenantID := range paths {
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)
	return tenantIDs
}

func looksLikeBadgerDir(path string) bool {
	if path == "" {
		return false
	}
	manifestPath := filepath.Join(path, "MANIFEST")
	info, err := os.Stat(manifestPath)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func closeDatabases(databases []*db.DB) {
	for _, database := range databases {
		if database != nil {
			_ = database.Close()
		}
	}
}

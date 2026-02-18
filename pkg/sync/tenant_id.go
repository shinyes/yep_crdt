package sync

import (
	"fmt"
	"path/filepath"
	"strings"
)

func normalizeTenantID(raw string) (string, error) {
	tenantID := strings.TrimSpace(raw)
	if tenantID == "" {
		return "", fmt.Errorf("tenant id cannot be empty")
	}
	if filepath.IsAbs(tenantID) {
		return "", fmt.Errorf("tenant id cannot be absolute path: %s", tenantID)
	}
	if strings.ContainsAny(tenantID, `/\`) {
		return "", fmt.Errorf("tenant id cannot contain path separator: %s", tenantID)
	}
	if strings.Contains(tenantID, ":") {
		return "", fmt.Errorf("tenant id cannot contain ':' : %s", tenantID)
	}
	clean := filepath.Clean(tenantID)
	if clean != tenantID || clean == "." || clean == ".." {
		return "", fmt.Errorf("invalid tenant id: %s", tenantID)
	}
	return tenantID, nil
}

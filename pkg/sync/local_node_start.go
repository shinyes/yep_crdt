package sync

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/shinyes/yep_crdt/pkg/db"
)

const localNodeTenantRegistryFile = "_tenant_subscriptions.json"

// StartNodeFromDataRoot discovers local tenant databases, opens them, and starts multi-tenant sync.
func StartNodeFromDataRoot(opts NodeFromDataRootOptions) (*LocalNode, error) {
	if opts.ListenPort <= 0 {
		return nil, fmt.Errorf("listen port must be > 0")
	}
	if strings.TrimSpace(opts.Password) == "" {
		return nil, fmt.Errorf("sync password cannot be empty")
	}

	dataRoot := opts.DataRoot
	if dataRoot == "" {
		dataRoot = "."
	}

	discovery, err := discoverTenantLocations(dataRoot, opts.ListenPort)
	if err != nil {
		return nil, err
	}
	tenantIDs := append([]string(nil), discovery.tenantIDs...)
	tenantPaths := make(map[string]string, len(discovery.tenantPaths))
	for tenantID, path := range discovery.tenantPaths {
		tenantPaths[tenantID] = path
	}
	tenantRegistryPath := filepath.Join(dataRoot, localNodeTenantRegistryFile)
	subscribedTenantIDs, err := loadTenantRegistry(tenantRegistryPath)
	if err != nil {
		return nil, err
	}
	if len(subscribedTenantIDs) > 0 {
		tenantIDs = tenantIDs[:0]
		for _, tenantID := range subscribedTenantIDs {
			tenantIDs = append(tenantIDs, tenantID)
			if _, exists := tenantPaths[tenantID]; !exists {
				tenantPaths[tenantID] = filepath.Join(dataRoot, tenantID)
			}
		}
	}
	if len(tenantIDs) == 0 {
		return nil, fmt.Errorf("no tenant discovered in data root: %s", dataRoot)
	}
	sort.Strings(tenantIDs)

	identityPath := opts.IdentityPath
	if identityPath == "" {
		identityPath = filepath.Join(dataRoot, "_tenet_identity", fmt.Sprintf("node_%d.json", opts.ListenPort))
	}

	if opts.Reset {
		for _, tenantID := range tenantIDs {
			if err := os.RemoveAll(tenantPaths[tenantID]); err != nil {
				return nil, err
			}
		}
		if err := os.Remove(identityPath); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}

	databases := make(map[string]*db.DB, len(tenantIDs))
	openOrder := make([]*db.DB, 0, len(tenantIDs))

	for _, tenantID := range tenantIDs {
		nodeDir := tenantPaths[tenantID]
		database, err := db.OpenBadgerWithConfig(db.BadgerOpenConfig{
			Path:                   nodeDir,
			DatabaseID:             tenantID,
			BadgerValueLogFileSize: opts.BadgerValueLogFileSize,
			EnsureSchema:           opts.EnsureSchema,
		})
		if err != nil {
			closeDatabases(openOrder)
			return nil, err
		}

		databases[tenantID] = database
		openOrder = append(openOrder, database)
	}

	nodeOpts := make([]Option, 0, 2)
	if opts.IncrementalOnly {
		nodeOpts = append(nodeOpts, WithTimeoutThreshold(0), WithClockThreshold(0))
	}

	engine, err := EnableMultiTenantSync(openOrder, db.SyncConfig{
		ListenPort:   opts.ListenPort,
		ConnectTo:    opts.ConnectTo,
		Password:     opts.Password,
		Debug:        opts.Debug,
		IdentityPath: identityPath,
	}, nodeOpts...)
	if err != nil {
		closeDatabases(openOrder)
		return nil, err
	}

	node := &LocalNode{
		engine:                 engine,
		databases:              databases,
		tenantIDs:              tenantIDs,
		dataRoot:               dataRoot,
		tenantRegistryPath:     tenantRegistryPath,
		badgerValueLogFileSize: opts.BadgerValueLogFileSize,
		ensureSchema:           opts.EnsureSchema,
	}
	if err := node.persistTenantRegistry(); err != nil {
		engine.Stop()
		closeDatabases(openOrder)
		return nil, err
	}
	return node, nil
}

type localNodeTenantRegistry struct {
	Version int      `json:"version"`
	Tenants []string `json:"tenants"`
}

func loadTenantRegistry(path string) ([]string, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var data localNodeTenantRegistry
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, err
	}
	tenants := make([]string, 0, len(data.Tenants))
	seen := make(map[string]struct{}, len(data.Tenants))
	for _, rawTenantID := range data.Tenants {
		tenantID, err := normalizeTenantID(rawTenantID)
		if err != nil {
			continue
		}
		if _, exists := seen[tenantID]; exists {
			continue
		}
		seen[tenantID] = struct{}{}
		tenants = append(tenants, tenantID)
	}
	sort.Strings(tenants)
	return tenants, nil
}

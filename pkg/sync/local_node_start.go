package sync

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/shinyes/yep_crdt/pkg/db"
)

// StartLocalNode discovers local tenant databases, opens them, and starts multi-tenant sync.
func StartLocalNode(opts LocalNodeOptions) (*LocalNode, error) {
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
	tenantIDs := discovery.tenantIDs
	if len(tenantIDs) == 0 {
		return nil, fmt.Errorf("no tenant discovered in data root: %s", dataRoot)
	}
	tenantPaths := make(map[string]string, len(tenantIDs))
	for tenantID, path := range discovery.tenantPaths {
		tenantPaths[tenantID] = path
	}

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

	return &LocalNode{
		engine:    engine,
		databases: databases,
		tenantIDs: tenantIDs,
	}, nil
}

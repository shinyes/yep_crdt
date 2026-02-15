package sync

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/shinyes/yep_crdt/pkg/db"
)

// LocalNodeOptions configures local filesystem based multi-tenant startup.
type LocalNodeOptions struct {
	DataRoot     string
	ListenPort   int
	ConnectTo    string
	Password     string
	Debug        bool
	Reset        bool
	IdentityPath string
	// BadgerValueLogFileSize sets max bytes per Badger vlog file. 0 means store default (128MB).
	BadgerValueLogFileSize int64
	EnsureSchema           func(*db.DB) error
}

// LocalNode represents a started local multi-tenant node.
type LocalNode struct {
	mu        sync.RWMutex
	engine    *MultiEngine
	databases map[string]*db.DB
	tenantIDs []string
	closed    bool
}

type tenantDiscovery struct {
	tenantIDs   []string
	tenantPaths map[string]string
}

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

	engine, err := EnableMultiTenantSync(openOrder, db.SyncConfig{
		ListenPort:   opts.ListenPort,
		ConnectTo:    opts.ConnectTo,
		Password:     opts.Password,
		Debug:        opts.Debug,
		IdentityPath: identityPath,
	})
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

// Close stops sync and closes all opened databases.
func (n *LocalNode) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	engine := n.engine
	databases := make([]*db.DB, 0, len(n.databases))
	for _, database := range n.databases {
		databases = append(databases, database)
	}
	n.mu.Unlock()

	if engine != nil {
		engine.Stop()
	}

	var errs []error
	for _, database := range databases {
		if database == nil {
			continue
		}
		if err := database.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// Engine returns the running multi-tenant engine.
func (n *LocalNode) Engine() *MultiEngine {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.engine
}

// TenantDatabase returns one opened tenant database and whether it exists.
func (n *LocalNode) TenantDatabase(tenantID string) (*db.DB, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	database, ok := n.databases[tenantID]
	if !ok || database == nil {
		return nil, false
	}
	return database, true
}

// TenantIDs returns all started tenant IDs.
func (n *LocalNode) TenantIDs() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	out := make([]string, len(n.tenantIDs))
	copy(out, n.tenantIDs)
	return out
}

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

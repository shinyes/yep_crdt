package sync

import (
	"errors"
	"fmt"
	"time"

	"github.com/shinyes/yep_crdt/pkg/db"
)

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

// ManualGCTenant triggers negotiated manual GC for one tenant.
func (n *LocalNode) ManualGCTenant(tenantID string, timeout time.Duration) (*ManualGCResult, error) {
	n.mu.RLock()
	engine := n.engine
	closed := n.closed
	n.mu.RUnlock()

	if closed {
		return nil, fmt.Errorf("local node is closed")
	}
	if engine == nil {
		return nil, fmt.Errorf("sync engine is not started")
	}

	return engine.ManualGC(tenantID, timeout)
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

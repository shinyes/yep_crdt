package sync

import (
	"fmt"
	"log"
	"sort"
	"sync/atomic"

	"github.com/shinyes/yep_crdt/pkg/db"
)

func (m *MultiEngine) onPeerConnected(peerID string) {
	runtimes := m.snapshotRuntimes()
	for _, rt := range runtimes {
		log.Printf("[MultiEngine:%s] peer connected: %s", rt.tenantID, shortPeerID(peerID))
		rt.nodeMgr.OnPeerConnected(peerID)
		go rt.vs.OnPeerConnected(peerID)
	}
}

func (m *MultiEngine) onPeerDisconnected(peerID string) {
	runtimes := m.snapshotRuntimes()
	for _, rt := range runtimes {
		log.Printf("[MultiEngine:%s] peer disconnected: %s", rt.tenantID, shortPeerID(peerID))
		rt.nodeMgr.OnPeerDisconnected(peerID)
		if rt.chunks != nil {
			rt.chunks.CleanupPeer(peerID)
		}
	}
}

func (m *MultiEngine) snapshotRuntimes() []*tenantRuntime {
	m.mu.RLock()
	defer m.mu.RUnlock()

	runtimes := make([]*tenantRuntime, 0, len(m.tenants))
	for _, rt := range m.tenants {
		runtimes = append(runtimes, rt)
	}
	return runtimes
}

// Stop stops all tenant runtimes and the shared transport.
func (m *MultiEngine) Stop() {
	runtimes := m.snapshotRuntimes()
	for _, rt := range runtimes {
		rt.cancel()
	}
	for _, rt := range runtimes {
		rt.workerWg.Wait()
		rt.nodeMgr.Stop()
	}
	if m.network != nil {
		m.network.Stop()
	}
}

// Connect connects shared transport to a remote node.
func (m *MultiEngine) Connect(addr string) error {
	if m.network == nil {
		return fmt.Errorf("network not started")
	}
	return m.network.Connect(addr)
}

// Peers returns connected peers on shared transport.
func (m *MultiEngine) Peers() []string {
	if m.network == nil {
		return nil
	}
	return m.network.Peers()
}

// LocalAddr returns shared local listen address.
func (m *MultiEngine) LocalAddr() string {
	if m.network == nil {
		return ""
	}
	return m.network.LocalAddr()
}

// LocalID returns shared local node ID.
func (m *MultiEngine) LocalID() string {
	if m.network == nil {
		return ""
	}
	return m.network.LocalID()
}

// TenantIDs returns all started tenant IDs.
func (m *MultiEngine) TenantIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tenantIDs := make([]string, 0, len(m.tenants))
	for tenantID := range m.tenants {
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)
	return tenantIDs
}

// TenantDatabase returns one tenant database if started.
func (m *MultiEngine) TenantDatabase(tenantID string) (*db.DB, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rt, exists := m.tenants[tenantID]
	if !exists || rt == nil || rt.db == nil {
		return nil, false
	}
	return rt.db, true
}

// TenantStats returns one tenant's runtime metrics.
func (m *MultiEngine) TenantStats(tenantID string) (EngineStats, bool) {
	m.mu.RLock()
	rt, exists := m.tenants[tenantID]
	network := m.network
	m.mu.RUnlock()
	if !exists {
		return EngineStats{}, false
	}

	stats := EngineStats{
		ChangeEnqueued:     atomic.LoadUint64(&rt.stats.changeEnqueued),
		ChangeProcessed:    atomic.LoadUint64(&rt.stats.changeProcessed),
		ChangeBackpressure: atomic.LoadUint64(&rt.stats.changeBackpressure),
	}
	if rt.changeQ != nil {
		stats.ChangeQueueDepth = len(rt.changeQ)
	}
	if network != nil {
		stats.Network = network.Stats()
	}
	return stats, true
}

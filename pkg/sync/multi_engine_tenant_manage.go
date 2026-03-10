package sync

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"

	"github.com/shinyes/yep_crdt/pkg/db"
)

// AddTenant starts sync runtime for one tenant on the existing shared transport.
func (m *MultiEngine) AddTenant(database *db.DB) error {
	rt, err := m.addTenantRuntime(database, true)
	if err != nil {
		return err
	}
	m.startTenantRuntime(rt, true)
	return nil
}

// RemoveTenant stops sync runtime for one tenant and leaves its transport channel.
func (m *MultiEngine) RemoveTenant(tenantID string) error {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return fmt.Errorf("tenant id is required")
	}

	m.mu.Lock()
	rt, exists := m.tenants[tenantID]
	if !exists || rt == nil {
		m.mu.Unlock()
		return fmt.Errorf("tenant not started: %s", tenantID)
	}
	delete(m.tenants, tenantID)
	m.mu.Unlock()

	if err := m.stopTenantRuntime(rt, true); err != nil {
		return err
	}
	return nil
}

func (m *MultiEngine) addTenantRuntime(database *db.DB, joinTransport bool) (*tenantRuntime, error) {
	if m == nil {
		return nil, fmt.Errorf("multi engine is nil")
	}
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	tenantID := strings.TrimSpace(database.DatabaseID)
	if tenantID == "" {
		return nil, fmt.Errorf("database has empty tenant id")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.network == nil {
		return nil, fmt.Errorf("network not started")
	}
	if _, exists := m.tenants[tenantID]; exists {
		return nil, fmt.Errorf("tenant already started: %s", tenantID)
	}

	joined := false
	if joinTransport {
		if err := m.network.JoinTenant(tenantID); err != nil {
			return nil, fmt.Errorf("join tenant channel failed: %w", err)
		}
		joined = true
	}

	rt, err := m.newTenantRuntimeLocked(database, tenantID)
	if err != nil {
		if joined {
			_ = m.network.LeaveTenant(tenantID)
		}
		return nil, err
	}
	m.tenants[tenantID] = rt
	return rt, nil
}

func (m *MultiEngine) newTenantRuntimeLocked(database *db.DB, tenantID string) (*tenantRuntime, error) {
	if m.network == nil {
		return nil, fmt.Errorf("network not started")
	}

	scopedNetwork := &tenantScopedNetwork{
		tenantID: tenantID,
		network:  m.network,
	}

	nodeMgr := NewNodeManager(database, m.network.LocalID(), m.nodeOpts...)
	nodeMgr.RegisterNetwork(scopedNetwork)

	ctx, cancel := context.WithCancel(context.Background())
	rt := &tenantRuntime{
		tenantID: tenantID,
		db:       database,
		network:  scopedNetwork,
		nodeMgr:  nodeMgr,
		vs:       NewVersionSync(database, nodeMgr),
		chunks:   newLocalFileChunkReceiver(),
		ctx:      ctx,
		cancel:   cancel,
		changeQ:  make(chan db.ChangeEvent, engineChangeQueueSize),
	}

	m.network.SetTenantBroadcastHandler(tenantID, PeerMessageHandler{
		OnReceive: func(peerID string, msg NetworkMessage) {
			rt.handleMessage(peerID, msg)
		},
	})

	database.OnChangeDetailed(func(event db.ChangeEvent) {
		rt.enqueueChange(event)
	})

	return rt, nil
}

func (m *MultiEngine) startTenantRuntime(rt *tenantRuntime, syncExistingPeers bool) {
	if rt == nil {
		return
	}

	rt.nodeMgr.Start(rt.ctx)
	rt.workerWg.Add(1)
	go rt.runChangeWorker()

	if !syncExistingPeers {
		return
	}

	for _, peerID := range m.Peers() {
		if peerID == "" || peerID == rt.nodeMgr.GetLocalNodeID() {
			continue
		}
		rt.nodeMgr.OnPeerConnected(peerID)
		go rt.vs.OnPeerConnected(peerID)
	}
}

func (m *MultiEngine) stopTenantRuntime(rt *tenantRuntime, leaveTransport bool) error {
	if rt == nil {
		return nil
	}

	if m.network != nil {
		m.network.RemoveTenantBroadcastHandler(rt.tenantID)
	}

	rt.cancel()
	rt.workerWg.Wait()
	rt.nodeMgr.Stop()

	if leaveTransport && m.network != nil {
		if err := m.network.LeaveTenant(rt.tenantID); err != nil {
			return fmt.Errorf("leave tenant channel failed: %w", err)
		}
	}
	return nil
}

func (rt *tenantRuntime) enqueueChange(event db.ChangeEvent) {
	select {
	case <-rt.ctx.Done():
		return
	default:
	}

	select {
	case rt.changeQ <- event:
		atomic.AddUint64(&rt.stats.changeEnqueued, 1)
	default:
		atomic.AddUint64(&rt.stats.changeBackpressure, 1)
		log.Printf("[MultiEngine:%s] change queue full, drop change: table=%s, key=%s",
			rt.tenantID, event.TableName, shortPeerID(event.Key.String()))
	}
}

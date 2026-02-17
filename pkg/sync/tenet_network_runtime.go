package sync

import (
	"fmt"
	stdlog "log"
)

// Start starts the tenant network.
func (tn *TenantNetwork) Start() error {
	stdlog.Printf("[TenantNetwork:%s] starting tunnel, port=%d", tn.tenantID, tn.config.ListenPort)
	if err := tn.tunnel.Start(); err != nil {
		return fmt.Errorf("start tunnel failed: %w", err)
	}
	localID := tn.tunnel.LocalID()
	if localID != "" {
		tn.localNodeID.Store(localID)
	}
	stdlog.Printf("[TenantNetwork:%s] local node ID: %s", tn.tenantID, localID)
	stdlog.Printf("[TenantNetwork:%s] local addr: %s", tn.tenantID, tn.tunnel.LocalAddr())
	return nil
}

// Stop stops the tenant network.
func (tn *TenantNetwork) Stop() {
	stdlog.Printf("[TenantNetwork:%s] stopping tunnel", tn.tenantID)
	tn.cancel()
	tn.mu.Lock()
	tn.dropAllPendingResponsesLocked(true)
	tn.mu.Unlock()
	_ = tn.tunnel.GracefulStop()
}

// Connect connects to another node.
func (tn *TenantNetwork) Connect(addr string) error {
	stdlog.Printf("[TenantNetwork:%s] connecting to %s", tn.tenantID, addr)
	return tn.tunnel.Connect(addr)
}

// LocalID returns local node ID.
func (tn *TenantNetwork) LocalID() string {
	return tn.tunnel.LocalID()
}

// LocalAddr returns local listen address.
func (tn *TenantNetwork) LocalAddr() string {
	return tn.tunnel.LocalAddr()
}

// Peers returns connected peers.
func (tn *TenantNetwork) Peers() []string {
	return tn.tunnel.Peers()
}

// TenantID returns tenant ID.
func (tn *TenantNetwork) TenantID() string {
	return tn.tenantID
}

// SetPeerHandler sets a handler for one peer.
func (tn *TenantNetwork) SetPeerHandler(peerID string, handler PeerMessageHandler) {
	tn.mu.Lock()
	tn.peerHandlers[peerID] = handler
	tn.mu.Unlock()
}

// SetBroadcastHandler sets a handler for all incoming messages.
func (tn *TenantNetwork) SetBroadcastHandler(handler PeerMessageHandler) {
	tn.mu.Lock()
	tn.broadcastHandler = handler
	tn.mu.Unlock()
}

// SetTenantBroadcastHandler sets a handler for one tenant/channel.
func (tn *TenantNetwork) SetTenantBroadcastHandler(tenantID string, handler PeerMessageHandler) {
	if tenantID == "" {
		return
	}
	tn.mu.Lock()
	tn.tenantHandlers[tenantID] = handler
	tn.mu.Unlock()
}

// RemoveTenantBroadcastHandler removes tenant-specific handler.
func (tn *TenantNetwork) RemoveTenantBroadcastHandler(tenantID string) {
	if tenantID == "" {
		return
	}
	tn.mu.Lock()
	delete(tn.tenantHandlers, tenantID)
	tn.mu.Unlock()
}

// AddPeerConnectedHandler adds a callback without overriding internal callbacks.
func (tn *TenantNetwork) AddPeerConnectedHandler(handler func(peerID string)) {
	if handler == nil {
		return
	}
	tn.mu.Lock()
	tn.onPeerConnected = append(tn.onPeerConnected, handler)
	tn.mu.Unlock()
}

// AddPeerDisconnectedHandler adds a callback without overriding internal callbacks.
func (tn *TenantNetwork) AddPeerDisconnectedHandler(handler func(peerID string)) {
	if handler == nil {
		return
	}
	tn.mu.Lock()
	tn.onPeerDropped = append(tn.onPeerDropped, handler)
	tn.mu.Unlock()
}

// RemovePeerHandler removes one peer handler.
func (tn *TenantNetwork) RemovePeerHandler(peerID string) {
	tn.mu.Lock()
	delete(tn.peerHandlers, peerID)
	tn.dropPendingResponsesForPeerLocked(peerID, true)
	tn.mu.Unlock()
}

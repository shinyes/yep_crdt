package sync

import (
	stdlog "log"
	"sync/atomic"

	"github.com/vmihailenco/msgpack/v5"
)

func (tn *TenantNetwork) setupCallbacks() {
	tn.tunnel.OnReceive(func(peerID string, data []byte) {
		tn.handleReceive(peerID, data)
	})

	tn.tunnel.OnPeerConnected(func(peerID string) {
		stdlog.Printf("[TenantNetwork:%s] node connected: %s", tn.tenantID, peerID)

		tn.mu.RLock()
		hooks := append([]func(string){}, tn.onPeerConnected...)
		tn.mu.RUnlock()

		for _, hook := range hooks {
			if hook != nil {
				hook(peerID)
			}
		}
	})

	tn.tunnel.OnPeerDisconnected(func(peerID string) {
		stdlog.Printf("[TenantNetwork:%s] node disconnected: %s", tn.tenantID, peerID)

		tn.mu.Lock()
		tn.dropPendingResponsesForPeerLocked(peerID, true)
		hooks := append([]func(string){}, tn.onPeerDropped...)
		tn.mu.Unlock()

		for _, hook := range hooks {
			if hook != nil {
				hook(peerID)
			}
		}
	})
}

func (tn *TenantNetwork) handleReceive(peerID string, data []byte) {
	var lite fetchRawResponseLite
	if err := msgpack.Unmarshal(data, &lite); err != nil {
		stdlog.Printf("[TenantNetwork:%s] parse message failed: %v", tn.tenantID, err)
		return
	}

	// Route only matched response messages to in-flight waiters.
	if lite.RequestID != "" {
		tn.mu.RLock()
		waiter, ok := tn.responseChannels[lite.RequestID]
		tn.mu.RUnlock()

		if ok && isWaiterResponseType(lite.Type) {
			if waiter.peerID != "" && waiter.peerID != peerID {
				atomic.AddUint64(&tn.stats.unexpectedPeerResponses, 1)
				stdlog.Printf("[TenantNetwork:%s] ignore response from unexpected peer: request_id=%s expected=%s got=%s",
					tn.tenantID, lite.RequestID, waiter.peerID, peerID)
				return
			}

			// Hot path: fetch raw responses only need a subset of fields.
			if lite.Type == MsgTypeFetchRawResponse {
				tn.routeFetchRawResponseToWaiter(waiter, lite, peerID)
				return
			}

			var msg NetworkMessage
			if err := msgpack.Unmarshal(data, &msg); err != nil {
				stdlog.Printf("[TenantNetwork:%s] parse message failed: %v", tn.tenantID, err)
				return
			}
			tn.routeResponseToWaiter(waiter, msg, peerID)
			return
		}
	}

	var msg NetworkMessage
	if err := msgpack.Unmarshal(data, &msg); err != nil {
		stdlog.Printf("[TenantNetwork:%s] parse message failed: %v", tn.tenantID, err)
		return
	}

	tenantID := msg.TenantID
	if tenantID == "" {
		tenantID = tn.tenantID
	}

	tn.mu.RLock()
	handler, ok := tn.peerHandlers[peerID]
	broadcastHandler := tn.broadcastHandler
	tenantHandler, hasTenantHandler := tn.tenantHandlers[tenantID]
	tn.mu.RUnlock()

	if hasTenantHandler && tenantHandler.OnReceive != nil {
		tenantHandler.OnReceive(peerID, msg)
	} else if ok && handler.OnReceive != nil {
		handler.OnReceive(peerID, msg)
	} else if broadcastHandler.OnReceive != nil {
		broadcastHandler.OnReceive(peerID, msg)
	}
}

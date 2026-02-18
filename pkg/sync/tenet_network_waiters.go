package sync

import (
	stdlog "log"
	"sync/atomic"
)

func (tn *TenantNetwork) dropPendingResponsesForPeerLocked(peerID string, notify bool) {
	for requestID, waiter := range tn.responseChannels {
		if waiter.peerID != peerID {
			continue
		}
		delete(tn.responseChannels, requestID)
		if !notify {
			continue
		}
		if waiter.ch != nil {
			select {
			case waiter.ch <- NetworkMessage{Type: internalMsgTypePeerDisconnected}:
			default:
			}
		}
		if waiter.fetchCh != nil {
			select {
			case waiter.fetchCh <- fetchRawResponseLite{Type: internalMsgTypePeerDisconnected}:
			default:
			}
		}
	}
}

func (tn *TenantNetwork) dropAllPendingResponsesLocked(notify bool) {
	for requestID, waiter := range tn.responseChannels {
		delete(tn.responseChannels, requestID)
		if !notify {
			continue
		}
		if waiter.ch != nil {
			select {
			case waiter.ch <- NetworkMessage{Type: internalMsgTypePeerDisconnected}:
			default:
			}
		}
		if waiter.fetchCh != nil {
			select {
			case waiter.fetchCh <- fetchRawResponseLite{Type: internalMsgTypePeerDisconnected}:
			default:
			}
		}
	}
}

func (tn *TenantNetwork) signalWaiterOverflow(waiter pendingResponse) {
	if waiter.overflowCh == nil {
		return
	}
	select {
	case waiter.overflowCh <- struct{}{}:
	default:
	}
}

func (tn *TenantNetwork) routeResponseToWaiter(waiter pendingResponse, msg NetworkMessage, peerID string) {
	if waiter.ch == nil {
		atomic.AddUint64(&tn.stats.droppedResponses, 1)
		stdlog.Printf("[TenantNetwork:%s] response channel missing: request_id=%s peer=%s", tn.tenantID, msg.RequestID, peerID)
		tn.signalWaiterOverflow(waiter)
		return
	}

	select {
	case waiter.ch <- msg:
		atomic.AddUint64(&tn.stats.routedResponses, 1)
	default:
		atomic.AddUint64(&tn.stats.droppedResponses, 1)
		stdlog.Printf("[TenantNetwork:%s] response channel is full: request_id=%s peer=%s", tn.tenantID, msg.RequestID, peerID)
		tn.signalWaiterOverflow(waiter)
	}
}

func (tn *TenantNetwork) routeFetchRawResponseToWaiter(waiter pendingResponse, msg fetchRawResponseLite, peerID string) {
	if waiter.fetchCh != nil {
		select {
		case waiter.fetchCh <- msg:
			atomic.AddUint64(&tn.stats.routedResponses, 1)
		default:
			atomic.AddUint64(&tn.stats.droppedResponses, 1)
			stdlog.Printf("[TenantNetwork:%s] fetch response channel is full: request_id=%s peer=%s",
				tn.tenantID, msg.RequestID, peerID)
			tn.signalWaiterOverflow(waiter)
		}
		return
	}

	tn.routeResponseToWaiter(waiter, NetworkMessage{
		Type:       msg.Type,
		RequestID:  msg.RequestID,
		Key:        msg.Key,
		RawData:    msg.RawData,
		LocalFiles: msg.LocalFiles,
	}, peerID)
}

package sync

import "sync/atomic"

// Stats returns a transport-level metrics snapshot.
func (tn *TenantNetwork) Stats() TenantNetworkStats {
	tn.mu.RLock()
	inFlight := len(tn.responseChannels)
	tn.mu.RUnlock()

	return TenantNetworkStats{
		RoutedResponses:         atomic.LoadUint64(&tn.stats.routedResponses),
		UnexpectedPeerResponses: atomic.LoadUint64(&tn.stats.unexpectedPeerResponses),
		DroppedResponses:        atomic.LoadUint64(&tn.stats.droppedResponses),
		FetchRequests:           atomic.LoadUint64(&tn.stats.fetchRequests),
		FetchRequestSendErrors:  atomic.LoadUint64(&tn.stats.fetchRequestSendErrors),
		FetchRowsReceived:       atomic.LoadUint64(&tn.stats.fetchRowsReceived),
		FetchSuccess:            atomic.LoadUint64(&tn.stats.fetchSuccess),
		FetchTimeouts:           atomic.LoadUint64(&tn.stats.fetchTimeouts),
		FetchPartialTimeouts:    atomic.LoadUint64(&tn.stats.fetchPartialTimeouts),
		FetchOverflows:          atomic.LoadUint64(&tn.stats.fetchOverflows),
		FetchPeerDisconnected:   atomic.LoadUint64(&tn.stats.fetchPeerDisconnected),
		FetchChannelClosed:      atomic.LoadUint64(&tn.stats.fetchChannelClosed),
		InFlightRequests:        inFlight,
	}
}

package sync

import (
	"errors"
	stdlog "log"
	"sync/atomic"
	"time"
)

// FetchRawTableData fetches raw table data with default timeout.
func (tn *TenantNetwork) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	return tn.fetchRawTableDataWithTenant(sourceNodeID, tableName, tn.tenantID, 30*time.Second)
}

// FetchRawTableDataWithTimeout fetches raw table data with timeout.
func (tn *TenantNetwork) FetchRawTableDataWithTimeout(sourceNodeID string, tableName string, timeout time.Duration) ([]RawRowData, error) {
	return tn.fetchRawTableDataWithTenant(sourceNodeID, tableName, tn.tenantID, timeout)
}

func (tn *TenantNetwork) fetchRawTableDataWithTenant(sourceNodeID string, tableName string, tenantID string, timeout time.Duration) ([]RawRowData, error) {
	if tenantID == "" {
		tenantID = tn.tenantID
	}

	requestID := tn.nextRequestID()
	msg := NetworkMessage{
		Type:      MsgTypeFetchRawRequest,
		TenantID:  tenantID,
		RequestID: requestID,
		Table:     tableName,
		Timestamp: time.Now().UnixMilli(),
	}

	start := time.Now()
	atomic.AddUint64(&tn.stats.fetchRequests, 1)

	responseCh := make(chan fetchRawResponseLite, tn.config.FetchResponseBuffer)
	overflowCh := make(chan struct{}, 1)
	tn.mu.Lock()
	tn.responseChannels[requestID] = pendingResponse{
		peerID:     sourceNodeID,
		fetchCh:    responseCh,
		overflowCh: overflowCh,
	}
	tn.mu.Unlock()

	defer func() {
		tn.mu.Lock()
		delete(tn.responseChannels, requestID)
		tn.mu.Unlock()
	}()

	if err := tn.sendValue(sourceNodeID, msg); err != nil {
		atomic.AddUint64(&tn.stats.fetchRequestSendErrors, 1)
		return nil, err
	}

	rows, err := collectFetchRawResponsesLite(responseCh, overflowCh, timeout, tn.config.FetchResponseIdleTimeout)
	atomic.AddUint64(&tn.stats.fetchRowsReceived, uint64(len(rows)))

	if err == nil {
		atomic.AddUint64(&tn.stats.fetchSuccess, 1)
		stdlog.Printf("[TenantNetwork:%s] fetch raw done: peer=%s, table=%s, request_id=%s, rows=%d, duration=%s",
			tenantID, shortPeerID(sourceNodeID), tableName, requestID, len(rows), time.Since(start))
		return rows, nil
	}

	switch {
	case errors.Is(err, ErrTimeoutWaitingResponseCompletion):
		atomic.AddUint64(&tn.stats.fetchPartialTimeouts, 1)
	case errors.Is(err, ErrTimeoutWaitingResponse):
		atomic.AddUint64(&tn.stats.fetchTimeouts, 1)
	case errors.Is(err, ErrResponseOverflow):
		atomic.AddUint64(&tn.stats.fetchOverflows, 1)
	case errors.Is(err, ErrPeerDisconnectedBeforeResponse):
		atomic.AddUint64(&tn.stats.fetchPeerDisconnected, 1)
	case errors.Is(err, ErrResponseChannelClosed):
		atomic.AddUint64(&tn.stats.fetchChannelClosed, 1)
	}

	stdlog.Printf("[TenantNetwork:%s] fetch raw failed: peer=%s, table=%s, request_id=%s, rows=%d, duration=%s, err=%v",
		tenantID, shortPeerID(sourceNodeID), tableName, requestID, len(rows), time.Since(start), err)
	return rows, err
}

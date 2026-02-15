package sync

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

func TestTenantNetworkHandleReceive_RequestIDWithoutWaiterFallsThrough(t *testing.T) {
	tn := &TenantNetwork{
		tenantID:         "tenant-1",
		peerHandlers:     make(map[string]PeerMessageHandler),
		broadcastHandler: PeerMessageHandler{},
		responseChannels: make(map[string]pendingResponse),
	}

	called := false
	tn.SetBroadcastHandler(PeerMessageHandler{OnReceive: func(peerID string, msg NetworkMessage) {
		called = true
		if msg.Type != MsgTypeFetchRawRequest {
			t.Fatalf("unexpected message type: %s", msg.Type)
		}
	}})

	payload, err := msgpack.Marshal(&NetworkMessage{
		Type:      MsgTypeFetchRawRequest,
		RequestID: "req-1",
		Table:     "users",
	})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	tn.handleReceive("peer-1", payload)
	if !called {
		t.Fatal("message should fall through to broadcast handler when no waiter exists")
	}
}

func TestTenantNetworkHandleReceive_ResponseRoutesToWaiter(t *testing.T) {
	tn := &TenantNetwork{
		tenantID:         "tenant-1",
		peerHandlers:     make(map[string]PeerMessageHandler),
		broadcastHandler: PeerMessageHandler{},
		responseChannels: make(map[string]pendingResponse),
	}

	responseCh := make(chan NetworkMessage, 1)
	tn.responseChannels["req-1"] = pendingResponse{peerID: "peer-1", ch: responseCh}

	broadcastCalled := false
	tn.SetBroadcastHandler(PeerMessageHandler{OnReceive: func(peerID string, msg NetworkMessage) {
		broadcastCalled = true
	}})

	payload, err := msgpack.Marshal(&NetworkMessage{
		Type:      MsgTypeFetchRawResponse,
		RequestID: "req-1",
		Key:       "k1",
		RawData:   []byte("v1"),
	})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	tn.handleReceive("peer-1", payload)

	select {
	case got := <-responseCh:
		if got.Key != "k1" {
			t.Fatalf("unexpected response key: %s", got.Key)
		}
	default:
		t.Fatal("response should be routed into waiter channel")
	}

	if broadcastCalled {
		t.Fatal("response message should not be forwarded to broadcast handler")
	}
}

func TestTenantNetworkHandleReceive_ResponseRoutesToFetchWaiter(t *testing.T) {
	tn := &TenantNetwork{
		tenantID:         "tenant-1",
		peerHandlers:     make(map[string]PeerMessageHandler),
		broadcastHandler: PeerMessageHandler{},
		responseChannels: make(map[string]pendingResponse),
	}

	responseCh := make(chan fetchRawResponseLite, 1)
	tn.responseChannels["req-1"] = pendingResponse{peerID: "peer-1", fetchCh: responseCh}

	payload, err := msgpack.Marshal(&NetworkMessage{
		Type:      MsgTypeFetchRawResponse,
		RequestID: "req-1",
		Key:       "k1",
		RawData:   []byte("v1"),
	})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	tn.handleReceive("peer-1", payload)

	select {
	case got := <-responseCh:
		if got.Type != MsgTypeFetchRawResponse {
			t.Fatalf("unexpected response type: %s", got.Type)
		}
		if got.Key != "k1" {
			t.Fatalf("unexpected response key: %s", got.Key)
		}
		if string(got.RawData) != "v1" {
			t.Fatalf("unexpected raw data: %s", string(got.RawData))
		}
	default:
		t.Fatal("response should be routed into fetch waiter channel")
	}
}

func TestTenantNetworkHandleReceive_ResponseFromUnexpectedPeerIgnored(t *testing.T) {
	tn := &TenantNetwork{
		tenantID:         "tenant-1",
		peerHandlers:     make(map[string]PeerMessageHandler),
		broadcastHandler: PeerMessageHandler{},
		responseChannels: make(map[string]pendingResponse),
	}

	responseCh := make(chan NetworkMessage, 1)
	tn.responseChannels["req-1"] = pendingResponse{peerID: "peer-expected", ch: responseCh}

	payload, err := msgpack.Marshal(&NetworkMessage{
		Type:      MsgTypeFetchRawResponse,
		RequestID: "req-1",
		Key:       "k1",
		RawData:   []byte("v1"),
	})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	tn.handleReceive("peer-unexpected", payload)

	select {
	case msg := <-responseCh:
		t.Fatalf("unexpected routed response: %+v", msg)
	default:
	}
}

func TestCollectFetchRawResponses_WithDoneMarker(t *testing.T) {
	ch := make(chan NetworkMessage, 4)
	ch <- NetworkMessage{
		Type:    MsgTypeFetchRawResponse,
		Key:     "k1",
		RawData: []byte("v1"),
	}
	ch <- NetworkMessage{
		Type:    MsgTypeFetchRawResponse,
		Key:     "k2",
		RawData: []byte("v2"),
	}
	ch <- NetworkMessage{
		Type: MsgTypeFetchRawResponse,
		Key:  fetchRawResponseDoneKey,
	}

	rows, err := collectFetchRawResponses(ch, nil, 2*time.Second, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("collect failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Key != "k1" || string(rows[0].Data) != "v1" {
		t.Fatalf("unexpected first row: %+v", rows[0])
	}
	if rows[1].Key != "k2" || string(rows[1].Data) != "v2" {
		t.Fatalf("unexpected second row: %+v", rows[1])
	}
}

func TestCollectFetchRawResponses_IdleFallbackWithoutDoneMarker(t *testing.T) {
	ch := make(chan NetworkMessage, 1)
	ch <- NetworkMessage{
		Type:    MsgTypeFetchRawResponse,
		Key:     "k1",
		RawData: []byte("v1"),
	}

	start := time.Now()
	rows, err := collectFetchRawResponses(ch, nil, 2*time.Second, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("collect failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].Key != "k1" || string(rows[0].Data) != "v1" {
		t.Fatalf("unexpected row: %+v", rows[0])
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("idle fallback took too long: %v", elapsed)
	}
}

func TestCollectFetchRawResponses_TimeoutWithoutRows(t *testing.T) {
	ch := make(chan NetworkMessage)
	_, err := collectFetchRawResponses(ch, nil, 50*time.Millisecond, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, ErrTimeoutWaitingResponse) {
		t.Fatalf("expected timeout classification, got: %v", err)
	}
}

func TestCollectFetchRawResponses_TimeoutWithPartialRowsReturnsError(t *testing.T) {
	ch := make(chan NetworkMessage, 1)
	ch <- NetworkMessage{
		Type:    MsgTypeFetchRawResponse,
		Key:     "k1",
		RawData: []byte("v1"),
	}

	rows, err := collectFetchRawResponses(ch, nil, 50*time.Millisecond, 500*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error for incomplete response")
	}
	if !errors.Is(err, ErrTimeoutWaitingResponseCompletion) {
		t.Fatalf("expected partial-timeout classification, got: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected collected partial rows, got %d", len(rows))
	}
}

func TestCollectFetchRawResponses_Overflow(t *testing.T) {
	ch := make(chan NetworkMessage)
	overflowCh := make(chan struct{}, 1)
	overflowCh <- struct{}{}

	_, err := collectFetchRawResponses(ch, overflowCh, time.Second, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected overflow error")
	}
	if !errors.Is(err, ErrResponseOverflow) {
		t.Fatalf("expected overflow classification, got: %v", err)
	}
}

func TestCollectFetchRawResponses_PeerDisconnected(t *testing.T) {
	ch := make(chan NetworkMessage, 1)
	ch <- NetworkMessage{Type: internalMsgTypePeerDisconnected}

	_, err := collectFetchRawResponses(ch, nil, time.Second, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected peer disconnected error")
	}
	if !errors.Is(err, ErrPeerDisconnectedBeforeResponse) {
		t.Fatalf("expected peer-disconnected classification, got: %v", err)
	}
}

func TestCollectFetchRawResponsesLite_WithDoneMarker(t *testing.T) {
	ch := make(chan fetchRawResponseLite, 4)
	ch <- fetchRawResponseLite{
		Type:    MsgTypeFetchRawResponse,
		Key:     "k1",
		RawData: []byte("v1"),
	}
	ch <- fetchRawResponseLite{
		Type:    MsgTypeFetchRawResponse,
		Key:     "k2",
		RawData: []byte("v2"),
	}
	ch <- fetchRawResponseLite{
		Type: MsgTypeFetchRawResponse,
		Key:  fetchRawResponseDoneKey,
	}

	rows, err := collectFetchRawResponsesLite(ch, nil, 2*time.Second, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("collect failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Key != "k1" || string(rows[0].Data) != "v1" {
		t.Fatalf("unexpected first row: %+v", rows[0])
	}
	if rows[1].Key != "k2" || string(rows[1].Data) != "v2" {
		t.Fatalf("unexpected second row: %+v", rows[1])
	}
}

func TestCollectFetchRawResponsesLite_PeerDisconnected(t *testing.T) {
	ch := make(chan fetchRawResponseLite, 1)
	ch <- fetchRawResponseLite{Type: internalMsgTypePeerDisconnected}

	_, err := collectFetchRawResponsesLite(ch, nil, time.Second, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected peer disconnected error")
	}
	if !errors.Is(err, ErrPeerDisconnectedBeforeResponse) {
		t.Fatalf("expected peer-disconnected classification, got: %v", err)
	}
}

func TestTenantNetworkNextRequestID_SequentialWithoutTunnel(t *testing.T) {
	tn := &TenantNetwork{}
	first := tn.nextRequestID()
	second := tn.nextRequestID()
	if first == second {
		t.Fatalf("request IDs should be unique, got first=%s second=%s", first, second)
	}
	if first != "1" || second != "2" {
		t.Fatalf("unexpected fallback request IDs: first=%s second=%s", first, second)
	}
}

func TestTenantNetworkRemovePeerHandler_NotifiesPendingWaiter(t *testing.T) {
	tn := &TenantNetwork{
		tenantID:         "tenant-1",
		peerHandlers:     make(map[string]PeerMessageHandler),
		broadcastHandler: PeerMessageHandler{},
		responseChannels: make(map[string]pendingResponse),
	}

	ch := make(chan NetworkMessage, 1)
	tn.responseChannels["req-1"] = pendingResponse{
		peerID: "peer-1",
		ch:     ch,
	}

	tn.RemovePeerHandler("peer-1")

	if _, exists := tn.responseChannels["req-1"]; exists {
		t.Fatal("pending response should be removed for peer")
	}

	select {
	case msg := <-ch:
		if msg.Type != internalMsgTypePeerDisconnected {
			t.Fatalf("expected disconnect notification, got %+v", msg)
		}
	default:
		t.Fatal("pending waiter should be notified")
	}
}

func TestTenantNetworkRemovePeerHandler_NotifiesPendingFetchWaiter(t *testing.T) {
	tn := &TenantNetwork{
		tenantID:         "tenant-1",
		peerHandlers:     make(map[string]PeerMessageHandler),
		broadcastHandler: PeerMessageHandler{},
		responseChannels: make(map[string]pendingResponse),
	}

	ch := make(chan fetchRawResponseLite, 1)
	tn.responseChannels["req-1"] = pendingResponse{
		peerID:  "peer-1",
		fetchCh: ch,
	}

	tn.RemovePeerHandler("peer-1")

	if _, exists := tn.responseChannels["req-1"]; exists {
		t.Fatal("pending response should be removed for peer")
	}

	select {
	case msg := <-ch:
		if msg.Type != internalMsgTypePeerDisconnected {
			t.Fatalf("expected disconnect notification, got %+v", msg)
		}
	default:
		t.Fatal("pending fetch waiter should be notified")
	}
}

func TestNormalizeTenetConfig_Defaults(t *testing.T) {
	cfg := normalizeTenetConfig(&TenetConfig{
		Password: "p",
	})

	if cfg.FetchResponseBuffer != defaultFetchResponseBuffer {
		t.Fatalf("unexpected default buffer size: %d", cfg.FetchResponseBuffer)
	}
	if cfg.FetchResponseIdleTimeout != defaultFetchResponseIdleTimeout {
		t.Fatalf("unexpected default idle timeout: %v", cfg.FetchResponseIdleTimeout)
	}
}

func TestNormalizeTenetConfig_KeepCustomValues(t *testing.T) {
	cfg := normalizeTenetConfig(&TenetConfig{
		Password:                 "p",
		FetchResponseBuffer:      1024,
		FetchResponseIdleTimeout: 3 * time.Second,
	})

	if cfg.FetchResponseBuffer != 1024 {
		t.Fatalf("unexpected custom buffer size: %d", cfg.FetchResponseBuffer)
	}
	if cfg.FetchResponseIdleTimeout != 3*time.Second {
		t.Fatalf("unexpected custom idle timeout: %v", cfg.FetchResponseIdleTimeout)
	}
}

func TestTenantNetworkStats_Snapshot(t *testing.T) {
	tn := &TenantNetwork{
		tenantID:         "tenant-1",
		peerHandlers:     make(map[string]PeerMessageHandler),
		broadcastHandler: PeerMessageHandler{},
		responseChannels: make(map[string]pendingResponse),
	}

	ch := make(chan NetworkMessage, 1)
	tn.responseChannels["req-1"] = pendingResponse{peerID: "peer-1", ch: ch}

	payload, err := msgpack.Marshal(&NetworkMessage{
		Type:      MsgTypeFetchRawResponse,
		RequestID: "req-1",
		Key:       "k1",
		RawData:   []byte("v1"),
	})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	tn.handleReceive("peer-1", payload)
	tn.handleReceive("peer-2", payload) // unexpected peer for same requestID

	stats := tn.Stats()
	if stats.RoutedResponses != 1 {
		t.Fatalf("expected routed responses = 1, got %d", stats.RoutedResponses)
	}
	if stats.UnexpectedPeerResponses != 1 {
		t.Fatalf("expected unexpected-peer responses = 1, got %d", stats.UnexpectedPeerResponses)
	}
	if stats.InFlightRequests != 1 {
		t.Fatalf("expected in-flight requests = 1, got %d", stats.InFlightRequests)
	}
}

func BenchmarkTenantNetworkNextRequestID_NoTunnel(b *testing.B) {
	tn := &TenantNetwork{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := tn.nextRequestID()
		if _, err := strconv.ParseUint(id, 10, 64); err != nil {
			b.Fatalf("invalid request ID %q: %v", id, err)
		}
	}
}

func BenchmarkTenantNetworkNextRequestID_WithLocalIDCache(b *testing.B) {
	tn := &TenantNetwork{}
	tn.localNodeID.Store("node-bench")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := tn.nextRequestID()
		if _, err := strconv.ParseUint(id, 10, 64); err != nil {
			b.Fatalf("invalid request ID %q: %v", id, err)
		}
	}
}

func BenchmarkTenantNetworkHandleReceive_ResponseRouting(b *testing.B) {
	tn := &TenantNetwork{
		tenantID:         "tenant-1",
		peerHandlers:     make(map[string]PeerMessageHandler),
		broadcastHandler: PeerMessageHandler{},
		responseChannels: make(map[string]pendingResponse),
	}
	ch := make(chan NetworkMessage, 1)
	tn.responseChannels["req-1"] = pendingResponse{peerID: "peer-1", ch: ch}

	payload, err := msgpack.Marshal(&NetworkMessage{
		Type:      MsgTypeFetchRawResponse,
		RequestID: "req-1",
		Key:       "k1",
		RawData:   []byte("v1"),
	})
	if err != nil {
		b.Fatalf("marshal failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tn.handleReceive("peer-1", payload)
		<-ch
	}
}

func BenchmarkTenantNetworkHandleReceive_ResponseRouting_FetchLiteChannel(b *testing.B) {
	tn := &TenantNetwork{
		tenantID:         "tenant-1",
		peerHandlers:     make(map[string]PeerMessageHandler),
		broadcastHandler: PeerMessageHandler{},
		responseChannels: make(map[string]pendingResponse),
	}
	ch := make(chan fetchRawResponseLite, 1)
	tn.responseChannels["req-1"] = pendingResponse{peerID: "peer-1", fetchCh: ch}

	payload, err := msgpack.Marshal(&NetworkMessage{
		Type:      MsgTypeFetchRawResponse,
		RequestID: "req-1",
		Key:       "k1",
		RawData:   []byte("v1"),
	})
	if err != nil {
		b.Fatalf("marshal failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tn.handleReceive("peer-1", payload)
		<-ch
	}
}

func BenchmarkDecodeNetworkMessageFull_FetchRawResponse(b *testing.B) {
	payload, err := msgpack.Marshal(&NetworkMessage{
		Type:      MsgTypeFetchRawResponse,
		RequestID: "req-1",
		Table:     "users",
		Key:       "k1",
		RawData:   []byte("v1"),
		Timestamp: 123,
	})
	if err != nil {
		b.Fatalf("marshal failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var msg NetworkMessage
		if err := msgpack.Unmarshal(payload, &msg); err != nil {
			b.Fatalf("unmarshal failed: %v", err)
		}
	}
}

func BenchmarkDecodeFetchRawResponseLite(b *testing.B) {
	type fetchRawResponseLite struct {
		Type      string `msgpack:"type"`
		RequestID string `msgpack:"request_id"`
		Table     string `msgpack:"table,omitempty"`
		Key       string `msgpack:"key,omitempty"`
		RawData   []byte `msgpack:"raw_data,omitempty"`
		Timestamp int64  `msgpack:"timestamp"`
		Clock     int64  `msgpack:"clock,omitempty"`
	}

	payload, err := msgpack.Marshal(&NetworkMessage{
		Type:      MsgTypeFetchRawResponse,
		RequestID: "req-1",
		Table:     "users",
		Key:       "k1",
		RawData:   []byte("v1"),
		Timestamp: 123,
	})
	if err != nil {
		b.Fatalf("marshal failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var msg fetchRawResponseLite
		if err := msgpack.Unmarshal(payload, &msg); err != nil {
			b.Fatalf("unmarshal failed: %v", err)
		}
	}
}

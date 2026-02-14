package sync

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"
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

	payload, err := json.Marshal(&NetworkMessage{
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

	payload, err := json.Marshal(&NetworkMessage{
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

	rows, err := collectFetchRawResponses(ch, time.Second)
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
	rows, err := collectFetchRawResponses(ch, time.Second)
	if err != nil {
		t.Fatalf("collect failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].Key != "k1" || string(rows[0].Data) != "v1" {
		t.Fatalf("unexpected row: %+v", rows[0])
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("idle fallback took too long: %v", elapsed)
	}
}

func TestCollectFetchRawResponses_TimeoutWithoutRows(t *testing.T) {
	ch := make(chan NetworkMessage)
	_, err := collectFetchRawResponses(ch, 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestCollectFetchRawResponses_PeerDisconnected(t *testing.T) {
	ch := make(chan NetworkMessage, 1)
	ch <- NetworkMessage{Type: internalMsgTypePeerDisconnected}

	_, err := collectFetchRawResponses(ch, time.Second)
	if err == nil {
		t.Fatal("expected peer disconnected error")
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

	payload, err := json.Marshal(&NetworkMessage{
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
	payload, err := json.Marshal(&NetworkMessage{
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
		if err := json.Unmarshal(payload, &msg); err != nil {
			b.Fatalf("unmarshal failed: %v", err)
		}
	}
}

func BenchmarkDecodeFetchRawResponseLite(b *testing.B) {
	type fetchRawResponseLite struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
		Table     string `json:"table,omitempty"`
		Key       string `json:"key,omitempty"`
		RawData   []byte `json:"raw_data,omitempty"`
		Timestamp int64  `json:"timestamp"`
		Clock     int64  `json:"clock,omitempty"`
	}

	payload, err := json.Marshal(&NetworkMessage{
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
		if err := json.Unmarshal(payload, &msg); err != nil {
			b.Fatalf("unmarshal failed: %v", err)
		}
	}
}

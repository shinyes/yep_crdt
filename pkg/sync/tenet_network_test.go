package sync

import (
	"encoding/json"
	"testing"
)

func TestTenantNetworkHandleReceive_RequestIDWithoutWaiterFallsThrough(t *testing.T) {
	tn := &TenantNetwork{
		tenantID:         "tenant-1",
		peerHandlers:     make(map[string]PeerMessageHandler),
		broadcastHandler: PeerMessageHandler{},
		responseChannels: make(map[string]pendingResponse),
	}

	called := false
	tn.SetBroadcastHandler(PeerMessageHandler{OnReceive: func(peerID string, msg *NetworkMessage) {
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

	responseCh := make(chan *NetworkMessage, 1)
	tn.responseChannels["req-1"] = pendingResponse{peerID: "peer-1", ch: responseCh}

	broadcastCalled := false
	tn.SetBroadcastHandler(PeerMessageHandler{OnReceive: func(peerID string, msg *NetworkMessage) {
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

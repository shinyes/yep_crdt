package sync

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestRunManualGCPreparePhase_MinSafeTimestamp(t *testing.T) {
	request := func(peerID string, msg *NetworkMessage, timeout time.Duration) (*NetworkMessage, error) {
		if msg == nil || msg.Type != MsgTypeGCPrepare {
			t.Fatalf("unexpected prepare message: %#v", msg)
		}
		switch peerID {
		case "peer-a":
			return &NetworkMessage{
				Type:          MsgTypeGCPrepareAck,
				Success:       true,
				SafeTimestamp: 95,
			}, nil
		case "peer-b":
			return &NetworkMessage{
				Type:          MsgTypeGCPrepareAck,
				Success:       true,
				SafeTimestamp: 80,
			}, nil
		default:
			return nil, fmt.Errorf("unexpected peer: %s", peerID)
		}
	}

	safeTimestamp, preparedPeers, err := runManualGCPreparePhase(
		100,
		[]string{"peer-a", "peer-b"},
		time.Second,
		request,
	)
	if err != nil {
		t.Fatalf("runManualGCPreparePhase failed: %v", err)
	}
	if safeTimestamp != 80 {
		t.Fatalf("unexpected safe timestamp: want=80 got=%d", safeTimestamp)
	}
	if len(preparedPeers) != 2 {
		t.Fatalf("unexpected prepared peers count: want=2 got=%d", len(preparedPeers))
	}
}

func TestRunManualGCPreparePhase_Rejected(t *testing.T) {
	request := func(peerID string, msg *NetworkMessage, timeout time.Duration) (*NetworkMessage, error) {
		return &NetworkMessage{
			Type:    MsgTypeGCPrepareAck,
			Success: false,
			Error:   "not ready",
		}, nil
	}

	_, _, err := runManualGCPreparePhase(100, []string{"peer-a"}, time.Second, request)
	if err == nil {
		t.Fatal("expected prepare rejection error")
	}
	if !strings.Contains(err.Error(), "prepare rejected") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunManualGCCommitPhase_Rejected(t *testing.T) {
	request := func(peerID string, msg *NetworkMessage, timeout time.Duration) (*NetworkMessage, error) {
		if msg == nil || msg.Type != MsgTypeGCCommit {
			t.Fatalf("unexpected commit message: %#v", msg)
		}
		if peerID == "peer-a" {
			return &NetworkMessage{
				Type:    MsgTypeGCCommitAck,
				Success: true,
			}, nil
		}
		return &NetworkMessage{
			Type:    MsgTypeGCCommitAck,
			Success: false,
			Error:   "safe timestamp moved",
		}, nil
	}

	committedPeers, err := runManualGCCommitPhase(
		[]string{"peer-a", "peer-b"},
		123,
		time.Second,
		request,
	)
	if err == nil {
		t.Fatal("expected commit rejection error")
	}
	if len(committedPeers) != 1 || committedPeers[0] != "peer-a" {
		t.Fatalf("unexpected committed peers: %#v", committedPeers)
	}
}

func TestNodeManager_HandleManualGCPrepare_SendsAck(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	nm := NewNodeManager(db.Open(s, "test-node"), "node-1")
	net := &manualGCRecordNetwork{}
	nm.RegisterNetwork(net)

	nm.HandleManualGCPrepare("peer-1", NetworkMessage{
		Type:      MsgTypeGCPrepare,
		RequestID: "req-prepare",
	})

	sent := net.LastSent()
	if sent == nil {
		t.Fatal("expected prepare ack to be sent")
	}
	if sent.Type != MsgTypeGCPrepareAck {
		t.Fatalf("unexpected response type: %s", sent.Type)
	}
	if sent.RequestID != "req-prepare" {
		t.Fatalf("unexpected request id: %s", sent.RequestID)
	}
	if !sent.Success {
		t.Fatalf("expected success=true, got false: err=%s", sent.Error)
	}
}

func TestNodeManager_HandleManualGCCommit_RejectsUnsafeTimestamp(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	nm := NewNodeManager(db.Open(s, "test-node"), "node-1")
	net := &manualGCRecordNetwork{}
	nm.RegisterNetwork(net)

	unsafeTimestamp := nm.CalculateSafeTimestamp() + 1_000_000
	nm.HandleManualGCCommit("peer-1", NetworkMessage{
		Type:          MsgTypeGCCommit,
		RequestID:     "req-commit",
		SafeTimestamp: unsafeTimestamp,
	})

	sent := net.LastSent()
	if sent == nil {
		t.Fatal("expected commit ack to be sent")
	}
	if sent.Type != MsgTypeGCCommitAck {
		t.Fatalf("unexpected response type: %s", sent.Type)
	}
	if sent.Success {
		t.Fatalf("expected commit rejection, got success")
	}
	if !strings.Contains(sent.Error, "exceeds local safe timestamp") {
		t.Fatalf("unexpected rejection reason: %s", sent.Error)
	}
}

func TestNodeManager_HandleManualGCCommit_Success(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	nm := NewNodeManager(db.Open(s, "test-node"), "node-1")
	net := &manualGCRecordNetwork{}
	nm.RegisterNetwork(net)

	safeTimestamp := nm.CalculateSafeTimestamp()
	nm.HandleManualGCCommit("peer-1", NetworkMessage{
		Type:          MsgTypeGCCommit,
		RequestID:     "req-commit-ok",
		SafeTimestamp: safeTimestamp,
	})

	sent := net.LastSent()
	if sent == nil {
		t.Fatal("expected commit ack to be sent")
	}
	if sent.Type != MsgTypeGCCommitAck {
		t.Fatalf("unexpected response type: %s", sent.Type)
	}
	if !sent.Success {
		t.Fatalf("expected commit success, got err=%s", sent.Error)
	}
}

func TestMultiEngine_ManualGC_LocalOnly(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := db.Open(s, "tenant-1")
	nm := NewNodeManager(database, "node-1")
	rt := &tenantRuntime{
		tenantID: "tenant-1",
		db:       database,
		nodeMgr:  nm,
	}
	engine := &MultiEngine{
		tenants: map[string]*tenantRuntime{
			"tenant-1": rt,
		},
	}

	result, err := engine.ManualGC("tenant-1", 0)
	if err != nil {
		t.Fatalf("manual gc failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected manual gc result")
	}
	if result.TenantID != "tenant-1" {
		t.Fatalf("unexpected tenant id: %s", result.TenantID)
	}
	if result.LocalResult == nil {
		t.Fatal("expected local gc result")
	}
	if len(result.PreparedPeers) != 0 {
		t.Fatalf("unexpected prepared peers: %#v", result.PreparedPeers)
	}
	if len(result.CommittedPeers) != 0 {
		t.Fatalf("unexpected committed peers: %#v", result.CommittedPeers)
	}
}

type manualGCRecordNetwork struct {
	mu   sync.Mutex
	sent *NetworkMessage
}

func (n *manualGCRecordNetwork) SendHeartbeat(targetNodeID string, clock int64) error {
	return nil
}

func (n *manualGCRecordNetwork) BroadcastHeartbeat(clock int64) error {
	return nil
}

func (n *manualGCRecordNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	return nil
}

func (n *manualGCRecordNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	return nil
}

func (n *manualGCRecordNetwork) SendRawDelta(targetNodeID string, table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return nil
}

func (n *manualGCRecordNetwork) BroadcastRawDelta(table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return nil
}

func (n *manualGCRecordNetwork) SendMessage(targetNodeID string, msg *NetworkMessage) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if msg == nil {
		n.sent = nil
		return nil
	}
	cloned := *msg
	n.sent = &cloned
	return nil
}

func (n *manualGCRecordNetwork) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	return nil, nil
}

func (n *manualGCRecordNetwork) LastSent() *NetworkMessage {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sent == nil {
		return nil
	}
	cloned := *n.sent
	return &cloned
}

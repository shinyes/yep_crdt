package sync

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestNodeManager_Basic(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "test-node")

	nm := NewNodeManager(database, "node-1",
		WithHeartbeatInterval(1*time.Second),
		WithTimeoutThreshold(3*time.Second),
		WithClockThreshold(1000),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nm.Start(ctx)

	node2ID := "node-2"
	nm.OnPeerConnected(node2ID)
	nm.OnHeartbeat(node2ID, 1000, 0)

	time.Sleep(2 * time.Second)
	if !nm.IsNodeOnline(node2ID) {
		t.Error("node-2 should be online")
	}

	nm.OnPeerDisconnected(node2ID)
	if nm.IsNodeOnline(node2ID) {
		t.Error("node-2 should be offline after disconnect")
	}
}

func TestNodeManager_BroadcastHeartbeat_NoNetworkReturnsError(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "test-node")
	nm := NewNodeManager(database, "node-1")

	err = nm.BroadcastHeartbeat(database.Clock().Now())
	if !errors.Is(err, ErrNoNetwork) {
		t.Fatalf("expected ErrNoNetwork, got: %v", err)
	}
}

func TestNodeManager_BroadcastRawData_NoNetworkReturnsError(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "test-node")
	nm := NewNodeManager(database, "node-1")

	err = nm.BroadcastRawData("users", "k1", []byte("v1"), database.Clock().Now())
	if !errors.Is(err, ErrNoNetwork) {
		t.Fatalf("expected ErrNoNetwork, got: %v", err)
	}
}

func TestDefaultNetwork_NoOpRemoved_ReturnsErrNoNetwork(t *testing.T) {
	network := NewDefaultNetwork()

	if err := network.SendHeartbeat("node-2", 1); !errors.Is(err, ErrNoNetwork) {
		t.Fatalf("expected ErrNoNetwork from SendHeartbeat, got: %v", err)
	}
	if _, err := network.FetchRawTableData("node-2", "users"); !errors.Is(err, ErrNoNetwork) {
		t.Fatalf("expected ErrNoNetwork from FetchRawTableData, got: %v", err)
	}
}

func TestNodeManager_SafeTimestamp(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "test-node")
	nm := NewNodeManager(database, "node-1")

	baseTime := database.Clock().Now()
	safetyOffset := defaultSafeTimestampOffset.Milliseconds()

	safeTs := nm.CalculateSafeTimestamp()
	expectedSafeTs := baseTime - safetyOffset

	timeDiff := safeTs - expectedSafeTs
	if timeDiff < -100 || timeDiff > 100 {
		t.Errorf("expected SafeTimestamp close to local-30s: want=%d got=%d diff=%d", expectedSafeTs, safeTs, timeDiff)
	}

	node2Clock := baseTime - 10000
	nm.OnHeartbeat("node-2", node2Clock, 0)

	safeTs = nm.CalculateSafeTimestamp()
	expectedSafeTs = node2Clock - safetyOffset
	if safeTs != expectedSafeTs {
		t.Errorf("expected SafeTimestamp to use node-2 clock: want=%d got=%d", expectedSafeTs, safeTs)
	}

	node3Clock := baseTime - 20000
	nm.OnHeartbeat("node-3", node3Clock, 0)

	safeTs = nm.CalculateSafeTimestamp()
	expectedSafeTs = node3Clock - safetyOffset
	if safeTs != expectedSafeTs {
		t.Errorf("expected SafeTimestamp to use node-3 clock: want=%d got=%d", expectedSafeTs, safeTs)
	}

	// Offline nodes still constrain safe timestamp for GC safety.
	nm.OnPeerDisconnected("node-3")
	safeTs = nm.CalculateSafeTimestamp()
	expectedSafeTs = node3Clock - safetyOffset
	if safeTs != expectedSafeTs {
		t.Errorf("expected offline node clock to still constrain SafeTimestamp: want=%d got=%d", expectedSafeTs, safeTs)
	}
}

func TestNodeManager_Rejoin(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "test-node")

	nm := NewNodeManager(database, "node-1", WithClockThreshold(8000))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nm.Start(ctx)

	localClock := int64(100000)
	database.Clock().Update(localClock)

	node2FirstClock := int64(95000)
	nm.OnHeartbeat("node-2", node2FirstClock, 0)
	if !nm.IsNodeOnline("node-2") {
		t.Error("node-2 should be online")
	}

	time.Sleep(1 * time.Second)

	advancedLocalClock := int64(110000)
	database.Clock().Update(advancedLocalClock)

	node2RejoinClock := int64(95000)
	clockDiff := advancedLocalClock - node2RejoinClock
	nm.OnHeartbeat("node-2", node2RejoinClock, 0)

	if clockDiff > 8000 {
		t.Logf("clock gap is large, should trigger performFullSync")
	} else {
		t.Logf("clock gap within threshold, should trigger performClockReset")
	}

	if !nm.IsNodeOnline("node-2") {
		t.Error("node-2 should be online after rejoin handling")
	}
}

func TestNodeManager_Rejoin_LongOfflineRequiresFullSync(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "test-node")
	if err := database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}); err != nil {
		t.Fatalf("define table failed: %v", err)
	}

	const hugeClockThreshold = int64(1 << 60)
	nm := NewNodeManager(database, "node-1",
		WithTimeoutThreshold(40*time.Millisecond),
		WithClockThreshold(hugeClockThreshold),
	)
	net := &rejoinFullSyncNetwork{}
	nm.RegisterNetwork(net)

	peerClock := database.Clock().Now()
	nm.OnHeartbeat("node-2", peerClock, 0)
	nm.OnPeerDisconnected("node-2")

	time.Sleep(70 * time.Millisecond)
	nm.OnPeerConnected("node-2")

	if got := net.FetchCalls(); got == 0 {
		t.Fatal("expected full sync fetch for long-offline rejoin")
	}
}

func TestNodeManager_Rejoin_ShortOfflineNoForcedFullSync(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "test-node")
	if err := database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}); err != nil {
		t.Fatalf("define table failed: %v", err)
	}

	const hugeClockThreshold = int64(1 << 60)
	nm := NewNodeManager(database, "node-1",
		WithTimeoutThreshold(200*time.Millisecond),
		WithClockThreshold(hugeClockThreshold),
	)
	net := &rejoinFullSyncNetwork{}
	nm.RegisterNetwork(net)

	peerClock := database.Clock().Now()
	nm.OnHeartbeat("node-2", peerClock, 0)
	nm.OnPeerDisconnected("node-2")

	time.Sleep(30 * time.Millisecond)
	nm.OnPeerConnected("node-2")

	if got := net.FetchCalls(); got != 0 {
		t.Fatalf("expected no forced full sync for short offline rejoin, got fetch calls=%d", got)
	}
}

func TestNodeManager_Rejoin_IncrementalOnlyMode_NoFullSync(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "test-node")
	if err := database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}); err != nil {
		t.Fatalf("define table failed: %v", err)
	}

	// Incremental-only mode:
	// - TimeoutThreshold<=0 disables long-offline full sync gate
	// - ClockThreshold<=0 disables clock-gap full sync gate
	nm := NewNodeManager(database, "node-1",
		WithTimeoutThreshold(0),
		WithClockThreshold(0),
	)
	net := &rejoinFullSyncNetwork{}
	nm.RegisterNetwork(net)

	peerClock := database.Clock().Now()
	nm.OnHeartbeat("node-2", peerClock, 0)
	nm.OnPeerDisconnected("node-2")

	time.Sleep(120 * time.Millisecond)
	// stale clock to simulate large clock gap, still should not trigger full sync in incremental-only mode
	nm.OnHeartbeat("node-2", peerClock-1_000_000, 0)

	if got := net.FetchCalls(); got != 0 {
		t.Fatalf("expected no full sync in incremental-only mode, got fetch calls=%d", got)
	}
}

func TestNodeManager_GCFloorLag_TriggersFullSyncAndCatchesUp(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "test-node")
	if err := database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}); err != nil {
		t.Fatalf("define table failed: %v", err)
	}
	if err := database.SetGCFloor(100); err != nil {
		t.Fatalf("set local gc floor failed: %v", err)
	}

	nm := NewNodeManager(database, "node-1")
	net := &rejoinFullSyncNetwork{}
	nm.RegisterNetwork(net)
	nm.OnPeerConnected("node-2")

	nm.ObservePeerGCFloor("node-2", 150)

	deadline := time.Now().Add(2 * time.Second)
	for {
		if net.FetchCalls() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected full sync to be triggered for gc floor lag")
		}
		time.Sleep(10 * time.Millisecond)
	}

	deadline = time.Now().Add(2 * time.Second)
	for {
		if database.GCFloor() >= 150 && nm.CanUseIncrementalWithPeer("node-2") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected catch-up done: floor=%d, can_incremental=%v",
				database.GCFloor(), nm.CanUseIncrementalWithPeer("node-2"))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNodeManager_GCFloorPeerBehind_BlocksIncrementalOnly(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "test-node")
	if err := database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}); err != nil {
		t.Fatalf("define table failed: %v", err)
	}
	if err := database.SetGCFloor(200); err != nil {
		t.Fatalf("set local gc floor failed: %v", err)
	}

	nm := NewNodeManager(database, "node-1")
	net := &rejoinFullSyncNetwork{}
	nm.RegisterNetwork(net)
	nm.OnPeerConnected("node-2")

	nm.ObservePeerGCFloor("node-2", 150)
	if nm.CanUseIncrementalWithPeer("node-2") {
		t.Fatal("expected incremental to be blocked for behind peer")
	}

	time.Sleep(100 * time.Millisecond)
	if got := net.FetchCalls(); got != 0 {
		t.Fatalf("expected no local full sync when peer is behind, got fetch calls=%d", got)
	}
}

// TestNodeManager_DataReject verifies incoming timestamp does not block merge path.
func TestNodeManager_DataReject(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "test-node")
	nm := NewNodeManager(database, "node-1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	nm.Start(ctx)

	initialClock := database.Clock().Now()
	testRawData := []byte(`{"Entries":{}}`)

	key1, _ := uuid.NewV7()
	staleDataTimestamp := initialClock - 100000
	err = nm.dataSync.OnReceiveMerge("test-table", key1.String(), testRawData, staleDataTimestamp)
	if err == nil || !strings.Contains(err.Error(), "test-table") {
		t.Fatalf("expected table-not-exist error, got: %v", err)
	}

	currentClockBeforeSend := database.Clock().Now()
	key2, _ := uuid.NewV7()
	newDataTimestamp := currentClockBeforeSend + 10000000
	err = nm.dataSync.OnReceiveMerge("test-table", key2.String(), testRawData, newDataTimestamp)
	if err == nil || !strings.Contains(err.Error(), "test-table") {
		t.Fatalf("expected table-not-exist error, got: %v", err)
	}

	updatedClock := database.Clock().Now()
	if updatedClock < newDataTimestamp {
		t.Fatalf("expected local clock to advance, before=%d after=%d target=%d", currentClockBeforeSend, updatedClock, newDataTimestamp)
	}

	key3, _ := uuid.NewV7()
	nowStaleTimestamp := currentClockBeforeSend + 100000
	err = nm.dataSync.OnReceiveMerge("test-table", key3.String(), testRawData, nowStaleTimestamp)
	if err == nil || !strings.Contains(err.Error(), "test-table") {
		t.Fatalf("expected table-not-exist error, got: %v", err)
	}
}

func TestNodeManager_DataReject_NonV7Key(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "test-node")
	nm := NewNodeManager(database, "node-1")

	invalidKey := uuid.New().String() // v4
	err = nm.dataSync.OnReceiveMerge("test-table", invalidKey, []byte(`{"Entries":{}}`), database.Clock().Now())
	if err == nil || !strings.Contains(err.Error(), "UUIDv7") {
		t.Fatalf("expected non-v7 key rejection, got: %v", err)
	}
}

type rejoinFullSyncNetwork struct {
	mu         sync.Mutex
	fetchCalls int
}

func (n *rejoinFullSyncNetwork) SendHeartbeat(targetNodeID string, clock int64) error {
	return nil
}

func (n *rejoinFullSyncNetwork) BroadcastHeartbeat(clock int64) error {
	return nil
}

func (n *rejoinFullSyncNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	return nil
}

func (n *rejoinFullSyncNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	return nil
}

func (n *rejoinFullSyncNetwork) SendRawDelta(targetNodeID string, table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return nil
}

func (n *rejoinFullSyncNetwork) BroadcastRawDelta(table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return nil
}

func (n *rejoinFullSyncNetwork) SendMessage(targetNodeID string, msg *NetworkMessage) error {
	return nil
}

func (n *rejoinFullSyncNetwork) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	n.mu.Lock()
	n.fetchCalls++
	n.mu.Unlock()
	return nil, nil
}

func (n *rejoinFullSyncNetwork) FetchCalls() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.fetchCalls
}

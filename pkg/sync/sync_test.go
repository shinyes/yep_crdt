package sync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestNodeManager_Basic(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := db.Open(s, "test-node")

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
	nm.OnHeartbeat(node2ID, 1000)

	time.Sleep(2 * time.Second)
	if !nm.IsNodeOnline(node2ID) {
		t.Error("node-2 should be online")
	}

	nm.OnPeerDisconnected(node2ID)
	if nm.IsNodeOnline(node2ID) {
		t.Error("node-2 should be offline after disconnect")
	}
}

func TestNodeManager_SafeTimestamp(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := db.Open(s, "test-node")
	nm := NewNodeManager(database, "node-1")

	baseTime := database.Clock().Now()
	safetyOffset := DefaultConfig().GCTimeOffset.Milliseconds()

	safeTs := nm.CalculateSafeTimestamp()
	expectedSafeTs := baseTime - safetyOffset

	timeDiff := safeTs - expectedSafeTs
	if timeDiff < -100 || timeDiff > 100 {
		t.Errorf("expected SafeTimestamp close to local-30s: want=%d got=%d diff=%d", expectedSafeTs, safeTs, timeDiff)
	}

	node2Clock := baseTime - 10000
	nm.OnHeartbeat("node-2", node2Clock)

	safeTs = nm.CalculateSafeTimestamp()
	expectedSafeTs = node2Clock - safetyOffset
	if safeTs != expectedSafeTs {
		t.Errorf("expected SafeTimestamp to use node-2 clock: want=%d got=%d", expectedSafeTs, safeTs)
	}

	node3Clock := baseTime - 20000
	nm.OnHeartbeat("node-3", node3Clock)

	safeTs = nm.CalculateSafeTimestamp()
	expectedSafeTs = node3Clock - safetyOffset
	if safeTs != expectedSafeTs {
		t.Errorf("expected SafeTimestamp to use node-3 clock: want=%d got=%d", expectedSafeTs, safeTs)
	}
}

func TestNodeManager_Rejoin(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := db.Open(s, "test-node")

	nm := NewNodeManager(database, "node-1", WithClockThreshold(8000))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nm.Start(ctx)

	localClock := int64(100000)
	database.Clock().Update(localClock)

	node2FirstClock := int64(95000)
	nm.OnHeartbeat("node-2", node2FirstClock)
	if !nm.IsNodeOnline("node-2") {
		t.Error("node-2 should be online")
	}

	time.Sleep(1 * time.Second)

	advancedLocalClock := int64(110000)
	database.Clock().Update(advancedLocalClock)

	node2RejoinClock := int64(95000)
	clockDiff := advancedLocalClock - node2RejoinClock
	nm.OnHeartbeat("node-2", node2RejoinClock)

	if clockDiff > 8000 {
		t.Logf("clock gap is large, should trigger performFullSync")
	} else {
		t.Logf("clock gap within threshold, should trigger performClockReset")
	}

	if !nm.IsNodeOnline("node-2") {
		t.Error("node-2 should be online after rejoin handling")
	}
}

// TestNodeManager_DataReject verifies incoming timestamp does not block merge path.
func TestNodeManager_DataReject(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := db.Open(s, "test-node")
	nm := NewNodeManager(database, "node-1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	nm.Start(ctx)

	initialClock := database.Clock().Now()
	testRawData := []byte(`{"Entries":{}}`)

	key1 := uuid.New().String()
	staleDataTimestamp := initialClock - 100000
	err = nm.dataSync.OnReceiveMerge("test-table", key1, testRawData, staleDataTimestamp)
	if err == nil || !strings.Contains(err.Error(), "test-table") {
		t.Fatalf("expected table-not-exist error, got: %v", err)
	}

	currentClockBeforeSend := database.Clock().Now()
	key2 := uuid.New().String()
	newDataTimestamp := currentClockBeforeSend + 10000000
	err = nm.dataSync.OnReceiveMerge("test-table", key2, testRawData, newDataTimestamp)
	if err == nil || !strings.Contains(err.Error(), "test-table") {
		t.Fatalf("expected table-not-exist error, got: %v", err)
	}

	updatedClock := database.Clock().Now()
	if updatedClock < newDataTimestamp {
		t.Fatalf("expected local clock to advance, before=%d after=%d target=%d", currentClockBeforeSend, updatedClock, newDataTimestamp)
	}

	key3 := uuid.New().String()
	nowStaleTimestamp := currentClockBeforeSend + 100000
	err = nm.dataSync.OnReceiveMerge("test-table", key3, testRawData, nowStaleTimestamp)
	if err == nil || !strings.Contains(err.Error(), "test-table") {
		t.Fatalf("expected table-not-exist error, got: %v", err)
	}
}

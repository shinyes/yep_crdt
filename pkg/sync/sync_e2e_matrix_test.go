package sync

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
)

type inMemoryP2PBus struct {
	mu    sync.RWMutex
	nodes map[string]*NodeManager
	stats inMemoryP2PStats
}

type inMemoryP2PStats struct {
	fetchCalls          int
	rawDataDelivered    int
	rawDeltaDelivered   int
	rawDataBlocked      int
	rawDeltaBlocked     int
	heartbeatDeliveries int
}

func newInMemoryP2PBus(nodes map[string]*NodeManager) *inMemoryP2PBus {
	cloned := make(map[string]*NodeManager, len(nodes))
	for nodeID, nm := range nodes {
		cloned[nodeID] = nm
	}
	return &inMemoryP2PBus{nodes: cloned}
}

func (b *inMemoryP2PBus) endpoint(localNodeID string) NetworkInterface {
	return &inMemoryP2PEndpoint{
		bus:         b,
		localNodeID: localNodeID,
	}
}

func (b *inMemoryP2PBus) getNode(nodeID string) *NodeManager {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.nodes[nodeID]
}

func (b *inMemoryP2PBus) peerIDs(localNodeID string) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	peers := make([]string, 0, len(b.nodes))
	for nodeID := range b.nodes {
		if nodeID == localNodeID {
			continue
		}
		peers = append(peers, nodeID)
	}
	return peers
}

func (b *inMemoryP2PBus) withStats(fn func(*inMemoryP2PStats)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	fn(&b.stats)
}

func (b *inMemoryP2PBus) fetchCalls() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.stats.fetchCalls
}

func (b *inMemoryP2PBus) rawDeltaBlocked() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.stats.rawDeltaBlocked
}

func (b *inMemoryP2PBus) rawDeltaDelivered() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.stats.rawDeltaDelivered
}

func (b *inMemoryP2PBus) fetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	source := b.getNode(sourceNodeID)
	if source == nil {
		return nil, fmt.Errorf("source node not found: %s", sourceNodeID)
	}
	b.withStats(func(stats *inMemoryP2PStats) {
		stats.fetchCalls++
	})
	return source.dataSync.ExportTableRawData(tableName)
}

func (b *inMemoryP2PBus) deliver(senderNodeID string, targetNodeID string, msg *NetworkMessage) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}

	target := b.getNode(targetNodeID)
	if target == nil {
		return fmt.Errorf("target node not found: %s", targetNodeID)
	}

	copied := *msg
	copied.Columns = append([]string(nil), msg.Columns...)
	copied.RawData = append([]byte(nil), msg.RawData...)
	if len(msg.LocalFiles) > 0 {
		copied.LocalFiles = make([]SyncedLocalFile, len(msg.LocalFiles))
		copy(copied.LocalFiles, msg.LocalFiles)
	}

	if copied.Type != MsgTypeHeartbeat {
		target.MarkPeerSeen(senderNodeID)
		if copied.GCFloor > 0 {
			target.ObservePeerGCFloor(senderNodeID, copied.GCFloor)
		}
	}

	switch copied.Type {
	case MsgTypeHeartbeat:
		target.OnHeartbeat(senderNodeID, copied.Clock, copied.GCFloor)
		b.withStats(func(stats *inMemoryP2PStats) {
			stats.heartbeatDeliveries++
		})
		return nil

	case MsgTypeRawData:
		if !target.CanUseIncrementalWithPeer(senderNodeID) {
			b.withStats(func(stats *inMemoryP2PStats) {
				stats.rawDataBlocked++
			})
			return nil
		}
		if err := target.dataSync.OnReceiveMergeWithFiles(copied.Table, copied.Key, copied.RawData, copied.Timestamp, copied.LocalFiles); err != nil {
			return err
		}
		b.withStats(func(stats *inMemoryP2PStats) {
			stats.rawDataDelivered++
		})
		return nil

	case MsgTypeRawDelta:
		if !target.CanUseIncrementalWithPeer(senderNodeID) {
			b.withStats(func(stats *inMemoryP2PStats) {
				stats.rawDeltaBlocked++
			})
			return nil
		}
		if err := target.dataSync.OnReceiveDeltaWithFiles(copied.Table, copied.Key, copied.Columns, copied.RawData, copied.Timestamp, copied.LocalFiles); err != nil {
			return err
		}
		b.withStats(func(stats *inMemoryP2PStats) {
			stats.rawDeltaDelivered++
		})
		return nil

	default:
		return fmt.Errorf("unsupported message type: %s", copied.Type)
	}
}

type inMemoryP2PEndpoint struct {
	bus         *inMemoryP2PBus
	localNodeID string
}

func (ep *inMemoryP2PEndpoint) SendHeartbeat(targetNodeID string, clock int64) error {
	return ep.bus.deliver(ep.localNodeID, targetNodeID, &NetworkMessage{
		Type:      MsgTypeHeartbeat,
		Clock:     clock,
		Timestamp: clock,
	})
}

func (ep *inMemoryP2PEndpoint) BroadcastHeartbeat(clock int64) error {
	for _, peerID := range ep.bus.peerIDs(ep.localNodeID) {
		if err := ep.SendHeartbeat(peerID, clock); err != nil {
			return err
		}
	}
	return nil
}

func (ep *inMemoryP2PEndpoint) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	return ep.bus.deliver(ep.localNodeID, targetNodeID, &NetworkMessage{
		Type:      MsgTypeRawData,
		Table:     table,
		Key:       key,
		RawData:   rawData,
		Timestamp: timestamp,
	})
}

func (ep *inMemoryP2PEndpoint) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	for _, peerID := range ep.bus.peerIDs(ep.localNodeID) {
		if err := ep.SendRawData(peerID, table, key, rawData, timestamp); err != nil {
			return err
		}
	}
	return nil
}

func (ep *inMemoryP2PEndpoint) SendRawDelta(targetNodeID string, table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return ep.bus.deliver(ep.localNodeID, targetNodeID, &NetworkMessage{
		Type:      MsgTypeRawDelta,
		Table:     table,
		Key:       key,
		Columns:   append([]string(nil), columns...),
		RawData:   rawData,
		Timestamp: timestamp,
	})
}

func (ep *inMemoryP2PEndpoint) BroadcastRawDelta(table string, key string, columns []string, rawData []byte, timestamp int64) error {
	for _, peerID := range ep.bus.peerIDs(ep.localNodeID) {
		if err := ep.SendRawDelta(peerID, table, key, columns, rawData, timestamp); err != nil {
			return err
		}
	}
	return nil
}

func (ep *inMemoryP2PEndpoint) SendMessage(targetNodeID string, msg *NetworkMessage) error {
	return ep.bus.deliver(ep.localNodeID, targetNodeID, msg)
}

func (ep *inMemoryP2PEndpoint) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	return ep.bus.fetchRawTableData(sourceNodeID, tableName)
}

func TestTwoNodeSyncMatrix_FullSyncDeltaAndGCFloor(t *testing.T) {
	nodeA, dbA := createTestNode(t, "node-a")
	nodeB, dbB := createTestNode(t, "node-b")
	defer dbA.Close()
	defer dbB.Close()

	schema := &meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "email", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}
	if err := dbA.DefineTable(schema); err != nil {
		t.Fatalf("define table on node-a failed: %v", err)
	}
	if err := dbB.DefineTable(schema); err != nil {
		t.Fatalf("define table on node-b failed: %v", err)
	}

	bus := newInMemoryP2PBus(map[string]*NodeManager{
		"node-a": nodeA,
		"node-b": nodeB,
	})
	nodeA.RegisterNetwork(bus.endpoint("node-a"))
	nodeB.RegisterNetwork(bus.endpoint("node-b"))
	nodeA.OnPeerConnected("node-b")
	nodeB.OnPeerConnected("node-a")

	userID, _ := uuid.NewV7()
	if err := dbA.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(userID, map[string]any{
			"name":  "Alice",
			"email": "alice+v1@example.com",
		})
	}); err != nil {
		t.Fatalf("seed node-a row failed: %v", err)
	}

	fullSyncResult, err := nodeB.FullSync(context.Background(), "node-a")
	if err != nil {
		t.Fatalf("full sync failed: %v", err)
	}
	if fullSyncResult.TablesSynced != 1 || fullSyncResult.RowsSynced != 1 {
		t.Fatalf("unexpected full sync result: %+v", fullSyncResult)
	}

	rowB := mustReadRow(t, dbB, "users", userID)
	if toScalarString(rowB["name"]) != "Alice" || toScalarString(rowB["email"]) != "alice+v1@example.com" {
		t.Fatalf("unexpected row after full sync: %+v", rowB)
	}

	if err := dbA.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(userID, map[string]any{
			"name":  "Alice",
			"email": "alice+v2@example.com",
		})
	}); err != nil {
		t.Fatalf("update node-a row(v2) failed: %v", err)
	}
	if err := nodeA.dataSync.SendRowDeltaToPeer("node-b", "users", userID, []string{"email"}); err != nil {
		t.Fatalf("send delta(v2) failed: %v", err)
	}

	rowB = mustReadRow(t, dbB, "users", userID)
	if toScalarString(rowB["name"]) != "Alice" {
		t.Fatalf("delta should not change untouched column name, got=%q", toScalarString(rowB["name"]))
	}
	if toScalarString(rowB["email"]) != "alice+v2@example.com" {
		t.Fatalf("delta should update email to v2, got=%q", toScalarString(rowB["email"]))
	}

	if err := nodeA.SetLocalGCFloor(300); err != nil {
		t.Fatalf("set node-a gc floor failed: %v", err)
	}
	if err := nodeB.SetLocalGCFloor(100); err != nil {
		t.Fatalf("set node-b gc floor failed: %v", err)
	}

	if err := dbA.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(userID, map[string]any{
			"name":  "Alice",
			"email": "alice+v3@example.com",
		})
	}); err != nil {
		t.Fatalf("update node-a row(v3) failed: %v", err)
	}
	if err := nodeA.dataSync.SendRowDeltaToPeer("node-b", "users", userID, []string{"email"}); err != nil {
		t.Fatalf("send delta(v3) failed: %v", err)
	}

	waitForCondition(t, 3*time.Second, func() bool {
		return bus.rawDeltaBlocked() > 0
	}, "expected blocked incremental delta after gc floor mismatch")

	waitForCondition(t, 3*time.Second, func() bool {
		return nodeB.LocalGCFloor() == 300 &&
			nodeB.CanUseIncrementalWithPeer("node-a") &&
			toScalarString(mustReadRow(t, dbB, "users", userID)["email"]) == "alice+v3@example.com"
	}, "expected gc floor catch-up via full sync and incremental gate reopen")

	if bus.fetchCalls() == 0 {
		t.Fatal("expected full sync fetch call when peer gc floor is ahead")
	}

	if err := dbA.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(userID, map[string]any{
			"name":  "Alice",
			"email": "alice+v4@example.com",
		})
	}); err != nil {
		t.Fatalf("update node-a row(v4) failed: %v", err)
	}
	if err := nodeA.dataSync.SendRowDeltaToPeer("node-b", "users", userID, []string{"email"}); err != nil {
		t.Fatalf("send delta(v4) failed: %v", err)
	}

	waitForCondition(t, 3*time.Second, func() bool {
		return toScalarString(mustReadRow(t, dbB, "users", userID)["email"]) == "alice+v4@example.com"
	}, "expected incremental delta to recover after gc floor catch-up")

	if bus.rawDeltaDelivered() < 2 {
		t.Fatalf("expected at least two delivered deltas (before and after gc floor catch-up), got=%d", bus.rawDeltaDelivered())
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, check func() bool, timeoutMsg string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		if check() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(timeoutMsg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func mustReadRow(t *testing.T, database *db.DB, tableName string, key uuid.UUID) map[string]any {
	t.Helper()

	var row map[string]any
	err := database.View(func(tx *db.Tx) error {
		table := tx.Table(tableName)
		if table == nil {
			return fmt.Errorf("table not found: %s", tableName)
		}
		var getErr error
		row, getErr = table.Get(key)
		return getErr
	})
	if err != nil {
		t.Fatalf("read row failed: %v", err)
	}
	if row == nil {
		t.Fatalf("row not found: table=%s key=%s", tableName, key.String())
	}
	return row
}

func toScalarString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

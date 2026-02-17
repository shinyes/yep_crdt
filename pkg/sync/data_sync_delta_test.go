package sync

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

type deltaCaptureNetwork struct {
	deltaCalls int
	table      string
	key        string
	columns    []string
	rawData    []byte
	timestamp  int64
}

func (n *deltaCaptureNetwork) SendHeartbeat(targetNodeID string, clock int64) error { return nil }
func (n *deltaCaptureNetwork) BroadcastHeartbeat(clock int64) error                 { return nil }
func (n *deltaCaptureNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	return nil
}
func (n *deltaCaptureNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	return nil
}
func (n *deltaCaptureNetwork) SendRawDelta(targetNodeID string, table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return nil
}
func (n *deltaCaptureNetwork) BroadcastRawDelta(table string, key string, columns []string, rawData []byte, timestamp int64) error {
	n.deltaCalls++
	n.table = table
	n.key = key
	n.columns = append([]string(nil), columns...)
	n.rawData = append([]byte(nil), rawData...)
	n.timestamp = timestamp
	return nil
}
func (n *deltaCaptureNetwork) SendMessage(targetNodeID string, msg *NetworkMessage) error { return nil }
func (n *deltaCaptureNetwork) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	return nil, nil
}

func TestBroadcastRowDelta_OnlySelectedColumns(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "delta-node")
	defer database.Close()

	err = database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "email", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	})
	if err != nil {
		t.Fatalf("define table failed: %v", err)
	}

	key, _ := uuid.NewV7()
	err = database.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(key, map[string]any{
			"name":  "Alice",
			"email": "alice@v1.com",
		})
	})
	if err != nil {
		t.Fatalf("seed row failed: %v", err)
	}

	net := &deltaCaptureNetwork{}
	dsm := NewDataSyncManager(database, "delta-node")
	dsm.SetNetwork(net)

	if err := dsm.BroadcastRowDelta("users", key, []string{"email"}); err != nil {
		t.Fatalf("broadcast delta failed: %v", err)
	}

	if net.deltaCalls != 1 {
		t.Fatalf("expected 1 delta broadcast, got %d", net.deltaCalls)
	}
	if net.table != "users" || net.key != key.String() {
		t.Fatalf("unexpected target row: table=%s key=%s", net.table, net.key)
	}

	m, err := crdt.FromBytesMap(net.rawData)
	if err != nil {
		t.Fatalf("decode delta payload failed: %v", err)
	}
	if len(m.Entries) != 1 {
		t.Fatalf("expected 1 entry in delta payload, got %d", len(m.Entries))
	}
	if _, ok := m.Entries["email"]; !ok {
		t.Fatalf("delta payload should include email")
	}
	if _, ok := m.Entries["name"]; ok {
		t.Fatalf("delta payload should not include name")
	}
}

func TestOnReceiveDelta_MergesOnlyChangedColumns(t *testing.T) {
	s1, err := store.NewBadgerStore(t.TempDir() + "/db1")
	if err != nil {
		t.Fatalf("create store1 failed: %v", err)
	}
	defer s1.Close()

	s2, err := store.NewBadgerStore(t.TempDir() + "/db2")
	if err != nil {
		t.Fatalf("create store2 failed: %v", err)
	}
	defer s2.Close()

	db1 := mustOpenDB(t, s1, "node-a")
	db2 := mustOpenDB(t, s2, "node-b")
	defer db1.Close()
	defer db2.Close()

	schema := &meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "email", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}
	if err := db1.DefineTable(schema); err != nil {
		t.Fatalf("define table on db1 failed: %v", err)
	}
	if err := db2.DefineTable(schema); err != nil {
		t.Fatalf("define table on db2 failed: %v", err)
	}

	key, _ := uuid.NewV7()
	if err := db2.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(key, map[string]any{
			"name":  "Alice",
			"email": "alice@v1.com",
		})
	}); err != nil {
		t.Fatalf("seed db2 failed: %v", err)
	}

	time.Sleep(10 * time.Millisecond)
	if err := db1.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(key, map[string]any{
			"name":  "Alice",
			"email": "alice@v2.com",
		})
	}); err != nil {
		t.Fatalf("seed db1 failed: %v", err)
	}

	var deltaRaw []byte
	if err := db1.View(func(tx *db.Tx) error {
		var e error
		deltaRaw, e = tx.Table("users").GetRawRowColumns(key, []string{"email"})
		return e
	}); err != nil {
		t.Fatalf("build delta payload failed: %v", err)
	}

	dsm := NewDataSyncManager(db2, "node-b")
	timestamp := db2.Clock().Now() + 1_000_000
	if err := dsm.OnReceiveDelta("users", key.String(), []string{"email"}, deltaRaw, timestamp); err != nil {
		t.Fatalf("receive delta failed: %v", err)
	}

	var row map[string]any
	if err := db2.View(func(tx *db.Tx) error {
		var e error
		row, e = tx.Table("users").Get(key)
		return e
	}); err != nil {
		t.Fatalf("read merged row failed: %v", err)
	}

	if asString(row["name"]) != "Alice" {
		t.Fatalf("name should stay unchanged, got %s", asString(row["name"]))
	}
	if asString(row["email"]) != "alice@v2.com" {
		t.Fatalf("email should be updated by delta, got %s", asString(row["email"]))
	}
}

func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return ""
	}
}

package sync

import (
	"testing"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
	"github.com/vmihailenco/msgpack/v5"
)

type captureNetwork struct {
	target string
	msg    *NetworkMessage

	rawCalls []rawCall
}

type rawCall struct {
	target    string
	table     string
	key       string
	rawData   []byte
	timestamp int64
}

func (n *captureNetwork) SendHeartbeat(targetNodeID string, clock int64) error { return nil }
func (n *captureNetwork) BroadcastHeartbeat(clock int64) error                 { return nil }
func (n *captureNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	copyData := make([]byte, len(rawData))
	copy(copyData, rawData)
	n.rawCalls = append(n.rawCalls, rawCall{
		target:    targetNodeID,
		table:     table,
		key:       key,
		rawData:   copyData,
		timestamp: timestamp,
	})
	return nil
}
func (n *captureNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	return nil
}
func (n *captureNetwork) SendRawDelta(targetNodeID string, table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return nil
}
func (n *captureNetwork) BroadcastRawDelta(table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return nil
}
func (n *captureNetwork) SendMessage(targetNodeID string, msg *NetworkMessage) error {
	if msg != nil && msg.Type == MsgTypeRawData {
		copyData := make([]byte, len(msg.RawData))
		copy(copyData, msg.RawData)
		n.rawCalls = append(n.rawCalls, rawCall{
			target:    targetNodeID,
			table:     msg.Table,
			key:       msg.Key,
			rawData:   copyData,
			timestamp: msg.Timestamp,
		})
		return nil
	}

	n.target = targetNodeID
	if msg == nil {
		n.msg = nil
		return nil
	}
	cloned := *msg
	n.msg = &cloned
	return nil
}
func (n *captureNetwork) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	return nil, nil
}

func TestVersionSyncOnPeerConnected_SendsDigestMessage(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "db-1")
	defer database.Close()

	err = database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	})
	if err != nil {
		t.Fatalf("define table failed: %v", err)
	}

	net := &captureNetwork{}
	nm := &NodeManager{
		localNodeID: "local-1",
		network:     net,
	}
	vs := NewVersionSync(database, nm)

	vs.OnPeerConnected("peer-1")

	if net.msg == nil {
		t.Fatal("expected digest message to be sent")
	}
	if net.target != "peer-1" {
		t.Fatalf("unexpected target: %s", net.target)
	}
	if net.msg.Type != MsgTypeVersionDigest {
		t.Fatalf("unexpected message type: %s", net.msg.Type)
	}
	if len(net.msg.RawData) == 0 {
		t.Fatal("digest payload should not be empty")
	}
}

func TestVersionSyncOnReceiveDigest_SendsOnlyDiffRows(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "db-1")
	defer database.Close()

	err = database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	})
	if err != nil {
		t.Fatalf("define table failed: %v", err)
	}

	k1, _ := uuid.NewV7()
	k2, _ := uuid.NewV7()
	if err := database.Update(func(tx *db.Tx) error {
		tbl := tx.Table("users")
		if err := tbl.Set(k1, map[string]any{"name": "alice"}); err != nil {
			return err
		}
		return tbl.Set(k2, map[string]any{"name": "bob"})
	}); err != nil {
		t.Fatalf("seed rows failed: %v", err)
	}

	net := &captureNetwork{}
	nm := &NodeManager{
		localNodeID: "local-1",
		network:     net,
	}
	vs := NewVersionSync(database, nm)

	localDigest := vs.BuildDigest()
	if localDigest == nil || len(localDigest.Tables) == 0 {
		t.Fatal("expected local digest")
	}

	var usersDigest *TableDigest
	for i := range localDigest.Tables {
		if localDigest.Tables[i].TableName == "users" {
			usersDigest = &localDigest.Tables[i]
			break
		}
	}
	if usersDigest == nil {
		t.Fatal("users table digest not found")
	}

	hash1, ok := usersDigest.RowKeys[k1.String()]
	if !ok {
		t.Fatalf("missing key in local digest: %s", k1.String())
	}

	remoteDigest := VersionDigest{
		NodeID: "peer-1",
		Tables: []TableDigest{
			{
				TableName: "users",
				RowKeys: map[string]uint32{
					k1.String(): hash1, // key1 is in-sync; key2 missing => should be sent
				},
			},
		},
	}
	rawDigest, err := msgpack.Marshal(remoteDigest)
	if err != nil {
		t.Fatalf("marshal remote digest failed: %v", err)
	}

	vs.OnReceiveDigest("peer-1", &NetworkMessage{
		Type:    MsgTypeVersionDigest,
		NodeID:  "local-1",
		RawData: rawDigest,
	})

	if len(net.rawCalls) != 1 {
		t.Fatalf("expected 1 diff row to be sent, got %d", len(net.rawCalls))
	}
	if net.msg != nil {
		t.Fatal("did not expect digest echo message")
	}
	call := net.rawCalls[0]
	if call.target != "peer-1" {
		t.Fatalf("unexpected target: %s", call.target)
	}
	if call.table != "users" {
		t.Fatalf("unexpected table: %s", call.table)
	}
	if call.key != k2.String() {
		t.Fatalf("unexpected diff key: %s", call.key)
	}
	if len(call.rawData) == 0 {
		t.Fatal("expected raw row payload")
	}
}

package sync

import (
	"testing"

	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

type captureNetwork struct {
	target string
	msg    *NetworkMessage
}

func (n *captureNetwork) SendHeartbeat(targetNodeID string, clock int64) error { return nil }
func (n *captureNetwork) BroadcastHeartbeat(clock int64) error                 { return nil }
func (n *captureNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	return nil
}
func (n *captureNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	return nil
}
func (n *captureNetwork) SendMessage(targetNodeID string, msg *NetworkMessage) error {
	n.target = targetNodeID
	n.msg = msg
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

	database := db.Open(s, "db-1")
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

package sync

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

type merkleMockNetwork struct {
	remoteDB *db.DB

	rawCalls []rawCall
	msg      *NetworkMessage
}

func (n *merkleMockNetwork) SendHeartbeat(targetNodeID string, clock int64) error { return nil }
func (n *merkleMockNetwork) BroadcastHeartbeat(clock int64) error                 { return nil }
func (n *merkleMockNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
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
func (n *merkleMockNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	return nil
}
func (n *merkleMockNetwork) SendRawDelta(targetNodeID string, table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return nil
}
func (n *merkleMockNetwork) BroadcastRawDelta(table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return nil
}
func (n *merkleMockNetwork) SendMessage(targetNodeID string, msg *NetworkMessage) error {
	if msg == nil {
		n.msg = nil
		return nil
	}
	cloned := *msg
	n.msg = &cloned
	return nil
}
func (n *merkleMockNetwork) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	return nil, nil
}

func (n *merkleMockNetwork) SendWithResponse(peerID string, msg *NetworkMessage, timeout time.Duration) (*NetworkMessage, error) {
	if msg == nil {
		return nil, fmt.Errorf("message is nil")
	}

	switch msg.Type {
	case MsgTypeMerkleRootReq:
		var req MerkleRootRequest
		if err := unmarshalSyncWire(msg.RawData, &req); err != nil {
			return nil, err
		}
		roots := make(map[string]string, len(req.Tables))
		for _, tableName := range req.Tables {
			table := n.remoteDB.Table(tableName)
			if table == nil {
				continue
			}
			root, err := table.MerkleRootHash()
			if err != nil {
				return nil, err
			}
			roots[tableName] = root
		}
		raw, err := marshalSyncWire(&MerkleRootResponse{Roots: roots})
		if err != nil {
			return nil, err
		}
		return &NetworkMessage{Type: MsgTypeMerkleRootAck, RequestID: msg.RequestID, RawData: raw}, nil

	case MsgTypeMerkleNodeReq:
		var req MerkleNodeRequest
		if err := unmarshalSyncWire(msg.RawData, &req); err != nil {
			return nil, err
		}
		resp := MerkleNodeResponse{
			Table:    req.Table,
			Level:    req.Level,
			Prefix:   req.Prefix,
			Children: map[string]string{},
		}
		table := n.remoteDB.Table(req.Table)
		if table != nil {
			nodeHash, err := table.MerkleNodeHash(req.Level, req.Prefix)
			if err != nil {
				return nil, err
			}
			children, err := table.MerkleChildren(req.Level, req.Prefix)
			if err != nil {
				return nil, err
			}
			resp.NodeHash = nodeHash
			resp.Children = children
		}
		raw, err := marshalSyncWire(&resp)
		if err != nil {
			return nil, err
		}
		return &NetworkMessage{Type: MsgTypeMerkleNodeAck, RequestID: msg.RequestID, RawData: raw}, nil

	case MsgTypeMerkleLeafReq:
		var req MerkleLeafRequest
		if err := unmarshalSyncWire(msg.RawData, &req); err != nil {
			return nil, err
		}
		resp := MerkleLeafResponse{
			Table:  req.Table,
			Prefix: req.Prefix,
			Rows:   map[string]string{},
		}
		table := n.remoteDB.Table(req.Table)
		if table != nil {
			rows, err := table.MerkleLeafRows(req.Prefix)
			if err != nil {
				return nil, err
			}
			resp.Rows = rows
		}
		raw, err := marshalSyncWire(&resp)
		if err != nil {
			return nil, err
		}
		return &NetworkMessage{Type: MsgTypeMerkleLeafAck, RequestID: msg.RequestID, RawData: raw}, nil

	default:
		return nil, fmt.Errorf("unsupported request type: %s", msg.Type)
	}
}

func TestVersionSync_MerkleCompareAndSync_SendsOnlyDiffRows(t *testing.T) {
	localStore, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create local store failed: %v", err)
	}
	defer localStore.Close()
	remoteStore, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create remote store failed: %v", err)
	}
	defer remoteStore.Close()

	localDB := mustOpenDB(t, localStore, "local-db")
	defer localDB.Close()
	remoteDB := mustOpenDB(t, remoteStore, "remote-db")
	defer remoteDB.Close()

	schema := &meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}
	if err := localDB.DefineTable(schema); err != nil {
		t.Fatalf("define local schema failed: %v", err)
	}
	if err := remoteDB.DefineTable(schema); err != nil {
		t.Fatalf("define remote schema failed: %v", err)
	}

	k1, _ := uuid.NewV7()
	k2, _ := uuid.NewV7()

	if err := localDB.Update(func(tx *db.Tx) error {
		tbl := tx.Table("users")
		if err := tbl.Set(k1, map[string]any{"name": "alice"}); err != nil {
			return err
		}
		return tbl.Set(k2, map[string]any{"name": "bob"})
	}); err != nil {
		t.Fatalf("seed local rows failed: %v", err)
	}
	if err := remoteDB.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(k1, map[string]any{"name": "alice"})
	}); err != nil {
		t.Fatalf("seed remote rows failed: %v", err)
	}

	net := &merkleMockNetwork{remoteDB: remoteDB}
	nm := &NodeManager{
		localNodeID: "local-1",
		network:     net,
		db:          localDB,
	}
	vs := NewVersionSync(localDB, nm)
	vs.nodeMgr.dataSync = nil

	if err := vs.OnPeerConnectedWithError("peer-1"); err != nil {
		t.Fatalf("merkle compare-and-sync failed: %v", err)
	}

	if len(net.rawCalls) != 1 {
		t.Fatalf("expected 1 diff row to be sent, got %d", len(net.rawCalls))
	}
	if net.rawCalls[0].key != k2.String() {
		t.Fatalf("expected only k2 to be sent, got %s", net.rawCalls[0].key)
	}
	if net.msg != nil && net.msg.Type == MsgTypeVersionDigest {
		t.Fatal("did not expect legacy version digest when merkle path succeeds")
	}
}

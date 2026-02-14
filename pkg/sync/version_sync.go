package sync

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
)

// VersionSync exchanges per-table digests when peers connect.
type VersionSync struct {
	db      *db.DB
	nodeMgr *NodeManager
}

// NewVersionSync creates a VersionSync component.
func NewVersionSync(database *db.DB, nodeMgr *NodeManager) *VersionSync {
	return &VersionSync{
		db:      database,
		nodeMgr: nodeMgr,
	}
}

// OnPeerConnected builds and sends local digest to a peer.
func (vs *VersionSync) OnPeerConnected(peerID string) {
	if vs.nodeMgr.network == nil {
		return
	}

	digest := vs.BuildDigest()
	if digest == nil {
		return
	}

	digestBytes, err := json.Marshal(digest)
	if err != nil {
		log.Printf("[VersionSync] marshal digest failed: %v", err)
		return
	}

	msg := &NetworkMessage{
		Type:      MsgTypeVersionDigest,
		NodeID:    vs.nodeMgr.localNodeID,
		RawData:   digestBytes,
		Timestamp: vs.db.Clock().Now(),
	}

	if err := vs.nodeMgr.network.SendMessage(peerID, msg); err != nil {
		log.Printf("[VersionSync] send digest failed: %v", err)
		return
	}

	log.Printf("[VersionSync] sent digest to %s, tables=%d", shortPeerID(peerID), len(digest.Tables))
}

// BuildDigest builds digest for all local tables.
func (vs *VersionSync) BuildDigest() *VersionDigest {
	tableNames := vs.db.TableNames()
	if len(tableNames) == 0 {
		return nil
	}

	digest := &VersionDigest{
		NodeID: vs.nodeMgr.localNodeID,
		Tables: make([]TableDigest, 0, len(tableNames)),
	}

	for _, tableName := range tableNames {
		table := vs.db.Table(tableName)
		if table == nil {
			continue
		}

		var rowDigests []db.RowDigest
		_ = vs.db.View(func(tx *db.Tx) error {
			t := tx.Table(tableName)
			if t != nil {
				rowDigests, _ = t.ScanRowDigest()
			}
			return nil
		})

		rowKeys := make(map[string]uint32, len(rowDigests))
		for _, rd := range rowDigests {
			rowKeys[rd.Key.String()] = rd.Hash
		}

		digest.Tables = append(digest.Tables, TableDigest{
			TableName: tableName,
			RowKeys:   rowKeys,
		})
	}

	return digest
}

// OnReceiveDigest compares remote digest and sends local diffs.
func (vs *VersionSync) OnReceiveDigest(peerID string, msg *NetworkMessage) {
	var remoteDigest VersionDigest
	if err := json.Unmarshal(msg.RawData, &remoteDigest); err != nil {
		log.Printf("[VersionSync] unmarshal remote digest failed: %v", err)
		return
	}

	log.Printf("[VersionSync] received digest from %s, tables=%d", shortPeerID(peerID), len(remoteDigest.Tables))

	remoteIndex := make(map[string]map[string]uint32)
	for _, td := range remoteDigest.Tables {
		remoteIndex[td.TableName] = td.RowKeys
	}

	var diffCount int
	tableNames := vs.db.TableNames()
	for _, tableName := range tableNames {
		table := vs.db.Table(tableName)
		if table == nil {
			continue
		}

		var localDigests []db.RowDigest
		_ = vs.db.View(func(tx *db.Tx) error {
			t := tx.Table(tableName)
			if t != nil {
				localDigests, _ = t.ScanRowDigest()
			}
			return nil
		})

		remoteRows := remoteIndex[tableName]
		for _, ld := range localDigests {
			keyStr := ld.Key.String()
			remoteHash, existsInRemote := uint32(0), false
			if remoteRows != nil {
				remoteHash, existsInRemote = remoteRows[keyStr]
			}

			if !existsInRemote || remoteHash != ld.Hash {
				vs.sendRow(peerID, tableName, ld.Key)
				diffCount++
			}
		}
	}

	log.Printf("[VersionSync] sent %d diff rows to %s", diffCount, shortPeerID(peerID))

	// Echo back local digest only when the remote digest comes from another node.
	if msg.NodeID != vs.nodeMgr.localNodeID {
		go vs.OnPeerConnected(peerID)
	}
}

// sendRow sends one local row to a peer.
func (vs *VersionSync) sendRow(peerID string, tableName string, key uuid.UUID) {
	var rawData []byte
	_ = vs.db.View(func(tx *db.Tx) error {
		table := tx.Table(tableName)
		if table != nil {
			rawData, _ = table.GetRawRow(key)
		}
		return nil
	})

	if rawData == nil {
		return
	}

	timestamp := vs.db.Clock().Now()
	if err := vs.nodeMgr.network.SendRawData(peerID, tableName, key.String(), rawData, timestamp); err != nil {
		log.Printf("[VersionSync] send row failed: table=%s, key=%s, err=%v",
			tableName, shortPeerID(key.String()), err)
	}
}

// CompareAndSync triggers a digest exchange with one peer.
func (vs *VersionSync) CompareAndSync(peerID string) error {
	if vs.nodeMgr.network == nil {
		return fmt.Errorf("network not registered")
	}

	vs.OnPeerConnected(peerID)
	return nil
}

func shortPeerID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

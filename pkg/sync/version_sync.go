package sync

import (
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/vmihailenco/msgpack/v5"
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
	if err := vs.OnPeerConnectedWithError(peerID); err != nil {
		log.Printf("[VersionSync] send digest failed: peer=%s, err=%v", shortPeerID(peerID), err)
	}
}

// OnPeerConnectedWithError builds and sends local digest to a peer and returns detailed errors.
func (vs *VersionSync) OnPeerConnectedWithError(peerID string) error {
	network := vs.nodeMgr.getNetwork()
	if network == nil {
		return ErrNoNetwork
	}

	digest, err := vs.buildDigest()
	if err != nil {
		return err
	}
	if digest == nil {
		return nil
	}

	digestBytes, err := msgpack.Marshal(digest)
	if err != nil {
		return fmt.Errorf("marshal digest failed: %w", err)
	}

	msg := &NetworkMessage{
		Type:      MsgTypeVersionDigest,
		NodeID:    vs.nodeMgr.localNodeID,
		RawData:   digestBytes,
		GCFloor:   vs.db.GCFloor(),
		Timestamp: vs.db.Clock().Now(),
	}

	if err := network.SendMessage(peerID, msg); err != nil {
		return fmt.Errorf("send digest failed: %w", err)
	}

	log.Printf("[VersionSync] sent digest to %s, tables=%d", shortPeerID(peerID), len(digest.Tables))
	return nil
}

// BuildDigest builds digest for all local tables.
func (vs *VersionSync) BuildDigest() *VersionDigest {
	digest, err := vs.buildDigest()
	if err != nil {
		log.Printf("[VersionSync] build digest failed: %v", err)
		return nil
	}
	return digest
}

func (vs *VersionSync) buildDigest() (*VersionDigest, error) {
	tableNames := vs.db.TableNames()
	if len(tableNames) == 0 {
		return nil, nil
	}

	digest := &VersionDigest{
		NodeID: vs.nodeMgr.localNodeID,
		Tables: make([]TableDigest, 0, len(tableNames)),
	}

	if err := vs.db.View(func(tx *db.Tx) error {
		for _, tableName := range tableNames {
			t := tx.Table(tableName)
			if t == nil {
				continue
			}

			rowDigests, err := t.ScanRowDigest()
			if err != nil {
				return fmt.Errorf("scan digest failed: table=%s, err=%w", tableName, err)
			}

			rowKeys := make(map[string]string, len(rowDigests))
			for _, rd := range rowDigests {
				rowKeys[rd.Key.String()] = rd.Hash
			}

			digest.Tables = append(digest.Tables, TableDigest{
				TableName: tableName,
				RowKeys:   rowKeys,
			})
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if len(digest.Tables) == 0 {
		return nil, nil
	}
	return digest, nil
}

// OnReceiveDigest compares remote digest and sends local diffs.
func (vs *VersionSync) OnReceiveDigest(peerID string, msg *NetworkMessage) {
	if msg == nil {
		return
	}
	if vs.nodeMgr != nil && vs.nodeMgr.db != nil && msg.GCFloor > 0 {
		vs.nodeMgr.ObservePeerGCFloor(peerID, msg.GCFloor)
	}
	if vs.nodeMgr != nil && vs.nodeMgr.db != nil && !vs.nodeMgr.CanUseIncrementalWithPeer(peerID) {
		log.Printf("[VersionSync] skip incremental digest sync with blocked peer: %s", shortPeerID(peerID))
		return
	}

	var remoteDigest VersionDigest
	if err := msgpack.Unmarshal(msg.RawData, &remoteDigest); err != nil {
		log.Printf("[VersionSync] unmarshal remote digest failed: %v", err)
		return
	}

	log.Printf("[VersionSync] received digest from %s, tables=%d", shortPeerID(peerID), len(remoteDigest.Tables))

	remoteIndex := make(map[string]map[string]string)
	for _, td := range remoteDigest.Tables {
		remoteIndex[td.TableName] = td.RowKeys
	}

	var diffCount int
	var sendFailCount int
	var tableErrCount int
	tableNames := vs.db.TableNames()
	for _, tableName := range tableNames {
		type rowPayload struct {
			key uuid.UUID
		}
		rowsToSend := make([]rowPayload, 0)

		if err := vs.db.View(func(tx *db.Tx) error {
			t := tx.Table(tableName)
			if t == nil {
				return nil
			}

			localDigests, err := t.ScanRowDigest()
			if err != nil {
				return fmt.Errorf("scan local digest failed: table=%s, err=%w", tableName, err)
			}

			remoteRows := remoteIndex[tableName]
			for _, ld := range localDigests {
				keyStr := ld.Key.String()
				remoteHash, existsInRemote := "", false
				if remoteRows != nil {
					remoteHash, existsInRemote = remoteRows[keyStr]
				}

				if existsInRemote && remoteHash == ld.Hash {
					continue
				}

				rowsToSend = append(rowsToSend, rowPayload{
					key: ld.Key,
				})
			}
			return nil
		}); err != nil {
			tableErrCount++
			log.Printf("[VersionSync] prepare diff rows failed: table=%s, err=%v", tableName, err)
			continue
		}

		for _, row := range rowsToSend {
			if err := vs.sendRowWithRetry(peerID, tableName, row.key); err != nil {
				sendFailCount++
				log.Printf("[VersionSync] send row failed: table=%s, key=%s, err=%v",
					tableName, shortPeerID(row.key.String()), err)
				continue
			}
			diffCount++
		}
	}

	log.Printf("[VersionSync] sent %d diff rows to %s (send_failures=%d, table_errors=%d)", diffCount, shortPeerID(peerID), sendFailCount, tableErrCount)
}

// CompareAndSync triggers a digest exchange with one peer.
func (vs *VersionSync) CompareAndSync(peerID string) error {
	if vs.nodeMgr.getNetwork() == nil {
		return ErrNoNetwork
	}

	return vs.OnPeerConnectedWithError(peerID)
}

func shortPeerID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func (vs *VersionSync) sendRowWithRetry(peerID string, tableName string, key uuid.UUID) error {
	const maxAttempts = 2

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := vs.sendRowOnce(peerID, tableName, key); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if attempt < maxAttempts {
			time.Sleep(20 * time.Millisecond)
		}
	}
	return fmt.Errorf("send row failed after %d attempts: %w", maxAttempts, lastErr)
}

func (vs *VersionSync) sendRowOnce(peerID string, tableName string, key uuid.UUID) error {
	if vs.nodeMgr.dataSync != nil {
		return vs.nodeMgr.dataSync.SendRowToPeer(peerID, tableName, key)
	}

	table := vs.db.Table(tableName)
	if table == nil {
		return fmt.Errorf("table does not exist: %s", tableName)
	}
	rawData, err := table.GetRawRow(key)
	if err != nil {
		return fmt.Errorf("get raw row failed: %w", err)
	}
	if rawData == nil {
		return fmt.Errorf("get raw row failed: empty raw data")
	}

	network := vs.nodeMgr.getNetwork()
	if network == nil {
		return ErrNoNetwork
	}

	timestamp := vs.db.Clock().Now()
	return network.SendRawData(peerID, tableName, key.String(), rawData, timestamp)
}

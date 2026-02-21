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
	network := vs.nodeMgr.getNetwork()
	if network == nil {
		return
	}

	digest := vs.BuildDigest()
	if digest == nil {
		return
	}

	digestBytes, err := msgpack.Marshal(digest)
	if err != nil {
		log.Printf("[VersionSync] marshal digest failed: %v", err)
		return
	}

	msg := &NetworkMessage{
		Type:      MsgTypeVersionDigest,
		NodeID:    vs.nodeMgr.localNodeID,
		RawData:   digestBytes,
		GCFloor:   vs.db.GCFloor(),
		Timestamp: vs.db.Clock().Now(),
	}

	if err := network.SendMessage(peerID, msg); err != nil {
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
		log.Printf("[VersionSync] build digest failed: %v", err)
		return nil
	}

	if len(digest.Tables) == 0 {
		return nil
	}
	return digest
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
			key     uuid.UUID
			rawData []byte
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

				rawData, err := t.GetRawRow(ld.Key)
				if err != nil {
					return fmt.Errorf("load raw row failed: table=%s, key=%s, err=%w", tableName, shortPeerID(ld.Key.String()), err)
				}
				if rawData == nil {
					return fmt.Errorf("load raw row failed: table=%s, key=%s, err=empty raw data", tableName, shortPeerID(ld.Key.String()))
				}

				rowsToSend = append(rowsToSend, rowPayload{
					key:     ld.Key,
					rawData: rawData,
				})
			}
			return nil
		}); err != nil {
			tableErrCount++
			log.Printf("[VersionSync] prepare diff rows failed: table=%s, err=%v", tableName, err)
			continue
		}

		for _, row := range rowsToSend {
			if err := vs.sendRowWithRetry(peerID, tableName, row.key, row.rawData); err != nil {
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

	vs.OnPeerConnected(peerID)
	return nil
}

func shortPeerID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func (vs *VersionSync) sendRowWithRetry(peerID string, tableName string, key uuid.UUID, rawData []byte) error {
	const maxAttempts = 2

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := vs.sendRowOnce(peerID, tableName, key, rawData); err == nil {
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

func (vs *VersionSync) sendRowOnce(peerID string, tableName string, key uuid.UUID, rawData []byte) error {
	if vs.nodeMgr.dataSync != nil {
		return vs.nodeMgr.dataSync.SendRowToPeer(peerID, tableName, key)
	}

	network := vs.nodeMgr.getNetwork()
	if network == nil {
		return ErrNoNetwork
	}

	timestamp := vs.db.Clock().Now()
	return network.SendRawData(peerID, tableName, key.String(), rawData, timestamp)
}

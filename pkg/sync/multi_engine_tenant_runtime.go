package sync

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

func (rt *tenantRuntime) runChangeWorker() {
	defer rt.workerWg.Done()

	ticker := time.NewTicker(defaultLocalFileChunkCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rt.ctx.Done():
			if rt.chunks != nil {
				rt.chunks.CleanupAll()
			}
			return
		case <-ticker.C:
			if rt.chunks != nil {
				rt.chunks.CleanupExpired()
			}
		case event := <-rt.changeQ:
			rt.onDataChangedDetailed(event.TableName, event.Key, event.Columns)
			atomic.AddUint64(&rt.stats.changeProcessed, 1)
		}
	}
}

func (rt *tenantRuntime) onDataChangedDetailed(tableName string, key uuid.UUID, columns []string) {
	if rt.nodeMgr == nil || rt.nodeMgr.dataSync == nil {
		return
	}

	peers := rt.nodeMgr.GetOnlineNodes()
	if len(peers) == 0 {
		return
	}

	for _, peerID := range peers {
		if peerID == "" || peerID == rt.nodeMgr.GetLocalNodeID() {
			continue
		}
		if !rt.nodeMgr.CanUseIncrementalWithPeer(peerID) {
			log.Printf("[MultiEngine:%s] skip incremental broadcast to blocked peer: peer=%s, table=%s, key=%s",
				rt.tenantID, shortPeerID(peerID), tableName, shortPeerID(key.String()))
			continue
		}

		if len(columns) > 0 {
			if err := rt.nodeMgr.dataSync.SendRowDeltaToPeer(peerID, tableName, key, columns); err != nil {
				log.Printf("[MultiEngine:%s] delta send failed, fallback full row: peer=%s, table=%s, key=%s, cols=%v, err=%v",
					rt.tenantID, shortPeerID(peerID), tableName, shortPeerID(key.String()), columns, err)
				if fullErr := rt.nodeMgr.dataSync.SendRowToPeer(peerID, tableName, key); fullErr != nil {
					log.Printf("[MultiEngine:%s] fallback full send failed: peer=%s, table=%s, key=%s, err=%v",
						rt.tenantID, shortPeerID(peerID), tableName, shortPeerID(key.String()), fullErr)
				}
			}
			continue
		}

		if err := rt.nodeMgr.dataSync.SendRowToPeer(peerID, tableName, key); err != nil {
			log.Printf("[MultiEngine:%s] full row send failed: peer=%s, table=%s, key=%s, err=%v",
				rt.tenantID, shortPeerID(peerID), tableName, shortPeerID(key.String()), err)
		}
	}
}

func (rt *tenantRuntime) handleMessage(peerID string, msg NetworkMessage) {
	if msg.Type != MsgTypeHeartbeat {
		rt.nodeMgr.MarkPeerSeen(peerID)
		if msg.GCFloor > 0 {
			rt.nodeMgr.ObservePeerGCFloor(peerID, msg.GCFloor)
		}
	}

	switch msg.Type {
	case MsgTypeHeartbeat:
		rt.nodeMgr.OnHeartbeat(peerID, msg.Clock, msg.GCFloor)

	case MsgTypeLocalFileChunk:
		if msg.RequestID == "" && !rt.nodeMgr.CanUseIncrementalWithPeer(peerID) {
			log.Printf("[MultiEngine:%s] skip local file chunk from blocked incremental peer: from=%s",
				rt.tenantID, shortPeerID(peerID))
			return
		}
		if rt.chunks == nil {
			log.Printf("[MultiEngine:%s] local file chunk receiver not initialized", rt.tenantID)
			return
		}
		if err := rt.chunks.HandleChunk(peerID, rt.db.FileStorageDir, msg); err != nil {
			log.Printf("[MultiEngine:%s] apply local file chunk failed: path=%s, idx=%d/%d, from=%s, err=%v",
				rt.tenantID, msg.FilePath, msg.ChunkIndex+1, msg.ChunkTotal, shortPeerID(peerID), err)
		}

	case MsgTypeRawData:
		if !rt.nodeMgr.CanUseIncrementalWithPeer(peerID) {
			log.Printf("[MultiEngine:%s] skip raw row from blocked incremental peer: from=%s",
				rt.tenantID, shortPeerID(peerID))
			return
		}
		if msg.Table != "" && msg.Key != "" && msg.RawData != nil {
			log.Printf("[MultiEngine:%s] received full row: table=%s, key=%s, from=%s",
				rt.tenantID, msg.Table, shortPeerID(msg.Key), shortPeerID(peerID))
			if err := rt.nodeMgr.dataSync.OnReceiveMergeWithFiles(msg.Table, msg.Key, msg.RawData, msg.Timestamp, msg.LocalFiles); err != nil {
				log.Printf("[MultiEngine:%s] merge failed: %v", rt.tenantID, err)
			}
		}

	case MsgTypeRawDelta:
		if !rt.nodeMgr.CanUseIncrementalWithPeer(peerID) {
			log.Printf("[MultiEngine:%s] skip raw delta from blocked incremental peer: from=%s",
				rt.tenantID, shortPeerID(peerID))
			return
		}
		if msg.Table != "" && msg.Key != "" && msg.RawData != nil {
			log.Printf("[MultiEngine:%s] received row delta: table=%s, key=%s, cols=%v, from=%s",
				rt.tenantID, msg.Table, shortPeerID(msg.Key), msg.Columns, shortPeerID(peerID))
			if err := rt.nodeMgr.dataSync.OnReceiveDeltaWithFiles(msg.Table, msg.Key, msg.Columns, msg.RawData, msg.Timestamp, msg.LocalFiles); err != nil {
				log.Printf("[MultiEngine:%s] delta merge failed: %v", rt.tenantID, err)
			}
		}

	case MsgTypeFetchRawRequest:
		rt.handleFetchRawRequest(peerID, msg)

	case MsgTypeFetchRawResponse:
		// handled by TenantNetwork request waiter

	case MsgTypeVersionDigest:
		rt.vs.OnReceiveDigest(peerID, &msg)

	case MsgTypeGCPrepare:
		rt.nodeMgr.HandleManualGCPrepare(peerID, msg)

	case MsgTypeGCCommit:
		rt.nodeMgr.HandleManualGCCommit(peerID, msg)

	case MsgTypeGCExecute:
		rt.nodeMgr.HandleManualGCExecute(peerID, msg)

	case MsgTypeGCAbort:
		rt.nodeMgr.HandleManualGCAbort(peerID, msg)
	}
}

func (rt *tenantRuntime) handleFetchRawRequest(peerID string, msg NetworkMessage) {
	if msg.Table == "" {
		return
	}

	rawRows, err := rt.nodeMgr.dataSync.ExportTableRawData(msg.Table)
	if err != nil {
		log.Printf("[MultiEngine:%s] export raw table failed: %v", rt.tenantID, err)
		return
	}

	for _, row := range rawRows {
		if err := rt.nodeMgr.dataSync.sendChunkedLocalFilesToPeer(peerID, msg.Table, row.Key, msg.RequestID, row.LocalFiles); err != nil {
			log.Printf("[MultiEngine:%s] send local file chunks failed: table=%s, key=%s, err=%v",
				rt.tenantID, msg.Table, shortPeerID(row.Key), err)
			continue
		}

		responseMsg := &NetworkMessage{
			Type:       MsgTypeFetchRawResponse,
			RequestID:  msg.RequestID,
			Table:      msg.Table,
			Key:        row.Key,
			RawData:    row.Data,
			LocalFiles: row.LocalFiles,
		}
		if err := rt.network.SendMessage(peerID, responseMsg); err != nil {
			log.Printf("[MultiEngine:%s] send row failed: %v", rt.tenantID, err)
		}
	}

	doneMsg := &NetworkMessage{
		Type:      MsgTypeFetchRawResponse,
		RequestID: msg.RequestID,
		Table:     msg.Table,
		Key:       fetchRawResponseDoneKey,
	}
	if err := rt.network.SendMessage(peerID, doneMsg); err != nil {
		log.Printf("[MultiEngine:%s] send fetch done marker failed: %v", rt.tenantID, err)
	}
}

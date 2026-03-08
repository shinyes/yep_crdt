package sync

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
)

type requestResponseNetwork interface {
	SendWithResponse(peerID string, msg *NetworkMessage, timeout time.Duration) (*NetworkMessage, error)
}

const merkleSyncTimeout = 5 * time.Second

func (vs *VersionSync) tryMerkleCompareAndSync(peerID string) error {
	network := vs.nodeMgr.getNetwork()
	if network == nil {
		return ErrNoNetwork
	}
	rr, ok := network.(requestResponseNetwork)
	if !ok {
		return fmt.Errorf("network does not support request/response merkle sync")
	}

	tableNames := vs.db.TableNames()
	if len(tableNames) == 0 {
		return nil
	}

	remoteRoots, err := vs.requestMerkleRoots(rr, peerID, tableNames)
	if err != nil {
		return err
	}

	var diffCount int
	var sendFailCount int
	var tableErrCount int

	for _, tableName := range tableNames {
		table := vs.db.Table(tableName)
		if table == nil {
			continue
		}

		localRoot, err := table.MerkleRootHash()
		if err != nil {
			tableErrCount++
			log.Printf("[VersionSync] read local merkle root failed: table=%s, err=%v", tableName, err)
			continue
		}
		remoteRoot := remoteRoots[tableName]
		if localRoot == remoteRoot {
			continue
		}
		if localRoot == "" {
			continue
		}

		keysToSend := make(map[uuid.UUID]struct{})
		if err := vs.collectMerkleDiffKeys(rr, peerID, tableName, table, 0, "", localRoot, remoteRoot, keysToSend); err != nil {
			tableErrCount++
			log.Printf("[VersionSync] collect merkle diff failed: table=%s, err=%v", tableName, err)
			continue
		}

		ordered := make([]string, 0, len(keysToSend))
		keyByStr := make(map[string]uuid.UUID, len(keysToSend))
		for k := range keysToSend {
			ks := k.String()
			ordered = append(ordered, ks)
			keyByStr[ks] = k
		}
		sort.Strings(ordered)

		for _, ks := range ordered {
			key := keyByStr[ks]
			if err := vs.sendRowWithRetry(peerID, tableName, key); err != nil {
				sendFailCount++
				log.Printf("[VersionSync] send merkle diff row failed: table=%s, key=%s, err=%v",
					tableName, shortPeerID(key.String()), err)
				continue
			}
			diffCount++
		}
	}

	log.Printf("[VersionSync] merkle sync sent %d diff rows to %s (send_failures=%d, table_errors=%d)",
		diffCount, shortPeerID(peerID), sendFailCount, tableErrCount)
	return nil
}

func (vs *VersionSync) requestMerkleRoots(rr requestResponseNetwork, peerID string, tableNames []string) (map[string]string, error) {
	reqBytes, err := marshalSyncWire(&MerkleRootRequest{Tables: append([]string(nil), tableNames...)})
	if err != nil {
		return nil, fmt.Errorf("marshal merkle root request failed: %w", err)
	}

	resp, err := rr.SendWithResponse(peerID, &NetworkMessage{
		Type:      MsgTypeMerkleRootReq,
		RawData:   reqBytes,
		GCFloor:   vs.db.GCFloor(),
		Timestamp: vs.db.Clock().Now(),
	}, merkleSyncTimeout)
	if err != nil {
		return nil, fmt.Errorf("send merkle root request failed: %w", err)
	}
	if resp == nil {
		return nil, fmt.Errorf("empty merkle root response")
	}
	if resp.Type != MsgTypeMerkleRootAck {
		return nil, fmt.Errorf("unexpected merkle root response type: %s", resp.Type)
	}

	var payload MerkleRootResponse
	if err := unmarshalSyncWire(resp.RawData, &payload); err != nil {
		return nil, fmt.Errorf("decode merkle root response failed: %w", err)
	}
	if payload.Roots == nil {
		return map[string]string{}, nil
	}
	return payload.Roots, nil
}

func (vs *VersionSync) requestMerkleNode(rr requestResponseNetwork, peerID string, tableName string, level int, prefix string) (*MerkleNodeResponse, error) {
	reqBytes, err := marshalSyncWire(&MerkleNodeRequest{
		Table:  tableName,
		Level:  level,
		Prefix: prefix,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal merkle node request failed: %w", err)
	}

	resp, err := rr.SendWithResponse(peerID, &NetworkMessage{
		Type:      MsgTypeMerkleNodeReq,
		RawData:   reqBytes,
		GCFloor:   vs.db.GCFloor(),
		Timestamp: vs.db.Clock().Now(),
	}, merkleSyncTimeout)
	if err != nil {
		return nil, fmt.Errorf("send merkle node request failed: %w", err)
	}
	if resp == nil {
		return nil, fmt.Errorf("empty merkle node response")
	}
	if resp.Type != MsgTypeMerkleNodeAck {
		return nil, fmt.Errorf("unexpected merkle node response type: %s", resp.Type)
	}

	var payload MerkleNodeResponse
	if err := unmarshalSyncWire(resp.RawData, &payload); err != nil {
		return nil, fmt.Errorf("decode merkle node response failed: %w", err)
	}
	if payload.Children == nil {
		payload.Children = map[string]string{}
	}
	return &payload, nil
}

func (vs *VersionSync) requestMerkleLeafRows(rr requestResponseNetwork, peerID string, tableName string, prefix string) (map[string]string, error) {
	reqBytes, err := marshalSyncWire(&MerkleLeafRequest{
		Table:  tableName,
		Prefix: prefix,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal merkle leaf request failed: %w", err)
	}

	resp, err := rr.SendWithResponse(peerID, &NetworkMessage{
		Type:      MsgTypeMerkleLeafReq,
		RawData:   reqBytes,
		GCFloor:   vs.db.GCFloor(),
		Timestamp: vs.db.Clock().Now(),
	}, merkleSyncTimeout)
	if err != nil {
		return nil, fmt.Errorf("send merkle leaf request failed: %w", err)
	}
	if resp == nil {
		return nil, fmt.Errorf("empty merkle leaf response")
	}
	if resp.Type != MsgTypeMerkleLeafAck {
		return nil, fmt.Errorf("unexpected merkle leaf response type: %s", resp.Type)
	}

	var payload MerkleLeafResponse
	if err := unmarshalSyncWire(resp.RawData, &payload); err != nil {
		return nil, fmt.Errorf("decode merkle leaf response failed: %w", err)
	}
	if payload.Rows == nil {
		return map[string]string{}, nil
	}
	return payload.Rows, nil
}

func (vs *VersionSync) collectMerkleDiffKeys(
	rr requestResponseNetwork,
	peerID string,
	tableName string,
	table *db.Table,
	level int,
	prefix string,
	localHash string,
	remoteHash string,
	out map[uuid.UUID]struct{},
) error {
	if localHash == remoteHash {
		return nil
	}
	if localHash == "" {
		return nil
	}

	if level >= db.MerkleMaxLevel() {
		localRows, err := table.MerkleLeafRows(prefix)
		if err != nil {
			return err
		}
		remoteRows, err := vs.requestMerkleLeafRows(rr, peerID, tableName, prefix)
		if err != nil {
			return err
		}

		for keyStr, localRowHash := range localRows {
			if remoteRowHash, ok := remoteRows[keyStr]; ok && remoteRowHash == localRowHash {
				continue
			}
			rowKey, err := uuid.Parse(keyStr)
			if err != nil {
				return fmt.Errorf("parse row key from leaf hash failed: key=%s, err=%w", keyStr, err)
			}
			out[rowKey] = struct{}{}
		}
		return nil
	}

	localChildren, err := table.MerkleChildren(level, prefix)
	if err != nil {
		return err
	}
	remoteNode, err := vs.requestMerkleNode(rr, peerID, tableName, level, prefix)
	if err != nil {
		return err
	}

	candidate := make(map[string]struct{}, len(localChildren)+len(remoteNode.Children))
	for nibble := range localChildren {
		candidate[nibble] = struct{}{}
	}
	for nibble := range remoteNode.Children {
		candidate[nibble] = struct{}{}
	}

	ordered := make([]string, 0, len(candidate))
	for nibble := range candidate {
		ordered = append(ordered, nibble)
	}
	sort.Strings(ordered)

	for _, nibble := range ordered {
		localChildHash := localChildren[nibble]
		remoteChildHash := remoteNode.Children[nibble]
		if localChildHash == remoteChildHash {
			continue
		}
		if localChildHash == "" {
			continue
		}
		childPrefix := prefix + nibble
		if err := vs.collectMerkleDiffKeys(rr, peerID, tableName, table, level+1, childPrefix, localChildHash, remoteChildHash, out); err != nil {
			return err
		}
	}
	return nil
}

func (vs *VersionSync) OnReceiveMerkleRootRequest(peerID string, msg *NetworkMessage) {
	if msg == nil {
		return
	}

	var req MerkleRootRequest
	if err := unmarshalSyncWire(msg.RawData, &req); err != nil {
		log.Printf("[VersionSync] decode merkle root request failed: %v", err)
		return
	}

	roots := make(map[string]string)
	for _, tableName := range req.Tables {
		table := vs.db.Table(tableName)
		if table == nil {
			continue
		}
		root, err := table.MerkleRootHash()
		if err != nil {
			log.Printf("[VersionSync] build merkle root failed: table=%s, err=%v", tableName, err)
			continue
		}
		roots[tableName] = root
	}

	payload, err := marshalSyncWire(&MerkleRootResponse{Roots: roots})
	if err != nil {
		log.Printf("[VersionSync] encode merkle root response failed: %v", err)
		return
	}
	vs.sendMerkleResponse(peerID, MsgTypeMerkleRootAck, msg.RequestID, payload)
}

func (vs *VersionSync) OnReceiveMerkleNodeRequest(peerID string, msg *NetworkMessage) {
	if msg == nil {
		return
	}

	var req MerkleNodeRequest
	if err := unmarshalSyncWire(msg.RawData, &req); err != nil {
		log.Printf("[VersionSync] decode merkle node request failed: %v", err)
		return
	}

	resp := MerkleNodeResponse{
		Table:    req.Table,
		Level:    req.Level,
		Prefix:   req.Prefix,
		Children: map[string]string{},
	}

	table := vs.db.Table(req.Table)
	if table != nil {
		nodeHash, err := table.MerkleNodeHash(req.Level, req.Prefix)
		if err != nil {
			log.Printf("[VersionSync] build merkle node hash failed: table=%s, level=%d, prefix=%s, err=%v",
				req.Table, req.Level, req.Prefix, err)
		} else {
			resp.NodeHash = nodeHash
			children, err := table.MerkleChildren(req.Level, req.Prefix)
			if err != nil {
				log.Printf("[VersionSync] build merkle node children failed: table=%s, level=%d, prefix=%s, err=%v",
					req.Table, req.Level, req.Prefix, err)
			} else {
				resp.Children = children
			}
		}
	}

	payload, err := marshalSyncWire(&resp)
	if err != nil {
		log.Printf("[VersionSync] encode merkle node response failed: %v", err)
		return
	}
	vs.sendMerkleResponse(peerID, MsgTypeMerkleNodeAck, msg.RequestID, payload)
}

func (vs *VersionSync) OnReceiveMerkleLeafRequest(peerID string, msg *NetworkMessage) {
	if msg == nil {
		return
	}

	var req MerkleLeafRequest
	if err := unmarshalSyncWire(msg.RawData, &req); err != nil {
		log.Printf("[VersionSync] decode merkle leaf request failed: %v", err)
		return
	}

	resp := MerkleLeafResponse{
		Table:  req.Table,
		Prefix: req.Prefix,
		Rows:   map[string]string{},
	}

	table := vs.db.Table(req.Table)
	if table != nil {
		rows, err := table.MerkleLeafRows(req.Prefix)
		if err != nil {
			log.Printf("[VersionSync] build merkle leaf rows failed: table=%s, prefix=%s, err=%v",
				req.Table, req.Prefix, err)
		} else {
			resp.Rows = rows
		}
	}

	payload, err := marshalSyncWire(&resp)
	if err != nil {
		log.Printf("[VersionSync] encode merkle leaf response failed: %v", err)
		return
	}
	vs.sendMerkleResponse(peerID, MsgTypeMerkleLeafAck, msg.RequestID, payload)
}

func (vs *VersionSync) sendMerkleResponse(peerID string, msgType string, requestID string, payload []byte) {
	network := vs.nodeMgr.getNetwork()
	if network == nil {
		log.Printf("[VersionSync] drop merkle response (no network): type=%s", msgType)
		return
	}

	resp := &NetworkMessage{
		Type:      msgType,
		RequestID: requestID,
		RawData:   payload,
		GCFloor:   vs.db.GCFloor(),
		Timestamp: vs.db.Clock().Now(),
	}
	if err := network.SendMessage(peerID, resp); err != nil {
		log.Printf("[VersionSync] send merkle response failed: peer=%s, type=%s, err=%v", shortPeerID(peerID), msgType, err)
	}
}

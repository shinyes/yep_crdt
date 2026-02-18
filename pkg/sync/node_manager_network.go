package sync

import (
	"context"
	"errors"
	"fmt"
)

// OnReceiveMerge applies one full-row CRDT payload.
func (nm *NodeManager) OnReceiveMerge(table string, key string, rawData []byte, timestamp int64) error {
	return nm.dataSync.OnReceiveMerge(table, key, rawData, timestamp)
}

// OnReceiveDelta applies one partial-row CRDT payload.
func (nm *NodeManager) OnReceiveDelta(table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return nm.dataSync.OnReceiveDelta(table, key, columns, rawData, timestamp)
}

// BroadcastHeartbeat sends heartbeat to all peers.
func (nm *NodeManager) BroadcastHeartbeat(clock int64) error {
	network := nm.getNetwork()
	if network == nil {
		return ErrNoNetwork
	}
	peers := nm.GetOnlineNodes()
	if len(peers) == 0 {
		return network.BroadcastHeartbeat(clock)
	}

	localFloor := nm.LocalGCFloor()
	var sendErrors []error
	for _, peerID := range peers {
		if peerID == "" || peerID == nm.localNodeID {
			continue
		}
		msg := &NetworkMessage{
			Type:      MsgTypeHeartbeat,
			Clock:     clock,
			GCFloor:   localFloor,
			Timestamp: clock,
		}
		if err := network.SendMessage(peerID, msg); err != nil {
			sendErrors = append(sendErrors, fmt.Errorf("peer=%s err=%w", shortPeerID(peerID), err))
		}
	}
	if len(sendErrors) > 0 {
		return errors.Join(sendErrors...)
	}
	return nil
}

// BroadcastRawData sends one full-row CRDT payload to all peers.
func (nm *NodeManager) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	network := nm.getNetwork()
	if network == nil {
		return ErrNoNetwork
	}
	return network.BroadcastRawData(table, key, rawData, timestamp)
}

// FetchRawTableData fetches all rows of a table from one remote peer.
func (nm *NodeManager) FetchRawTableData(sourceNodeID, tableName string) ([]RawRowData, error) {
	network := nm.getNetwork()
	if network == nil {
		return nil, ErrNoNetwork
	}
	return network.FetchRawTableData(sourceNodeID, tableName)
}

// FullSync runs full sync from one source node.
func (nm *NodeManager) FullSync(ctx context.Context, sourceNodeID string) (*SyncResult, error) {
	return nm.dataSync.FullSync(ctx, sourceNodeID)
}

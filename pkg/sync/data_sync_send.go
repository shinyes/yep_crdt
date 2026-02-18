package sync

import (
	"fmt"

	"github.com/google/uuid"
)

type networkMessageBroadcaster interface {
	Broadcast(msg *NetworkMessage) (int, error)
}

// BroadcastRow broadcasts the full raw CRDT row state.
func (dsm *DataSyncManager) BroadcastRow(tableName string, key uuid.UUID) error {
	dsm.mu.RLock()
	network := dsm.network
	dsm.mu.RUnlock()

	if network == nil {
		return ErrNoNetwork
	}

	table := dsm.db.Table(tableName)
	if table == nil {
		return fmt.Errorf("table does not exist: %s", tableName)
	}

	rawData, err := table.GetRawRow(key)
	if err != nil {
		return fmt.Errorf("get raw row failed: %w", err)
	}

	localFiles, err := dsm.buildLocalFilePayloadsFromRaw(rawData)
	if err != nil {
		return fmt.Errorf("build local file payloads failed: %w", err)
	}

	timestamp := dsm.db.Clock().Now()
	if len(localFiles) == 0 {
		return network.BroadcastRawData(tableName, key.String(), rawData, timestamp)
	}
	if hasChunkedLocalFiles(localFiles) {
		if err := dsm.broadcastChunkedLocalFiles(tableName, key.String(), localFiles); err != nil {
			return fmt.Errorf("broadcast chunked local files failed: %w", err)
		}
	}

	broadcaster, ok := network.(networkMessageBroadcaster)
	if !ok {
		return fmt.Errorf("network does not support LocalFileCRDT payload broadcast")
	}

	_, err = broadcaster.Broadcast(&NetworkMessage{
		Type:       MsgTypeRawData,
		Table:      tableName,
		Key:        key.String(),
		RawData:    rawData,
		LocalFiles: localFiles,
		GCFloor:    dsm.db.GCFloor(),
		Timestamp:  timestamp,
	})
	return err
}

// BroadcastRowDelta broadcasts only selected columns of a row.
func (dsm *DataSyncManager) BroadcastRowDelta(tableName string, key uuid.UUID, columns []string) error {
	dsm.mu.RLock()
	network := dsm.network
	dsm.mu.RUnlock()

	if network == nil {
		return ErrNoNetwork
	}

	if len(columns) == 0 {
		return dsm.BroadcastRow(tableName, key)
	}

	table := dsm.db.Table(tableName)
	if table == nil {
		return fmt.Errorf("table does not exist: %s", tableName)
	}

	rawData, err := table.GetRawRowColumns(key, columns)
	if err != nil {
		return fmt.Errorf("get raw row columns failed: %w", err)
	}

	localFiles, err := dsm.buildLocalFilePayloadsFromRaw(rawData)
	if err != nil {
		return fmt.Errorf("build local file payloads failed: %w", err)
	}

	timestamp := dsm.db.Clock().Now()
	if len(localFiles) == 0 {
		return network.BroadcastRawDelta(tableName, key.String(), columns, rawData, timestamp)
	}
	if hasChunkedLocalFiles(localFiles) {
		if err := dsm.broadcastChunkedLocalFiles(tableName, key.String(), localFiles); err != nil {
			return fmt.Errorf("broadcast chunked local files failed: %w", err)
		}
	}

	broadcaster, ok := network.(networkMessageBroadcaster)
	if !ok {
		return fmt.Errorf("network does not support LocalFileCRDT payload broadcast")
	}

	_, err = broadcaster.Broadcast(&NetworkMessage{
		Type:       MsgTypeRawDelta,
		Table:      tableName,
		Key:        key.String(),
		Columns:    append([]string(nil), columns...),
		RawData:    rawData,
		LocalFiles: localFiles,
		GCFloor:    dsm.db.GCFloor(),
		Timestamp:  timestamp,
	})
	return err
}

// SendRowToPeer sends full raw CRDT row state to one peer.
func (dsm *DataSyncManager) SendRowToPeer(targetNodeID string, tableName string, key uuid.UUID) error {
	dsm.mu.RLock()
	network := dsm.network
	dsm.mu.RUnlock()
	if network == nil {
		return ErrNoNetwork
	}

	table := dsm.db.Table(tableName)
	if table == nil {
		return fmt.Errorf("table does not exist: %s", tableName)
	}

	rawData, err := table.GetRawRow(key)
	if err != nil {
		return fmt.Errorf("get raw row failed: %w", err)
	}

	localFiles, err := dsm.buildLocalFilePayloadsFromRaw(rawData)
	if err != nil {
		return fmt.Errorf("build local file payloads failed: %w", err)
	}
	if hasChunkedLocalFiles(localFiles) {
		if err := dsm.sendChunkedLocalFilesToPeer(targetNodeID, tableName, key.String(), "", localFiles); err != nil {
			return fmt.Errorf("send chunked local files failed: %w", err)
		}
	}

	timestamp := dsm.db.Clock().Now()
	return network.SendMessage(targetNodeID, &NetworkMessage{
		Type:       MsgTypeRawData,
		Table:      tableName,
		Key:        key.String(),
		RawData:    rawData,
		LocalFiles: localFiles,
		GCFloor:    dsm.db.GCFloor(),
		Timestamp:  timestamp,
	})
}

// SendRowDeltaToPeer sends selected columns of one row to one peer.
func (dsm *DataSyncManager) SendRowDeltaToPeer(targetNodeID string, tableName string, key uuid.UUID, columns []string) error {
	dsm.mu.RLock()
	network := dsm.network
	dsm.mu.RUnlock()
	if network == nil {
		return ErrNoNetwork
	}

	if len(columns) == 0 {
		return dsm.SendRowToPeer(targetNodeID, tableName, key)
	}

	table := dsm.db.Table(tableName)
	if table == nil {
		return fmt.Errorf("table does not exist: %s", tableName)
	}

	rawData, err := table.GetRawRowColumns(key, columns)
	if err != nil {
		return fmt.Errorf("get raw row columns failed: %w", err)
	}

	localFiles, err := dsm.buildLocalFilePayloadsFromRaw(rawData)
	if err != nil {
		return fmt.Errorf("build local file payloads failed: %w", err)
	}
	if hasChunkedLocalFiles(localFiles) {
		if err := dsm.sendChunkedLocalFilesToPeer(targetNodeID, tableName, key.String(), "", localFiles); err != nil {
			return fmt.Errorf("send chunked local files failed: %w", err)
		}
	}

	timestamp := dsm.db.Clock().Now()
	return network.SendMessage(targetNodeID, &NetworkMessage{
		Type:       MsgTypeRawDelta,
		Table:      tableName,
		Key:        key.String(),
		Columns:    append([]string(nil), columns...),
		RawData:    rawData,
		LocalFiles: localFiles,
		GCFloor:    dsm.db.GCFloor(),
		Timestamp:  timestamp,
	})
}

package sync

import (
	"fmt"
	"time"
)

func (n *tenantScopedNetwork) SendHeartbeat(targetNodeID string, clock int64) error {
	return n.SendMessage(targetNodeID, &NetworkMessage{
		Type:  MsgTypeHeartbeat,
		Clock: clock,
	})
}

func (n *tenantScopedNetwork) BroadcastHeartbeat(clock int64) error {
	_, err := n.Broadcast(&NetworkMessage{
		Type:      MsgTypeHeartbeat,
		Clock:     clock,
		Timestamp: clock,
	})
	return err
}

func (n *tenantScopedNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	return n.SendMessage(targetNodeID, &NetworkMessage{
		Type:      MsgTypeRawData,
		Table:     table,
		Key:       key,
		RawData:   rawData,
		Timestamp: timestamp,
	})
}

func (n *tenantScopedNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	_, err := n.Broadcast(&NetworkMessage{
		Type:      MsgTypeRawData,
		Table:     table,
		Key:       key,
		RawData:   rawData,
		Timestamp: timestamp,
	})
	return err
}

func (n *tenantScopedNetwork) SendRawDelta(targetNodeID string, table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return n.SendMessage(targetNodeID, &NetworkMessage{
		Type:      MsgTypeRawDelta,
		Table:     table,
		Key:       key,
		Columns:   append([]string(nil), columns...),
		RawData:   rawData,
		Timestamp: timestamp,
	})
}

func (n *tenantScopedNetwork) BroadcastRawDelta(table string, key string, columns []string, rawData []byte, timestamp int64) error {
	_, err := n.Broadcast(&NetworkMessage{
		Type:      MsgTypeRawDelta,
		Table:     table,
		Key:       key,
		Columns:   append([]string(nil), columns...),
		RawData:   rawData,
		Timestamp: timestamp,
	})
	return err
}

func (n *tenantScopedNetwork) SendMessage(targetNodeID string, msg *NetworkMessage) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	cloned := *msg
	cloned.TenantID = n.tenantID
	return n.network.SendMessage(targetNodeID, &cloned)
}

func (n *tenantScopedNetwork) Broadcast(msg *NetworkMessage) (int, error) {
	if msg == nil {
		return 0, fmt.Errorf("message is nil")
	}
	cloned := *msg
	cloned.TenantID = n.tenantID
	return n.network.Broadcast(&cloned)
}

func (n *tenantScopedNetwork) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	return n.network.fetchRawTableDataWithTenant(sourceNodeID, tableName, n.tenantID, 30*time.Second)
}

var _ NetworkInterface = (*tenantScopedNetwork)(nil)

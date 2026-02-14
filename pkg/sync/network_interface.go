package sync

import (
	"fmt"
	"log"
)

// ErrNoNetwork indicates there is no registered network interface.
var ErrNoNetwork = fmt.Errorf("no network interface registered")

// NetworkInterface abstracts transport for sync messages.
type NetworkInterface interface {
	// SendHeartbeat sends a heartbeat to a target node.
	SendHeartbeat(targetNodeID string, clock int64) error

	// BroadcastHeartbeat broadcasts heartbeat to all peers.
	BroadcastHeartbeat(clock int64) error

	// SendRawData sends a raw CRDT row payload to a target node.
	SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error

	// BroadcastRawData broadcasts a raw CRDT row payload to all peers.
	BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error

	// SendMessage sends a custom control/data network message.
	SendMessage(targetNodeID string, msg *NetworkMessage) error

	// FetchRawTableData fetches all raw rows of a table from a remote node.
	FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error)
}

// DefaultNetwork is a no-op network implementation used by tests/mocks.
type DefaultNetwork struct{}

// NewDefaultNetwork creates a default network implementation.
func NewDefaultNetwork() NetworkInterface {
	return &DefaultNetwork{}
}

// SendHeartbeat sends heartbeat (default implementation).
func (n *DefaultNetwork) SendHeartbeat(targetNodeID string, clock int64) error {
	log.Printf("sending heartbeat to node %s, clock: %d", targetNodeID, clock)
	return nil
}

// BroadcastHeartbeat broadcasts heartbeat (default implementation).
func (n *DefaultNetwork) BroadcastHeartbeat(clock int64) error {
	log.Printf("broadcasting heartbeat, clock: %d", clock)
	return nil
}

// SendRawData sends raw CRDT payload (default implementation).
func (n *DefaultNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	log.Printf("sending raw data to node %s, table: %s, key: %s, size: %d bytes, timestamp: %d",
		targetNodeID, table, key, len(rawData), timestamp)
	return nil
}

// BroadcastRawData broadcasts raw CRDT payload (default implementation).
func (n *DefaultNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	log.Printf("broadcasting raw data, table: %s, key: %s, size: %d bytes, timestamp: %d",
		table, key, len(rawData), timestamp)
	return nil
}

// SendMessage sends a custom message (default implementation).
func (n *DefaultNetwork) SendMessage(targetNodeID string, msg *NetworkMessage) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}

	log.Printf("sending message to node %s, type: %s, request_id: %s",
		targetNodeID, msg.Type, msg.RequestID)
	return nil
}

// FetchRawTableData fetches raw table data (default implementation).
func (n *DefaultNetwork) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	log.Printf("fetching raw table data from node %s, table: %s", sourceNodeID, tableName)
	return []RawRowData{}, nil
}

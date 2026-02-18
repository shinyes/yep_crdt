package sync

import (
	"fmt"
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

	// SendRawDelta sends selected columns of one row to a target node.
	SendRawDelta(targetNodeID string, table string, key string, columns []string, rawData []byte, timestamp int64) error

	// BroadcastRawDelta broadcasts selected columns of one row to all peers.
	BroadcastRawDelta(table string, key string, columns []string, rawData []byte, timestamp int64) error

	// SendMessage sends a custom control/data network message.
	SendMessage(targetNodeID string, msg *NetworkMessage) error

	// FetchRawTableData fetches all raw rows of a table from a remote node.
	FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error)
}

// DefaultNetwork is an explicit "no network" implementation.
// All methods return ErrNoNetwork to avoid silently swallowing sync operations.
type DefaultNetwork struct{}

// NewDefaultNetwork creates a default network implementation.
func NewDefaultNetwork() NetworkInterface {
	return &DefaultNetwork{}
}

// SendHeartbeat returns ErrNoNetwork.
func (n *DefaultNetwork) SendHeartbeat(targetNodeID string, clock int64) error {
	return ErrNoNetwork
}

// BroadcastHeartbeat returns ErrNoNetwork.
func (n *DefaultNetwork) BroadcastHeartbeat(clock int64) error {
	return ErrNoNetwork
}

// SendRawData returns ErrNoNetwork.
func (n *DefaultNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	return ErrNoNetwork
}

// BroadcastRawData returns ErrNoNetwork.
func (n *DefaultNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	return ErrNoNetwork
}

// SendRawDelta returns ErrNoNetwork.
func (n *DefaultNetwork) SendRawDelta(targetNodeID string, table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return ErrNoNetwork
}

// BroadcastRawDelta returns ErrNoNetwork.
func (n *DefaultNetwork) BroadcastRawDelta(table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return ErrNoNetwork
}

// SendMessage returns ErrNoNetwork.
func (n *DefaultNetwork) SendMessage(targetNodeID string, msg *NetworkMessage) error {
	return ErrNoNetwork
}

// FetchRawTableData returns ErrNoNetwork.
func (n *DefaultNetwork) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	return nil, ErrNoNetwork
}

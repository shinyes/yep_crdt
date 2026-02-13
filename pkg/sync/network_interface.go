package sync

import (
	"fmt"
	"log"
)

// ErrNoNetwork 表示没有注册网络接口
var ErrNoNetwork = fmt.Errorf("no network interface registered")

// NetworkInterface 网络接口
// 使用者需要实现这个接口来传输心跳和数据。
type NetworkInterface interface {
	// SendHeartbeat 发送心跳
	SendHeartbeat(targetNodeID string, clock int64) error

	// BroadcastHeartbeat 广播心跳
	BroadcastHeartbeat(clock int64) error

	// SendRawData 发送原始 CRDT 字节到指定节点
	SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error

	// BroadcastRawData 广播原始 CRDT 字节到所有节点
	BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error

	// FetchRawTableData 获取远程节点指定表的所有原始数据
	FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error)
}

// DefaultNetwork 默认网络实现（仅用于测试）
type DefaultNetwork struct{}

// NewDefaultNetwork 创建默认网络实现
func NewDefaultNetwork() NetworkInterface {
	return &DefaultNetwork{}
}

// SendHeartbeat 发送心跳（默认实现）
func (n *DefaultNetwork) SendHeartbeat(targetNodeID string, clock int64) error {
	log.Printf("sending heartbeat to node %s, clock: %d", targetNodeID, clock)
	return nil
}

// BroadcastHeartbeat 广播心跳（默认实现）
func (n *DefaultNetwork) BroadcastHeartbeat(clock int64) error {
	log.Printf("broadcasting heartbeat, clock: %d", clock)
	return nil
}

// SendRawData 发送原始 CRDT 字节（默认实现）
func (n *DefaultNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	log.Printf("sending raw data to node %s, table: %s, key: %s, size: %d bytes, timestamp: %d",
		targetNodeID, table, key, len(rawData), timestamp)
	return nil
}

// BroadcastRawData 广播原始 CRDT 字节（默认实现）
func (n *DefaultNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	log.Printf("broadcasting raw data, table: %s, key: %s, size: %d bytes, timestamp: %d",
		table, key, len(rawData), timestamp)
	return nil
}

// FetchRawTableData 获取远程节点的原始表数据（默认实现）
func (n *DefaultNetwork) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	log.Printf("fetching raw table data from node %s, table: %s", sourceNodeID, tableName)

	// 返回空数据（默认实现不做实际传输）
	return []RawRowData{}, nil
}

package sync

import (
	"fmt"
	"log"
)

// ErrNoNetwork 表示没有注册网络接口
var ErrNoNetwork = fmt.Errorf("no network interface registered")

// NetworkInterface 网络接口
// 使用者需要实现这个接口来。传输心跳和数据
type NetworkInterface interface {
	// SendHeartbeat 发送心跳
	SendHeartbeat(targetNodeID string, clock int64) error

	// BroadcastHeartbeat 广播心跳
	BroadcastHeartbeat(clock int64) error

	// SendData 发送数据
	SendData(targetNodeID string, table string, key string, data any, timestamp int64) error

	// BroadcastData 广播数据
	BroadcastData(table string, key string, data any, timestamp int64) error

	// FetchData 获取数据
	FetchData(sourceNodeID string, tableName string) (map[string]map[string]any, error)
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

// SendData 发送数据（默认实现）
func (n *DefaultNetwork) SendData(targetNodeID string, table string, key string, data any, timestamp int64) error {
	log.Printf("sending data to node %s, table: %s, key: %s, timestamp: %d",
		targetNodeID, table, key, timestamp)
	return nil
}

// BroadcastData 广播数据（默认实现）
func (n *DefaultNetwork) BroadcastData(table string, key string, data any, timestamp int64) error {
	log.Printf("broadcasting data, table: %s, key: %s, timestamp: %d",
		table, key, timestamp)
	return nil
}

// FetchData 获取数据（默认实现）
func (n *DefaultNetwork) FetchData(sourceNodeID string, tableName string) (map[string]map[string]any, error) {
	log.Printf("fetching data from node %s, table: %s", sourceNodeID, tableName)
	
	// 返回模拟数据
	return map[string]map[string]any{
		"user-1": {
			"name": "Alice",
			"age":  30,
		},
		"user-2": {
			"name": "Bob",
			"age": 25,
		},
	}, nil
}

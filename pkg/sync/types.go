package sync

import (
	"time"
)

// NodeStatus 节点状态
type NodeStatus int

const (
	StatusOnline  NodeStatus = iota // 在线
	StatusOffline                // 离线
	StatusRejoining              // 重新加入中
)

// String 返回状态的字符串表示
func (s NodeStatus) String() string {
	switch s {
	case StatusOnline:
		return "Online"
	case StatusOffline:
		return "Offline"
	case StatusRejoining:
		return "Rejoining"
	default:
		return "Unknown"
	}
}

// NodeInfo 节点信息
type NodeInfo struct {
	ID             string    // 节点 ID
	LastHeartbeat  time.Time // 最后心跳时间
	IsOnline       bool      // 是否在线
	LastKnownClock int64     // 最后已知时钟值
	LastSyncTime   time.Time // 最后同步时间
}

// Config 节点管理器配置
type Config struct {
	HeartbeatInterval time.Duration // 心跳间隔
	TimeoutThreshold  time.Duration // 超时阈值
	ClockThreshold   int64        // 时钟差距阈值
	GCInterval      time.Duration // GC 间隔
	GCTimeOffset    time.Duration // GC 时间偏移
}

// Option 配置选项类型
type Option func(*Config)

// WithHeartbeatInterval 设置心跳间隔
func WithHeartbeatInterval(interval time.Duration) Option {
	return func(c *Config) {
		c.HeartbeatInterval = interval
	}
}

// WithTimeoutThreshold 设置超时阈值
func WithTimeoutThreshold(threshold time.Duration) Option {
	return func(c *Config) {
		c.TimeoutThreshold = threshold
	}
}

// WithClockThreshold 设置时钟差距阈值
func WithClockThreshold(threshold int64) Option {
	return func(c *Config) {
		c.ClockThreshold = threshold
	}
}

// WithGCInterval 设置 GC 间隔
func WithGCInterval(interval time.Duration) Option {
	return func(c *Config) {
		c.GCInterval = interval
	}
}

// WithGCTimeOffset 设置 GC 时间偏移
func WithGCTimeOffset(offset time.Duration) Option {
	return func(c *Config) {
		c.GCTimeOffset = offset
	}
}

// DefaultConfig 返回默认配置
func DefaultConfig() Config {
	return Config{
		HeartbeatInterval: 5 * time.Second,
		TimeoutThreshold:  1 * time.Minute,
		ClockThreshold:   5000, // 5 秒
		GCInterval:      1 * time.Minute,
		GCTimeOffset:    30 * time.Second,
	}
}

// GCResult GC 操作结果
type GCResult struct {
	TablesScanned     int   // 扫描的表数量
	RowsScanned       int   // 扫描的行数量
	TombstonesRemoved int   // 移除的墓碑数量
	Errors           []error // 遇到的错误
}

// SyncResult 同步操作结果
type SyncResult struct {
	TablesSynced     int   // 同步的表数量
	RowsSynced       int   // 同步的行数量
	RejectedCount     int   // 拒绝的数据数量
	Errors           []error // 遇到的错误
}

// HeartbeatMessage 心跳消息
type HeartbeatMessage struct {
	Type      string `json:"type"`      // 消息类型
	NodeID    string `json:"node_id"`   // 节点 ID
	Clock     int64  `json:"clock"`     // 时钟值
	Timestamp int64  `json:"timestamp"` // 时间戳
}

// DataMessage 数据同步消息
type DataMessage struct {
	Type      string      `json:"type"`      // 消息类型: data, fetch_request, fetch_response
	NodeID    string      `json:"node_id"`   // 节点 ID
	Table     string      `json:"table"`     // 表名
	Key       string      `json:"key"`       // 键
	Data      interface{} `json:"data"`      // 数据
	Timestamp int64       `json:"timestamp"` // 时间戳
}

// 消息类型常量
const (
	MsgTypeHeartbeat     = "heartbeat"
	MsgTypeData          = "data"
	MsgTypeFetchRequest  = "fetch_request"
	MsgTypeFetchResponse = "fetch_response"
)

// NetworkMessage 网络传输消息格式
type NetworkMessage struct {
	Type      string      `json:"type"`       // 消息类型
	TenantID  string      `json:"tenant_id,omitempty"`  // 租户 ID
	NodeID    string      `json:"node_id,omitempty"`   // 节点 ID（发送方）
	RequestID string      `json:"request_id,omitempty"` // 请求 ID（用于响应追踪）
	Table     string      `json:"table,omitempty"`     // 表名
	Key       string      `json:"key,omitempty"`       // 键
	Data      interface{} `json:"data,omitempty"`      // 数据
	Timestamp int64       `json:"timestamp"`  // 时间戳
	Clock     int64       `json:"clock,omitempty"`     // 时钟值（用于心跳）
}

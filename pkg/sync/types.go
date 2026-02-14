package sync

import (
	"time"
)

// NodeStatus 表示节点在线状态。
type NodeStatus int

const (
	StatusOnline    NodeStatus = iota // 节点在线。
	StatusOffline                     // 节点离线。
	StatusRejoining                   // 节点重连中。
)

// String 返回可读状态字符串。
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

// NodeInfo 保存单个节点的运行时信息。
type NodeInfo struct {
	ID             string    // 节点 ID。
	LastHeartbeat  time.Time // 最近一次心跳时间。
	IsOnline       bool      // 当前是否在线。
	LastKnownClock int64     // 最近一次已知逻辑时钟。
	LastSyncTime   time.Time // 最近一次同步时间。
}

// Config 控制同步子系统参数。
type Config struct {
	HeartbeatInterval time.Duration // 心跳发送间隔。
	TimeoutThreshold  time.Duration // 判定离线超时阈值。
	ClockThreshold    int64         // 时钟漂移阈值。
	GCInterval        time.Duration // GC 执行间隔。
	GCTimeOffset      time.Duration // GC 安全时间偏移量。
}

// Option 用于修改 Config。
type Option func(*Config)

// WithHeartbeatInterval 设置心跳间隔。
func WithHeartbeatInterval(interval time.Duration) Option {
	return func(c *Config) {
		c.HeartbeatInterval = interval
	}
}

// WithTimeoutThreshold 设置节点超时阈值。
func WithTimeoutThreshold(threshold time.Duration) Option {
	return func(c *Config) {
		c.TimeoutThreshold = threshold
	}
}

// WithClockThreshold 设置时钟漂移阈值。
func WithClockThreshold(threshold int64) Option {
	return func(c *Config) {
		c.ClockThreshold = threshold
	}
}

// WithGCInterval 设置 GC 执行间隔。
func WithGCInterval(interval time.Duration) Option {
	return func(c *Config) {
		c.GCInterval = interval
	}
}

// WithGCTimeOffset 设置 GC 安全时间偏移量。
func WithGCTimeOffset(offset time.Duration) Option {
	return func(c *Config) {
		c.GCTimeOffset = offset
	}
}

// DefaultConfig 返回默认同步配置。
func DefaultConfig() Config {
	return Config{
		HeartbeatInterval: 5 * time.Second,
		TimeoutThreshold:  1 * time.Minute,
		ClockThreshold:    5000, // 5 秒（毫秒单位）
		GCInterval:        1 * time.Minute,
		GCTimeOffset:      30 * time.Second,
	}
}

// GCResult 汇总一次 GC 执行结果。
type GCResult struct {
	TablesScanned     int     // 扫描的表数量。
	RowsScanned       int     // 扫描的行数量。
	TombstonesRemoved int     // 清理的墓碑数量。
	Errors            []error // 过程中的错误。
}

// SyncResult 汇总一次同步执行结果。
type SyncResult struct {
	TablesSynced  int     // 同步的表数量。
	RowsSynced    int     // 同步的行数量。
	RowsMerged    int     // 合并到已有行的数量。
	RowsCreated   int     // 新建行数量。
	RejectedCount int     // 被拒绝的行数量。
	Errors        []error // 过程中的错误。
}

// RawRowData 表示一行序列化 CRDT 传输数据。
type RawRowData struct {
	Key  string `json:"key"`  // UUID 字符串。
	Data []byte `json:"data"` // 序列化后的 MapCRDT 字节。
}

// HeartbeatMessage 为心跳消息载荷。
type HeartbeatMessage struct {
	Type      string `json:"type"`      // 消息类型。
	NodeID    string `json:"node_id"`   // 发送方节点 ID。
	Clock     int64  `json:"clock"`     // 发送方时钟值。
	Timestamp int64  `json:"timestamp"` // 发送时间戳。
}

// DataMessage 为历史兼容保留的旧同步消息格式。
type DataMessage struct {
	Type      string      `json:"type"`      // data/fetch_request/fetch_response
	NodeID    string      `json:"node_id"`   // 发送方节点 ID。
	Table     string      `json:"table"`     // 表名。
	Key       string      `json:"key"`       // 行键。
	Data      interface{} `json:"data"`      // 载荷。
	Timestamp int64       `json:"timestamp"` // 时间戳。
}

// 消息类型常量。
const (
	MsgTypeHeartbeat        = "heartbeat"
	MsgTypeData             = "data"               // 已废弃
	MsgTypeFetchRequest     = "fetch_request"      // 已废弃
	MsgTypeFetchResponse    = "fetch_response"     // 已废弃
	MsgTypeRawData          = "raw_data"           // 整行 CRDT 状态同步
	MsgTypeRawDelta         = "raw_delta"          // 列级 CRDT 状态同步
	MsgTypeFetchRawRequest  = "fetch_raw_request"  // 请求某表全部行数据
	MsgTypeFetchRawResponse = "fetch_raw_response" // 返回某行原始数据
	MsgTypeVersionDigest    = "version_digest"     // 版本摘要交换
)

// NetworkMessage 是节点间统一传输消息。
type NetworkMessage struct {
	Type      string      `json:"type"`
	TenantID  string      `json:"tenant_id,omitempty"`
	NodeID    string      `json:"node_id,omitempty"`
	RequestID string      `json:"request_id,omitempty"`
	Table     string      `json:"table,omitempty"`
	Key       string      `json:"key,omitempty"`
	Columns   []string    `json:"columns,omitempty"`
	Data      interface{} `json:"data,omitempty"`
	RawData   []byte      `json:"raw_data,omitempty"`
	Timestamp int64       `json:"timestamp"`
	Clock     int64       `json:"clock,omitempty"`
}

// TableDigest 是单表行哈希摘要。
type TableDigest struct {
	TableName string            `json:"table_name"`
	RowKeys   map[string]uint32 `json:"row_keys"` // key -> FNV 哈希
}

// VersionDigest 是节点完整版本摘要。
type VersionDigest struct {
	NodeID string        `json:"node_id"`
	Tables []TableDigest `json:"tables"`
}

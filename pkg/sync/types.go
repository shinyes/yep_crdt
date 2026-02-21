package sync

import "time"

// NodeStatus represents observed liveness state of a peer.
type NodeStatus int

const (
	StatusOnline NodeStatus = iota
	StatusOffline
	StatusRejoining
)

// String returns a human-readable status.
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

// NodeInfo stores runtime metadata for one peer.
type NodeInfo struct {
	ID                 string
	LastHeartbeat      time.Time
	IsOnline           bool
	LastKnownClock     int64
	LastKnownGCFloor   int64
	IncrementalBlocked bool
	LastSyncTime       time.Time
}

// Config controls sync runtime behavior.
type Config struct {
	HeartbeatInterval time.Duration
	TimeoutThreshold  time.Duration
	ClockThreshold    int64
}

// Option mutates Config.
type Option func(*Config)

// WithHeartbeatInterval sets heartbeat interval.
func WithHeartbeatInterval(interval time.Duration) Option {
	return func(c *Config) {
		c.HeartbeatInterval = interval
	}
}

// WithTimeoutThreshold sets offline timeout threshold.
func WithTimeoutThreshold(threshold time.Duration) Option {
	return func(c *Config) {
		c.TimeoutThreshold = threshold
	}
}

// WithClockThreshold sets clock drift threshold.
func WithClockThreshold(threshold int64) Option {
	return func(c *Config) {
		c.ClockThreshold = threshold
	}
}

// DefaultConfig returns runtime defaults.
func DefaultConfig() Config {
	return Config{
		HeartbeatInterval: 5 * time.Second,
		TimeoutThreshold:  1 * time.Minute,
		ClockThreshold:    5000,
	}
}

// SyncResult summarizes one sync run.
type SyncResult struct {
	TablesSynced  int
	RowsSynced    int
	RowsMerged    int
	RowsCreated   int
	RejectedCount int
	Errors        []error
}

// SyncedLocalFile carries one LocalFileCRDT payload for transport.
type SyncedLocalFile struct {
	Path    string `json:"path"`
	Hash    string `json:"hash,omitempty"`
	Size    int64  `json:"size,omitempty"`
	Chunked bool   `json:"chunked,omitempty"`
	Data    []byte `json:"data,omitempty"`
}

// RawRowData carries one serialized CRDT row.
type RawRowData struct {
	Key        string            `json:"key"`
	Data       []byte            `json:"data"`
	LocalFiles []SyncedLocalFile `json:"local_files,omitempty"`
}

// Message type constants.
const (
	MsgTypeHeartbeat        = "heartbeat"
	MsgTypeRawData          = "raw_data"
	MsgTypeRawDelta         = "raw_delta"
	MsgTypeLocalFileChunk   = "local_file_chunk"
	MsgTypeFetchRawRequest  = "fetch_raw_request"
	MsgTypeFetchRawResponse = "fetch_raw_response"
	MsgTypeVersionDigest    = "version_digest"

	// Manual GC coordination flow:
	// 1) gc_prepare -> gc_prepare_ack
	// 2) gc_commit  -> gc_commit_ack   (confirm only, no GC execution)
	// 3) gc_execute -> gc_execute_ack  (actual GC execution)
	// 4) gc_abort   -> gc_abort_ack    (best-effort pending cleanup)
	MsgTypeGCPrepare    = "gc_prepare"
	MsgTypeGCPrepareAck = "gc_prepare_ack"
	MsgTypeGCCommit     = "gc_commit"
	MsgTypeGCCommitAck  = "gc_commit_ack"
	MsgTypeGCExecute    = "gc_execute"
	MsgTypeGCExecuteAck = "gc_execute_ack"
	MsgTypeGCAbort      = "gc_abort"
	MsgTypeGCAbortAck   = "gc_abort_ack"
)

const (
	// fetchRawResponseDoneKey marks end-of-stream for fetch_raw_response batches.
	fetchRawResponseDoneKey = "__fetch_raw_done__"
)

// NetworkMessage is the unified transport payload.
type NetworkMessage struct {
	Type       string            `json:"type"`
	TenantID   string            `json:"tenant_id,omitempty"`
	NodeID     string            `json:"node_id,omitempty"`
	RequestID  string            `json:"request_id,omitempty"`
	Table      string            `json:"table,omitempty"`
	Key        string            `json:"key,omitempty"`
	Columns    []string          `json:"columns,omitempty"`
	RawData    []byte            `json:"raw_data,omitempty"`
	LocalFiles []SyncedLocalFile `json:"local_files,omitempty"`
	FilePath   string            `json:"file_path,omitempty"`
	FileHash   string            `json:"file_hash,omitempty"`
	FileSize   int64             `json:"file_size,omitempty"`
	ChunkIndex int               `json:"chunk_index,omitempty"`
	ChunkTotal int               `json:"chunk_total,omitempty"`
	ChunkData  []byte            `json:"chunk_data,omitempty"`

	Timestamp int64 `json:"timestamp"`
	Clock     int64 `json:"clock,omitempty"`
	GCFloor   int64 `json:"gc_floor,omitempty"`

	// Manual GC control fields.
	SafeTimestamp int64  `json:"safe_timestamp,omitempty"`
	Success       bool   `json:"success,omitempty"`
	Error         string `json:"error,omitempty"`
}

// TableDigest is one table digest for version sync.
type TableDigest struct {
	TableName string            `json:"table_name"`
	RowKeys   map[string]string `json:"row_keys"` // key -> SHA-256 digest
}

// VersionDigest is a per-node digest snapshot.
type VersionDigest struct {
	NodeID string        `json:"node_id"`
	Tables []TableDigest `json:"tables"`
}

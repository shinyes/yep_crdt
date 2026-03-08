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
	Path    string `json:"path" msgpack:"path"`
	Hash    string `json:"hash,omitempty" msgpack:"hash,omitempty"`
	Size    int64  `json:"size,omitempty" msgpack:"size,omitempty"`
	Chunked bool   `json:"chunked,omitempty" msgpack:"chunked,omitempty"`
	Data    []byte `json:"data,omitempty" msgpack:"data,omitempty"`
}

// RawRowData carries one serialized CRDT row.
type RawRowData struct {
	Key        string            `json:"key" msgpack:"key"`
	Data       []byte            `json:"data" msgpack:"data"`
	LocalFiles []SyncedLocalFile `json:"local_files,omitempty" msgpack:"local_files,omitempty"`
}

//go:generate go run ../../cmd/gen_sync_docs

// Message type constants.
const (
	MsgTypeHeartbeat        = "heartbeat"
	MsgTypeRawData          = "raw_data"
	MsgTypeRawDelta         = "raw_delta"
	MsgTypeLocalFileChunk   = "local_file_chunk"
	MsgTypeFetchRawRequest  = "fetch_raw_request"
	MsgTypeFetchRawResponse = "fetch_raw_response"
	MsgTypeVersionDigest    = "version_digest"
	MsgTypeMerkleRootReq    = "merkle_root_req"
	MsgTypeMerkleRootAck    = "merkle_root_ack"
	MsgTypeMerkleNodeReq    = "merkle_node_req"
	MsgTypeMerkleNodeAck    = "merkle_node_ack"
	MsgTypeMerkleLeafReq    = "merkle_leaf_req"
	MsgTypeMerkleLeafAck    = "merkle_leaf_ack"

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
	Type       string            `json:"type" msgpack:"type"`
	TenantID   string            `json:"tenant_id,omitempty" msgpack:"tenant_id,omitempty"`
	NodeID     string            `json:"node_id,omitempty" msgpack:"node_id,omitempty"`
	RequestID  string            `json:"request_id,omitempty" msgpack:"request_id,omitempty"`
	Table      string            `json:"table,omitempty" msgpack:"table,omitempty"`
	Key        string            `json:"key,omitempty" msgpack:"key,omitempty"`
	Columns    []string          `json:"columns,omitempty" msgpack:"columns,omitempty"`
	RawData    []byte            `json:"raw_data,omitempty" msgpack:"raw_data,omitempty"`
	LocalFiles []SyncedLocalFile `json:"local_files,omitempty" msgpack:"local_files,omitempty"`
	FilePath   string            `json:"file_path,omitempty" msgpack:"file_path,omitempty"`
	FileHash   string            `json:"file_hash,omitempty" msgpack:"file_hash,omitempty"`
	FileSize   int64             `json:"file_size,omitempty" msgpack:"file_size,omitempty"`
	ChunkIndex int               `json:"chunk_index,omitempty" msgpack:"chunk_index,omitempty"`
	ChunkTotal int               `json:"chunk_total,omitempty" msgpack:"chunk_total,omitempty"`
	ChunkData  []byte            `json:"chunk_data,omitempty" msgpack:"chunk_data,omitempty"`

	Timestamp int64 `json:"timestamp" msgpack:"timestamp"`
	Clock     int64 `json:"clock,omitempty" msgpack:"clock,omitempty"`
	GCFloor   int64 `json:"gc_floor,omitempty" msgpack:"gc_floor,omitempty"`

	// Manual GC control fields.
	SafeTimestamp int64  `json:"safe_timestamp,omitempty" msgpack:"safe_timestamp,omitempty"`
	Success       bool   `json:"success,omitempty" msgpack:"success,omitempty"`
	Error         string `json:"error,omitempty" msgpack:"error,omitempty"`
}

// TableDigest is one table digest for version sync.
type TableDigest struct {
	TableName string            `json:"table_name" msgpack:"table_name"`
	RowKeys   map[string]string `json:"row_keys" msgpack:"row_keys"` // key -> SHA-256 digest
}

// VersionDigest is a per-node digest snapshot.
type VersionDigest struct {
	NodeID string        `json:"node_id" msgpack:"node_id"`
	Tables []TableDigest `json:"tables" msgpack:"tables"`
}

// MerkleRootRequest requests table root hashes from a peer.
type MerkleRootRequest struct {
	Tables []string `json:"tables" msgpack:"tables"`
}

// MerkleRootResponse returns table root hashes.
type MerkleRootResponse struct {
	Roots map[string]string `json:"roots" msgpack:"roots"` // table -> root hash (hex)
}

// MerkleNodeRequest requests one node hash and its direct children hashes.
type MerkleNodeRequest struct {
	Table  string `json:"table" msgpack:"table"`
	Level  int    `json:"level" msgpack:"level"`
	Prefix string `json:"prefix" msgpack:"prefix"`
}

// MerkleNodeResponse returns one node hash and direct children hashes.
type MerkleNodeResponse struct {
	Table    string            `json:"table" msgpack:"table"`
	Level    int               `json:"level" msgpack:"level"`
	Prefix   string            `json:"prefix" msgpack:"prefix"`
	NodeHash string            `json:"node_hash" msgpack:"node_hash"` // hex
	Children map[string]string `json:"children" msgpack:"children"`   // child nibble -> hash hex
}

// MerkleLeafRequest requests row-level hashes under one leaf prefix.
type MerkleLeafRequest struct {
	Table  string `json:"table" msgpack:"table"`
	Prefix string `json:"prefix" msgpack:"prefix"`
}

// MerkleLeafResponse returns row-level hashes under one leaf prefix.
type MerkleLeafResponse struct {
	Table  string            `json:"table" msgpack:"table"`
	Prefix string            `json:"prefix" msgpack:"prefix"`
	Rows   map[string]string `json:"rows" msgpack:"rows"` // row key -> hash hex
}

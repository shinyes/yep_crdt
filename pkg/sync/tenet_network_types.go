package sync

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shinyes/tenet/api"
)

// TenantNetwork is the tenet-backed network for one tenant/database.
type TenantNetwork struct {
	tenantID string
	config   *TenetConfig
	tunnel   *api.Tunnel
	ctx      context.Context
	cancel   context.CancelFunc
	// requestCounter is used to generate low-allocation request IDs.
	requestCounter uint64
	// localNodeID caches tunnel.LocalID() to avoid repeated method calls on hot paths.
	localNodeID atomic.Value // string

	mu               sync.RWMutex
	peerHandlers     map[string]PeerMessageHandler
	broadcastHandler PeerMessageHandler
	tenantHandlers   map[string]PeerMessageHandler
	responseChannels map[string]pendingResponse // requestID -> waiter
	onPeerConnected  []func(peerID string)
	onPeerDropped    []func(peerID string)

	stats tenantNetworkStats
}

// TenetConfig defines network settings.
type TenetConfig struct {
	Password                 string
	ListenPort               int
	RelayNodes               []string
	EnableDebug              bool
	IdentityPath             string
	ChannelIDs               []string
	FetchResponseBuffer      int
	FetchResponseIdleTimeout time.Duration
}

// PeerMessageHandler handles incoming messages from peers.
type PeerMessageHandler struct {
	OnReceive func(peerID string, msg NetworkMessage)
}

type pendingResponse struct {
	peerID     string
	ch         chan NetworkMessage
	fetchCh    chan fetchRawResponseLite
	overflowCh chan struct{}
}

type fetchRawResponseLite struct {
	Type      string
	RequestID string
	Key       string
	RawData   []byte
}

const internalMsgTypePeerDisconnected = "__peer_disconnected__"
const defaultFetchResponseBuffer = 256
const defaultFetchResponseIdleTimeout = 1 * time.Second

func normalizeTenetConfig(config *TenetConfig) TenetConfig {
	cfg := *config
	if cfg.FetchResponseBuffer <= 0 {
		cfg.FetchResponseBuffer = defaultFetchResponseBuffer
	}
	if cfg.FetchResponseIdleTimeout <= 0 {
		cfg.FetchResponseIdleTimeout = defaultFetchResponseIdleTimeout
	}
	return cfg
}

type tenantNetworkStats struct {
	routedResponses         uint64
	unexpectedPeerResponses uint64
	droppedResponses        uint64
	fetchRequests           uint64
	fetchRequestSendErrors  uint64
	fetchRowsReceived       uint64
	fetchSuccess            uint64
	fetchTimeouts           uint64
	fetchPartialTimeouts    uint64
	fetchOverflows          uint64
	fetchPeerDisconnected   uint64
	fetchChannelClosed      uint64
}

// TenantNetworkStats is a snapshot of transport-level sync metrics.
type TenantNetworkStats struct {
	RoutedResponses         uint64
	UnexpectedPeerResponses uint64
	DroppedResponses        uint64
	FetchRequests           uint64
	FetchRequestSendErrors  uint64
	FetchRowsReceived       uint64
	FetchSuccess            uint64
	FetchTimeouts           uint64
	FetchPartialTimeouts    uint64
	FetchOverflows          uint64
	FetchPeerDisconnected   uint64
	FetchChannelClosed      uint64
	InFlightRequests        int
}

// Ensure TenantNetwork implements NetworkInterface.
var _ NetworkInterface = (*TenantNetwork)(nil)

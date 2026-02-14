package sync

import (
	"context"
	"encoding/json"
	"fmt"
	stdlog "log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shinyes/tenet/api"
	tenetlog "github.com/shinyes/tenet/log"
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
	responseChannels map[string]pendingResponse // requestID -> waiter
	onPeerConnected  []func(peerID string)
	onPeerDropped    []func(peerID string)
}

// TenetConfig defines network settings.
type TenetConfig struct {
	Password    string
	ListenPort  int
	RelayNodes  []string
	EnableDebug bool
}

// PeerMessageHandler handles incoming messages from peers.
type PeerMessageHandler struct {
	OnReceive func(peerID string, msg NetworkMessage)
}

type pendingResponse struct {
	peerID string
	ch     chan NetworkMessage
}

const internalMsgTypePeerDisconnected = "__peer_disconnected__"

func (tn *TenantNetwork) currentLocalID() string {
	if cached := tn.localNodeID.Load(); cached != nil {
		if id, ok := cached.(string); ok && id != "" {
			return id
		}
	}
	if tn.tunnel == nil {
		return ""
	}
	localID := tn.tunnel.LocalID()
	if localID != "" {
		tn.localNodeID.Store(localID)
	}
	return localID
}

func (tn *TenantNetwork) sendValue(peerID string, msg NetworkMessage) error {
	msg.TenantID = tn.tenantID
	msg.NodeID = tn.currentLocalID()
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixMilli()
	}

	payload, err := json.Marshal(&msg)
	if err != nil {
		return err
	}

	return tn.tunnel.Send(tn.tenantID, peerID, payload)
}

func (tn *TenantNetwork) broadcastValue(msg NetworkMessage) (int, error) {
	msg.TenantID = tn.tenantID
	msg.NodeID = tn.currentLocalID()
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixMilli()
	}

	payload, err := json.Marshal(&msg)
	if err != nil {
		return 0, err
	}

	return tn.tunnel.Broadcast(tn.tenantID, payload)
}

// NewTenantNetwork creates a tenet network for one tenant.
func NewTenantNetwork(tenantID string, config *TenetConfig) (*TenantNetwork, error) {
	if config == nil || config.Password == "" {
		return nil, fmt.Errorf("password is required")
	}

	opts := []api.Option{
		api.WithPassword(config.Password),
		api.WithListenPort(config.ListenPort),
		api.WithChannelID(tenantID),
	}

	if len(config.RelayNodes) > 0 {
		opts = append(opts, api.WithRelayNodes(config.RelayNodes))
	}

	if config.EnableDebug {
		logger := tenetlog.NewStdLogger(
			tenetlog.WithLevel(tenetlog.LevelDebug),
			tenetlog.WithPrefix(fmt.Sprintf("[tenet:%s]", tenantID)),
		)
		opts = append(opts, api.WithLogger(logger))
	} else {
		opts = append(opts, api.WithLogger(tenetlog.Nop()))
	}

	tunnel, err := api.NewTunnel(opts...)
	if err != nil {
		return nil, fmt.Errorf("create tunnel failed: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	tn := &TenantNetwork{
		tenantID:         tenantID,
		config:           config,
		tunnel:           tunnel,
		ctx:              ctx,
		cancel:           cancel,
		peerHandlers:     make(map[string]PeerMessageHandler),
		broadcastHandler: PeerMessageHandler{},
		responseChannels: make(map[string]pendingResponse),
	}
	if localID := tunnel.LocalID(); localID != "" {
		tn.localNodeID.Store(localID)
	}

	tn.setupCallbacks()
	return tn, nil
}

func (tn *TenantNetwork) setupCallbacks() {
	tn.tunnel.OnReceive(func(peerID string, data []byte) {
		tn.handleReceive(peerID, data)
	})

	tn.tunnel.OnPeerConnected(func(peerID string) {
		stdlog.Printf("[TenantNetwork:%s] node connected: %s", tn.tenantID, peerID)

		tn.mu.RLock()
		hooks := append([]func(string){}, tn.onPeerConnected...)
		tn.mu.RUnlock()

		for _, hook := range hooks {
			if hook != nil {
				hook(peerID)
			}
		}
	})

	tn.tunnel.OnPeerDisconnected(func(peerID string) {
		stdlog.Printf("[TenantNetwork:%s] node disconnected: %s", tn.tenantID, peerID)

		tn.mu.Lock()
		tn.dropPendingResponsesForPeerLocked(peerID, true)
		hooks := append([]func(string){}, tn.onPeerDropped...)
		tn.mu.Unlock()

		for _, hook := range hooks {
			if hook != nil {
				hook(peerID)
			}
		}
	})
}

func (tn *TenantNetwork) nextRequestID() string {
	seq := atomic.AddUint64(&tn.requestCounter, 1)
	return strconv.FormatUint(seq, 10)
}

func (tn *TenantNetwork) dropPendingResponsesForPeerLocked(peerID string, notify bool) {
	for requestID, waiter := range tn.responseChannels {
		if waiter.peerID != peerID {
			continue
		}
		delete(tn.responseChannels, requestID)
		if !notify {
			continue
		}
		select {
		case waiter.ch <- NetworkMessage{Type: internalMsgTypePeerDisconnected}:
		default:
		}
	}
}

func (tn *TenantNetwork) dropAllPendingResponsesLocked(notify bool) {
	for requestID, waiter := range tn.responseChannels {
		delete(tn.responseChannels, requestID)
		if !notify {
			continue
		}
		select {
		case waiter.ch <- NetworkMessage{Type: internalMsgTypePeerDisconnected}:
		default:
		}
	}
}

func (tn *TenantNetwork) handleReceive(peerID string, data []byte) {
	var msg NetworkMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		stdlog.Printf("[TenantNetwork:%s] parse message failed: %v", tn.tenantID, err)
		return
	}

	// Route only matched request IDs to in-flight waiters.
	if msg.RequestID != "" {
		tn.mu.RLock()
		waiter, ok := tn.responseChannels[msg.RequestID]
		tn.mu.RUnlock()

		if ok {
			select {
			case waiter.ch <- msg:
			default:
				stdlog.Printf("[TenantNetwork:%s] response channel is full", tn.tenantID)
			}
			return
		}
	}

	tn.mu.RLock()
	handler, ok := tn.peerHandlers[peerID]
	broadcastHandler := tn.broadcastHandler
	tn.mu.RUnlock()

	if ok && handler.OnReceive != nil {
		handler.OnReceive(peerID, msg)
	} else if broadcastHandler.OnReceive != nil {
		broadcastHandler.OnReceive(peerID, msg)
	}
}

// Start starts the tenant network.
func (tn *TenantNetwork) Start() error {
	stdlog.Printf("[TenantNetwork:%s] starting tunnel, port=%d", tn.tenantID, tn.config.ListenPort)
	if err := tn.tunnel.Start(); err != nil {
		return fmt.Errorf("start tunnel failed: %w", err)
	}
	localID := tn.tunnel.LocalID()
	if localID != "" {
		tn.localNodeID.Store(localID)
	}
	stdlog.Printf("[TenantNetwork:%s] local node ID: %s", tn.tenantID, localID)
	stdlog.Printf("[TenantNetwork:%s] local addr: %s", tn.tenantID, tn.tunnel.LocalAddr())
	return nil
}

// Stop stops the tenant network.
func (tn *TenantNetwork) Stop() {
	stdlog.Printf("[TenantNetwork:%s] stopping tunnel", tn.tenantID)
	tn.cancel()
	tn.mu.Lock()
	tn.dropAllPendingResponsesLocked(true)
	tn.mu.Unlock()
	_ = tn.tunnel.GracefulStop()
}

// Connect connects to another node.
func (tn *TenantNetwork) Connect(addr string) error {
	stdlog.Printf("[TenantNetwork:%s] connecting to %s", tn.tenantID, addr)
	return tn.tunnel.Connect(addr)
}

// LocalID returns local node ID.
func (tn *TenantNetwork) LocalID() string {
	return tn.tunnel.LocalID()
}

// LocalAddr returns local listen address.
func (tn *TenantNetwork) LocalAddr() string {
	return tn.tunnel.LocalAddr()
}

// Peers returns connected peers.
func (tn *TenantNetwork) Peers() []string {
	return tn.tunnel.Peers()
}

// TenantID returns tenant ID.
func (tn *TenantNetwork) TenantID() string {
	return tn.tenantID
}

// SetPeerHandler sets a handler for one peer.
func (tn *TenantNetwork) SetPeerHandler(peerID string, handler PeerMessageHandler) {
	tn.mu.Lock()
	tn.peerHandlers[peerID] = handler
	tn.mu.Unlock()
}

// SetBroadcastHandler sets a handler for all incoming messages.
func (tn *TenantNetwork) SetBroadcastHandler(handler PeerMessageHandler) {
	tn.mu.Lock()
	tn.broadcastHandler = handler
	tn.mu.Unlock()
}

// AddPeerConnectedHandler adds a callback without overriding internal callbacks.
func (tn *TenantNetwork) AddPeerConnectedHandler(handler func(peerID string)) {
	if handler == nil {
		return
	}
	tn.mu.Lock()
	tn.onPeerConnected = append(tn.onPeerConnected, handler)
	tn.mu.Unlock()
}

// AddPeerDisconnectedHandler adds a callback without overriding internal callbacks.
func (tn *TenantNetwork) AddPeerDisconnectedHandler(handler func(peerID string)) {
	if handler == nil {
		return
	}
	tn.mu.Lock()
	tn.onPeerDropped = append(tn.onPeerDropped, handler)
	tn.mu.Unlock()
}

// RemovePeerHandler removes one peer handler.
func (tn *TenantNetwork) RemovePeerHandler(peerID string) {
	tn.mu.Lock()
	delete(tn.peerHandlers, peerID)
	tn.dropPendingResponsesForPeerLocked(peerID, true)
	tn.mu.Unlock()
}

// Send sends a message to one peer.
func (tn *TenantNetwork) Send(peerID string, msg *NetworkMessage) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	return tn.sendValue(peerID, *msg)
}

// Broadcast broadcasts a message to all peers in the same tenant channel.
func (tn *TenantNetwork) Broadcast(msg *NetworkMessage) (int, error) {
	if msg == nil {
		return 0, fmt.Errorf("message is nil")
	}
	return tn.broadcastValue(*msg)
}

// SendMessage sends a custom message to one peer.
func (tn *TenantNetwork) SendMessage(targetNodeID string, msg *NetworkMessage) error {
	return tn.Send(targetNodeID, msg)
}

// SendWithResponse sends a request and waits for a response.
func (tn *TenantNetwork) SendWithResponse(peerID string, msg *NetworkMessage, timeout time.Duration) (*NetworkMessage, error) {
	if msg == nil {
		return nil, fmt.Errorf("message is nil")
	}
	requestID := tn.nextRequestID()
	reqMsg := *msg
	reqMsg.RequestID = requestID

	responseCh := make(chan NetworkMessage, 1)
	tn.mu.Lock()
	tn.responseChannels[requestID] = pendingResponse{peerID: peerID, ch: responseCh}
	tn.mu.Unlock()

	defer func() {
		tn.mu.Lock()
		delete(tn.responseChannels, requestID)
		tn.mu.Unlock()
	}()

	if err := tn.sendValue(peerID, reqMsg); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	select {
	case response, ok := <-responseCh:
		if !ok || response.Type == internalMsgTypePeerDisconnected {
			return nil, fmt.Errorf("peer disconnected before response")
		}
		return &response, nil
	case <-timer.C:
		return nil, fmt.Errorf("timeout waiting for response")
	}
}

// SendHeartbeat sends heartbeat to one peer.
func (tn *TenantNetwork) SendHeartbeat(targetNodeID string, clock int64) error {
	msg := NetworkMessage{
		Type:  MsgTypeHeartbeat,
		Clock: clock,
	}
	return tn.sendValue(targetNodeID, msg)
}

// BroadcastHeartbeat broadcasts heartbeat.
func (tn *TenantNetwork) BroadcastHeartbeat(clock int64) error {
	msg := NetworkMessage{
		Type:      MsgTypeHeartbeat,
		Clock:     clock,
		Timestamp: clock,
	}
	_, err := tn.broadcastValue(msg)
	return err
}

// SendRawData sends one raw row payload to one peer.
func (tn *TenantNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	msg := NetworkMessage{
		Type:      MsgTypeRawData,
		Table:     table,
		Key:       key,
		RawData:   rawData,
		Timestamp: timestamp,
	}
	return tn.sendValue(targetNodeID, msg)
}

// BroadcastRawData broadcasts one raw row payload.
func (tn *TenantNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	msg := NetworkMessage{
		Type:      MsgTypeRawData,
		Table:     table,
		Key:       key,
		RawData:   rawData,
		Timestamp: timestamp,
	}
	count, err := tn.broadcastValue(msg)
	if err != nil {
		stdlog.Printf("[TenantNetwork:%s] broadcast raw data failed: %v", tn.tenantID, err)
	} else {
		stdlog.Printf("[TenantNetwork:%s] broadcast raw data to %d peers, table=%s, key=%s", tn.tenantID, count, table, key)
	}
	return err
}

// SendRawDelta sends one column-level row delta payload to one peer.
func (tn *TenantNetwork) SendRawDelta(targetNodeID string, table string, key string, columns []string, rawData []byte, timestamp int64) error {
	msg := NetworkMessage{
		Type:      MsgTypeRawDelta,
		Table:     table,
		Key:       key,
		Columns:   append([]string(nil), columns...),
		RawData:   rawData,
		Timestamp: timestamp,
	}
	return tn.sendValue(targetNodeID, msg)
}

// BroadcastRawDelta broadcasts one column-level row delta payload.
func (tn *TenantNetwork) BroadcastRawDelta(table string, key string, columns []string, rawData []byte, timestamp int64) error {
	msg := NetworkMessage{
		Type:      MsgTypeRawDelta,
		Table:     table,
		Key:       key,
		Columns:   append([]string(nil), columns...),
		RawData:   rawData,
		Timestamp: timestamp,
	}
	count, err := tn.broadcastValue(msg)
	if err != nil {
		stdlog.Printf("[TenantNetwork:%s] broadcast raw delta failed: %v", tn.tenantID, err)
	} else {
		stdlog.Printf("[TenantNetwork:%s] broadcast raw delta to %d peers, table=%s, key=%s, columns=%v",
			tn.tenantID, count, table, key, columns)
	}
	return err
}

// FetchRawTableData fetches raw table data with default timeout.
func (tn *TenantNetwork) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	return tn.FetchRawTableDataWithTimeout(sourceNodeID, tableName, 30*time.Second)
}

// FetchRawTableDataWithTimeout fetches raw table data with timeout.
func (tn *TenantNetwork) FetchRawTableDataWithTimeout(sourceNodeID string, tableName string, timeout time.Duration) ([]RawRowData, error) {
	requestID := tn.nextRequestID()
	msg := NetworkMessage{
		Type:      MsgTypeFetchRawRequest,
		RequestID: requestID,
		Table:     tableName,
		Timestamp: time.Now().UnixMilli(),
	}

	responseCh := make(chan NetworkMessage, 64)
	tn.mu.Lock()
	tn.responseChannels[requestID] = pendingResponse{peerID: sourceNodeID, ch: responseCh}
	tn.mu.Unlock()

	defer func() {
		tn.mu.Lock()
		delete(tn.responseChannels, requestID)
		tn.mu.Unlock()
	}()

	if err := tn.sendValue(sourceNodeID, msg); err != nil {
		return nil, err
	}

	return collectFetchRawResponses(responseCh, timeout)
}

func collectFetchRawResponses(responseCh <-chan NetworkMessage, timeout time.Duration) ([]RawRowData, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	overallTimer := time.NewTimer(timeout)
	defer overallTimer.Stop()

	var idleTimer *time.Timer
	var idleC <-chan time.Time
	stopIdle := func() {
		if idleTimer == nil {
			return
		}
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
	}
	defer stopIdle()

	resetIdle := func() {
		if idleTimer == nil {
			idleTimer = time.NewTimer(fetchRawResponseIdleTimeout)
			idleC = idleTimer.C
			return
		}
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(fetchRawResponseIdleTimeout)
	}

	rows := make([]RawRowData, 0, cap(responseCh))
	for {
		select {
		case msg, ok := <-responseCh:
			if !ok {
				if len(rows) > 0 {
					return rows, nil
				}
				return nil, fmt.Errorf("response channel closed before data arrived")
			}
			if msg.Type == internalMsgTypePeerDisconnected {
				return nil, fmt.Errorf("peer disconnected before response")
			}
			if msg.Type != MsgTypeFetchRawResponse {
				continue
			}
			if msg.Key == fetchRawResponseDoneKey {
				return rows, nil
			}
			if msg.Key == "" || msg.RawData == nil {
				continue
			}

			// RawData already comes from JSON unmarshal into a per-message byte slice.
			// Reusing it avoids an extra copy on full-sync hot path.
			rows = append(rows, RawRowData{
				Key:  msg.Key,
				Data: msg.RawData,
			})
			resetIdle()

		case <-idleC:
			// Backward compatibility: old peers may not send done marker.
			return rows, nil

		case <-overallTimer.C:
			if len(rows) > 0 {
				return rows, nil
			}
			return nil, fmt.Errorf("timeout waiting for response")
		}
	}
}

// Ensure TenantNetwork implements NetworkInterface.
var _ NetworkInterface = (*TenantNetwork)(nil)

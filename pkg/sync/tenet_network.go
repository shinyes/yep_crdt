package sync

import (
	"context"
	"encoding/json"
	"fmt"
	stdlog "log"
	"sync"
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
	OnReceive func(peerID string, msg *NetworkMessage)
}

type pendingResponse struct {
	peerID string
	ch     chan *NetworkMessage
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
		for requestID, waiter := range tn.responseChannels {
			if waiter.peerID == peerID {
				delete(tn.responseChannels, requestID)
			}
		}
		hooks := append([]func(string){}, tn.onPeerDropped...)
		tn.mu.Unlock()

		for _, hook := range hooks {
			if hook != nil {
				hook(peerID)
			}
		}
	})
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
			case waiter.ch <- &msg:
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
		handler.OnReceive(peerID, &msg)
	} else if broadcastHandler.OnReceive != nil {
		broadcastHandler.OnReceive(peerID, &msg)
	}
}

// Start starts the tenant network.
func (tn *TenantNetwork) Start() error {
	stdlog.Printf("[TenantNetwork:%s] starting tunnel, port=%d", tn.tenantID, tn.config.ListenPort)
	if err := tn.tunnel.Start(); err != nil {
		return fmt.Errorf("start tunnel failed: %w", err)
	}
	stdlog.Printf("[TenantNetwork:%s] local node ID: %s", tn.tenantID, tn.tunnel.LocalID())
	stdlog.Printf("[TenantNetwork:%s] local addr: %s", tn.tenantID, tn.tunnel.LocalAddr())
	return nil
}

// Stop stops the tenant network.
func (tn *TenantNetwork) Stop() {
	stdlog.Printf("[TenantNetwork:%s] stopping tunnel", tn.tenantID)
	tn.cancel()
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
	for requestID, waiter := range tn.responseChannels {
		if waiter.peerID == peerID {
			delete(tn.responseChannels, requestID)
		}
	}
	tn.mu.Unlock()
}

// Send sends a message to one peer.
func (tn *TenantNetwork) Send(peerID string, msg *NetworkMessage) error {
	msg.TenantID = tn.tenantID
	msg.NodeID = tn.tunnel.LocalID()
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixMilli()
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return tn.tunnel.Send(tn.tenantID, peerID, payload)
}

// Broadcast broadcasts a message to all peers in the same tenant channel.
func (tn *TenantNetwork) Broadcast(msg *NetworkMessage) (int, error) {
	msg.TenantID = tn.tenantID
	msg.NodeID = tn.tunnel.LocalID()
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixMilli()
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return 0, err
	}

	return tn.tunnel.Broadcast(tn.tenantID, payload)
}

// SendMessage sends a custom message to one peer.
func (tn *TenantNetwork) SendMessage(targetNodeID string, msg *NetworkMessage) error {
	return tn.Send(targetNodeID, msg)
}

// SendWithResponse sends a request and waits for a response.
func (tn *TenantNetwork) SendWithResponse(peerID string, msg *NetworkMessage, timeout time.Duration) (*NetworkMessage, error) {
	requestID := fmt.Sprintf("%s-%d", tn.tunnel.LocalID(), time.Now().UnixNano())
	msg.RequestID = requestID

	responseCh := make(chan *NetworkMessage, 1)
	tn.mu.Lock()
	tn.responseChannels[requestID] = pendingResponse{peerID: peerID, ch: responseCh}
	tn.mu.Unlock()

	defer func() {
		tn.mu.Lock()
		delete(tn.responseChannels, requestID)
		tn.mu.Unlock()
	}()

	if err := tn.Send(peerID, msg); err != nil {
		return nil, err
	}

	select {
	case response := <-responseCh:
		if response == nil {
			return nil, fmt.Errorf("peer disconnected before response")
		}
		return response, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for response")
	}
}

// SendHeartbeat sends heartbeat to one peer.
func (tn *TenantNetwork) SendHeartbeat(targetNodeID string, clock int64) error {
	msg := &NetworkMessage{
		Type:  MsgTypeHeartbeat,
		Clock: clock,
	}
	return tn.Send(targetNodeID, msg)
}

// BroadcastHeartbeat broadcasts heartbeat.
func (tn *TenantNetwork) BroadcastHeartbeat(clock int64) error {
	msg := &NetworkMessage{
		Type:      MsgTypeHeartbeat,
		Clock:     clock,
		Timestamp: clock,
	}
	_, err := tn.Broadcast(msg)
	return err
}

// SendRawData sends one raw row payload to one peer.
func (tn *TenantNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	msg := &NetworkMessage{
		Type:      MsgTypeRawData,
		Table:     table,
		Key:       key,
		RawData:   rawData,
		Timestamp: timestamp,
	}
	return tn.Send(targetNodeID, msg)
}

// BroadcastRawData broadcasts one raw row payload.
func (tn *TenantNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	msg := &NetworkMessage{
		Type:      MsgTypeRawData,
		Table:     table,
		Key:       key,
		RawData:   rawData,
		Timestamp: timestamp,
	}
	count, err := tn.Broadcast(msg)
	if err != nil {
		stdlog.Printf("[TenantNetwork:%s] broadcast raw data failed: %v", tn.tenantID, err)
	} else {
		stdlog.Printf("[TenantNetwork:%s] broadcast raw data to %d peers, table=%s, key=%s", tn.tenantID, count, table, key)
	}
	return err
}

// SendRawDelta sends one column-level row delta payload to one peer.
func (tn *TenantNetwork) SendRawDelta(targetNodeID string, table string, key string, columns []string, rawData []byte, timestamp int64) error {
	msg := &NetworkMessage{
		Type:      MsgTypeRawDelta,
		Table:     table,
		Key:       key,
		Columns:   append([]string(nil), columns...),
		RawData:   rawData,
		Timestamp: timestamp,
	}
	return tn.Send(targetNodeID, msg)
}

// BroadcastRawDelta broadcasts one column-level row delta payload.
func (tn *TenantNetwork) BroadcastRawDelta(table string, key string, columns []string, rawData []byte, timestamp int64) error {
	msg := &NetworkMessage{
		Type:      MsgTypeRawDelta,
		Table:     table,
		Key:       key,
		Columns:   append([]string(nil), columns...),
		RawData:   rawData,
		Timestamp: timestamp,
	}
	count, err := tn.Broadcast(msg)
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
	msg := &NetworkMessage{
		Type:      MsgTypeFetchRawRequest,
		Table:     tableName,
		Timestamp: time.Now().UnixMilli(),
	}

	response, err := tn.SendWithResponse(sourceNodeID, msg, timeout)
	if err != nil {
		return nil, err
	}

	if response.RawData == nil {
		return nil, nil
	}

	result := []RawRowData{{
		Key:  response.Key,
		Data: response.RawData,
	}}
	return result, nil
}

// Ensure TenantNetwork implements NetworkInterface.
var _ NetworkInterface = (*TenantNetwork)(nil)

package sync

import (
	"fmt"
	stdlog "log"
	"time"
)

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
			return nil, fmt.Errorf("%w", ErrPeerDisconnectedBeforeResponse)
		}
		return &response, nil
	case <-timer.C:
		return nil, fmt.Errorf("%w", ErrTimeoutWaitingResponse)
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

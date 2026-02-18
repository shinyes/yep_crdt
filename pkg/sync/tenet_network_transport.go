package sync

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

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
	channelID := msg.TenantID
	if channelID == "" {
		channelID = tn.tenantID
	}
	msg.TenantID = channelID
	msg.NodeID = tn.currentLocalID()
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixMilli()
	}

	payload, err := msgpack.Marshal(&msg)
	if err != nil {
		return err
	}

	return tn.tunnel.Send(channelID, peerID, payload)
}

func (tn *TenantNetwork) broadcastValue(msg NetworkMessage) (int, error) {
	channelID := msg.TenantID
	if channelID == "" {
		channelID = tn.tenantID
	}
	msg.TenantID = channelID
	msg.NodeID = tn.currentLocalID()
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixMilli()
	}

	payload, err := msgpack.Marshal(&msg)
	if err != nil {
		return 0, err
	}

	return tn.tunnel.Broadcast(channelID, payload)
}

func (tn *TenantNetwork) nextRequestID() string {
	seq := atomic.AddUint64(&tn.requestCounter, 1)
	localID := tn.currentLocalID()
	if localID == "" {
		return strconv.FormatUint(seq, 10)
	}
	return localID + "-" + strconv.FormatUint(seq, 10)
}

func isWaiterResponseType(msgType string) bool {
	switch msgType {
	case MsgTypeFetchRawResponse, MsgTypeGCPrepareAck, MsgTypeGCCommitAck, MsgTypeGCExecuteAck, MsgTypeGCAbortAck:
		return true
	default:
		return false
	}
}

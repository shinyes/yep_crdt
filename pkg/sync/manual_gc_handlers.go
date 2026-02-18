package sync

import (
	"fmt"
	"log"
)

func (nm *NodeManager) HandleManualGCPrepare(peerID string, msg NetworkMessage) {
	resp := &NetworkMessage{
		Type:      MsgTypeGCPrepareAck,
		RequestID: msg.RequestID,
	}
	if msg.RequestID == "" {
		resp.Success = false
		resp.Error = "missing request id"
		nm.sendManualGCResponse(peerID, resp)
		return
	}

	resp.SafeTimestamp = nm.CalculateSafeTimestamp()
	resp.Success = true
	nm.sendManualGCResponse(peerID, resp)
}

func (nm *NodeManager) HandleManualGCCommit(peerID string, msg NetworkMessage) {
	resp := &NetworkMessage{
		Type:          MsgTypeGCCommitAck,
		RequestID:     msg.RequestID,
		SafeTimestamp: msg.SafeTimestamp,
	}
	if msg.RequestID == "" {
		resp.Success = false
		resp.Error = "missing request id"
		nm.sendManualGCResponse(peerID, resp)
		return
	}
	if msg.SafeTimestamp <= 0 {
		resp.Success = false
		resp.Error = "invalid safe timestamp"
		nm.sendManualGCResponse(peerID, resp)
		return
	}

	currentSafeTimestamp := nm.CalculateSafeTimestamp()
	if msg.SafeTimestamp > currentSafeTimestamp {
		resp.Success = false
		resp.Error = fmt.Sprintf("safe timestamp %d exceeds local safe timestamp %d",
			msg.SafeTimestamp, currentSafeTimestamp)
		nm.sendManualGCResponse(peerID, resp)
		return
	}

	nm.mu.Lock()
	if nm.manualGCPending == nil {
		nm.manualGCPending = make(map[string]int64)
	}
	nm.manualGCPending[peerID] = msg.SafeTimestamp
	nm.mu.Unlock()

	resp.Success = true
	nm.sendManualGCResponse(peerID, resp)
}

func (nm *NodeManager) HandleManualGCExecute(peerID string, msg NetworkMessage) {
	resp := &NetworkMessage{
		Type:          MsgTypeGCExecuteAck,
		RequestID:     msg.RequestID,
		SafeTimestamp: msg.SafeTimestamp,
	}
	if msg.RequestID == "" {
		resp.Success = false
		resp.Error = "missing request id"
		nm.sendManualGCResponse(peerID, resp)
		return
	}
	if msg.SafeTimestamp <= 0 {
		resp.Success = false
		resp.Error = "invalid safe timestamp"
		nm.sendManualGCResponse(peerID, resp)
		return
	}

	nm.mu.RLock()
	pendingSafeTimestamp, exists := nm.manualGCPending[peerID]
	nm.mu.RUnlock()
	if !exists {
		resp.Success = false
		resp.Error = "no pending manual gc commit"
		nm.sendManualGCResponse(peerID, resp)
		return
	}
	if pendingSafeTimestamp != msg.SafeTimestamp {
		resp.Success = false
		resp.Error = fmt.Sprintf("pending safe timestamp mismatch: pending=%d request=%d",
			pendingSafeTimestamp, msg.SafeTimestamp)
		nm.sendManualGCResponse(peerID, resp)
		return
	}

	result := nm.db.GC(msg.SafeTimestamp)
	if len(result.Errors) > 0 {
		resp.Success = false
		resp.Error = fmt.Sprintf("gc returned %d errors", len(result.Errors))
		nm.sendManualGCResponse(peerID, resp)
		return
	}
	if err := nm.SetLocalGCFloor(msg.SafeTimestamp); err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("update local gc floor failed: %v", err)
		nm.sendManualGCResponse(peerID, resp)
		return
	}

	nm.mu.Lock()
	// Keep delete idempotent and avoid removing a newer round accidentally.
	if current, ok := nm.manualGCPending[peerID]; ok && current == msg.SafeTimestamp {
		delete(nm.manualGCPending, peerID)
	}
	nm.mu.Unlock()

	resp.Success = true
	nm.sendManualGCResponse(peerID, resp)
}

func (nm *NodeManager) HandleManualGCAbort(peerID string, msg NetworkMessage) {
	resp := &NetworkMessage{
		Type:          MsgTypeGCAbortAck,
		RequestID:     msg.RequestID,
		SafeTimestamp: msg.SafeTimestamp,
		Success:       true,
	}
	if msg.RequestID == "" {
		resp.Success = false
		resp.Error = "missing request id"
		nm.sendManualGCResponse(peerID, resp)
		return
	}

	nm.mu.Lock()
	if msg.SafeTimestamp <= 0 {
		delete(nm.manualGCPending, peerID)
	} else if current, ok := nm.manualGCPending[peerID]; ok && current == msg.SafeTimestamp {
		delete(nm.manualGCPending, peerID)
	}
	nm.mu.Unlock()

	nm.sendManualGCResponse(peerID, resp)
}

func (nm *NodeManager) sendManualGCResponse(peerID string, resp *NetworkMessage) {
	if resp == nil {
		return
	}
	network := nm.getNetwork()
	if network == nil {
		log.Printf("[ManualGC] drop response: peer=%s, type=%s, err=%v", shortPeerID(peerID), resp.Type, ErrNoNetwork)
		return
	}
	if err := network.SendMessage(peerID, resp); err != nil {
		log.Printf("[ManualGC] send response failed: peer=%s, type=%s, err=%v",
			shortPeerID(peerID), resp.Type, err)
	}
}

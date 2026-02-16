package sync

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/shinyes/yep_crdt/pkg/db"
)

const defaultManualGCTimeout = 10 * time.Second

// ManualGCResult summarizes one negotiated manual GC run.
type ManualGCResult struct {
	TenantID       string
	SafeTimestamp  int64
	PreparedPeers  []string
	CommittedPeers []string // peers that confirmed commit phase
	ExecutedPeers  []string // peers that finished execute phase
	LocalResult    *db.GCResult
}

type manualGCRequester func(peerID string, msg *NetworkMessage, timeout time.Duration) (*NetworkMessage, error)

// ManualGC runs a tenant-scoped, three-phase manual GC:
// 1) prepare: collect peer safe timestamps and compute global min
// 2) commit: require all peers to confirm they can execute at the agreed safe timestamp
// 3) execute: after all confirms, trigger actual GC execution
func (m *MultiEngine) ManualGC(tenantID string, timeout time.Duration) (*ManualGCResult, error) {
	m.mu.RLock()
	rt, exists := m.tenants[tenantID]
	network := m.network
	m.mu.RUnlock()
	if !exists || rt == nil {
		return nil, fmt.Errorf("tenant not started: %s", tenantID)
	}

	peers := normalizeManualGCPeers(rt.nodeMgr.GetOnlineNodes(), rt.nodeMgr.GetLocalNodeID())
	timeout = normalizeManualGCTimeout(timeout)

	result := &ManualGCResult{
		TenantID:       tenantID,
		SafeTimestamp:  rt.nodeMgr.CalculateSafeTimestamp(),
		PreparedPeers:  make([]string, 0, len(peers)),
		CommittedPeers: make([]string, 0, len(peers)),
		ExecutedPeers:  make([]string, 0, len(peers)),
	}

	var request manualGCRequester
	if len(peers) > 0 {
		if network == nil {
			return nil, ErrNoNetwork
		}

		request = func(peerID string, msg *NetworkMessage, reqTimeout time.Duration) (*NetworkMessage, error) {
			if msg == nil {
				return nil, fmt.Errorf("message is nil")
			}
			cloned := *msg
			cloned.TenantID = tenantID
			return network.SendWithResponse(peerID, &cloned, reqTimeout)
		}

		safeTimestamp, preparedPeers, err := runManualGCPreparePhase(result.SafeTimestamp, peers, timeout, request)
		if err != nil {
			return nil, err
		}
		result.SafeTimestamp = safeTimestamp
		result.PreparedPeers = preparedPeers

		latestLocalSafe := rt.nodeMgr.CalculateSafeTimestamp()
		if result.SafeTimestamp > latestLocalSafe {
			return nil, fmt.Errorf("manual gc aborted: safe timestamp %d exceeds local safe timestamp %d",
				result.SafeTimestamp, latestLocalSafe)
		}

		committedPeers, err := runManualGCCommitPhase(peers, result.SafeTimestamp, timeout, request)
		if err != nil {
			return nil, err
		}
		result.CommittedPeers = committedPeers
	}

	localResult := rt.db.GC(result.SafeTimestamp)
	result.LocalResult = localResult
	if len(localResult.Errors) > 0 {
		if len(result.CommittedPeers) > 0 && request != nil {
			runManualGCAbortPhase(result.CommittedPeers, result.SafeTimestamp, timeout, request)
		}
		return result, fmt.Errorf("manual gc local run returned %d errors", len(localResult.Errors))
	}
	if err := rt.nodeMgr.SetLocalGCFloor(result.SafeTimestamp); err != nil {
		if len(result.CommittedPeers) > 0 && request != nil {
			runManualGCAbortPhase(result.CommittedPeers, result.SafeTimestamp, timeout, request)
		}
		return result, fmt.Errorf("manual gc update local gc floor failed: %w", err)
	}

	if len(peers) > 0 {
		executedPeers, err := runManualGCExecutePhase(peers, result.SafeTimestamp, timeout, request)
		result.ExecutedPeers = executedPeers
		if err != nil {
			runManualGCAbortPhase(peers, result.SafeTimestamp, timeout, request)
			return result, err
		}
	}

	log.Printf("[ManualGC:%s] done: safe_ts=%d, peers=%d, committed=%d, executed=%d, removed=%d",
		tenantID, result.SafeTimestamp, len(peers), len(result.CommittedPeers), len(result.ExecutedPeers), localResult.TombstonesRemoved)
	return result, nil
}

func runManualGCPreparePhase(localSafeTimestamp int64, peers []string, timeout time.Duration, request manualGCRequester) (int64, []string, error) {
	if request == nil {
		return 0, nil, fmt.Errorf("manual gc request function is nil")
	}

	safeTimestamp := localSafeTimestamp
	preparedPeers := make([]string, 0, len(peers))
	for _, peerID := range peers {
		resp, err := request(peerID, &NetworkMessage{Type: MsgTypeGCPrepare}, timeout)
		if err != nil {
			return 0, preparedPeers, fmt.Errorf("manual gc prepare failed: peer=%s, err=%w", shortPeerID(peerID), err)
		}
		if resp == nil {
			return 0, preparedPeers, fmt.Errorf("manual gc prepare failed: peer=%s, empty response", shortPeerID(peerID))
		}
		if resp.Type != MsgTypeGCPrepareAck {
			return 0, preparedPeers, fmt.Errorf("manual gc prepare failed: peer=%s, unexpected response type=%s",
				shortPeerID(peerID), resp.Type)
		}
		if !resp.Success {
			reason := resp.Error
			if reason == "" {
				reason = "peer rejected prepare"
			}
			return 0, preparedPeers, fmt.Errorf("manual gc prepare rejected: peer=%s, reason=%s",
				shortPeerID(peerID), reason)
		}

		if resp.SafeTimestamp < safeTimestamp {
			safeTimestamp = resp.SafeTimestamp
		}
		preparedPeers = append(preparedPeers, peerID)
	}

	return safeTimestamp, preparedPeers, nil
}

func runManualGCCommitPhase(peers []string, safeTimestamp int64, timeout time.Duration, request manualGCRequester) ([]string, error) {
	if request == nil {
		return nil, fmt.Errorf("manual gc request function is nil")
	}

	committedPeers := make([]string, 0, len(peers))
	for _, peerID := range peers {
		resp, err := request(peerID, &NetworkMessage{
			Type:          MsgTypeGCCommit,
			SafeTimestamp: safeTimestamp,
		}, timeout)
		if err != nil {
			return committedPeers, fmt.Errorf("manual gc commit failed: peer=%s, err=%w", shortPeerID(peerID), err)
		}
		if resp == nil {
			return committedPeers, fmt.Errorf("manual gc commit failed: peer=%s, empty response", shortPeerID(peerID))
		}
		if resp.Type != MsgTypeGCCommitAck {
			return committedPeers, fmt.Errorf("manual gc commit failed: peer=%s, unexpected response type=%s",
				shortPeerID(peerID), resp.Type)
		}
		if !resp.Success {
			reason := resp.Error
			if reason == "" {
				reason = "peer rejected commit"
			}
			return committedPeers, fmt.Errorf("manual gc commit rejected: peer=%s, reason=%s",
				shortPeerID(peerID), reason)
		}
		committedPeers = append(committedPeers, peerID)
	}

	return committedPeers, nil
}

func runManualGCExecutePhase(peers []string, safeTimestamp int64, timeout time.Duration, request manualGCRequester) ([]string, error) {
	if request == nil {
		return nil, fmt.Errorf("manual gc request function is nil")
	}

	executedPeers := make([]string, 0, len(peers))
	for _, peerID := range peers {
		resp, err := request(peerID, &NetworkMessage{
			Type:          MsgTypeGCExecute,
			SafeTimestamp: safeTimestamp,
		}, timeout)
		if err != nil {
			return executedPeers, fmt.Errorf("manual gc execute failed: peer=%s, err=%w", shortPeerID(peerID), err)
		}
		if resp == nil {
			return executedPeers, fmt.Errorf("manual gc execute failed: peer=%s, empty response", shortPeerID(peerID))
		}
		if resp.Type != MsgTypeGCExecuteAck {
			return executedPeers, fmt.Errorf("manual gc execute failed: peer=%s, unexpected response type=%s",
				shortPeerID(peerID), resp.Type)
		}
		if !resp.Success {
			reason := resp.Error
			if reason == "" {
				reason = "peer rejected execute"
			}
			return executedPeers, fmt.Errorf("manual gc execute rejected: peer=%s, reason=%s",
				shortPeerID(peerID), reason)
		}
		executedPeers = append(executedPeers, peerID)
	}

	return executedPeers, nil
}

func runManualGCAbortPhase(peers []string, safeTimestamp int64, timeout time.Duration, request manualGCRequester) {
	if request == nil || len(peers) == 0 {
		return
	}

	for _, peerID := range peers {
		resp, err := request(peerID, &NetworkMessage{
			Type:          MsgTypeGCAbort,
			SafeTimestamp: safeTimestamp,
		}, timeout)
		if err != nil {
			log.Printf("[ManualGC] abort notify failed: peer=%s, err=%v", shortPeerID(peerID), err)
			continue
		}
		if resp == nil {
			log.Printf("[ManualGC] abort notify got empty response: peer=%s", shortPeerID(peerID))
			continue
		}
		if resp.Type != MsgTypeGCAbortAck {
			log.Printf("[ManualGC] abort notify got unexpected response: peer=%s, type=%s", shortPeerID(peerID), resp.Type)
			continue
		}
		if !resp.Success {
			log.Printf("[ManualGC] abort notify rejected: peer=%s, reason=%s", shortPeerID(peerID), resp.Error)
		}
	}
}

func normalizeManualGCPeers(peers []string, localNodeID string) []string {
	if len(peers) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(peers))
	out := make([]string, 0, len(peers))
	for _, peerID := range peers {
		if peerID == "" || peerID == localNodeID {
			continue
		}
		if _, exists := seen[peerID]; exists {
			continue
		}
		seen[peerID] = struct{}{}
		out = append(out, peerID)
	}
	sort.Strings(out)
	return out
}

func normalizeManualGCTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return defaultManualGCTimeout
	}
	return timeout
}

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
	if nm.network == nil {
		log.Printf("[ManualGC] drop response: peer=%s, type=%s, err=%v", shortPeerID(peerID), resp.Type, ErrNoNetwork)
		return
	}
	if err := nm.network.SendMessage(peerID, resp); err != nil {
		log.Printf("[ManualGC] send response failed: peer=%s, type=%s, err=%v",
			shortPeerID(peerID), resp.Type, err)
	}
}

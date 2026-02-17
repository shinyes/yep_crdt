package sync

import (
	"fmt"
	"log"
	"time"
)

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

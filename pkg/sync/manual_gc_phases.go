package sync

import (
	"fmt"
	"log"
	"time"
)

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

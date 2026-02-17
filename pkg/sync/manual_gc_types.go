package sync

import (
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

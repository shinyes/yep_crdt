package sync

// GetNodeInfo returns one node record.
func (nm *NodeManager) GetNodeInfo(nodeID string) (*NodeInfo, bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	nodeInfo, exists := nm.nodes[nodeID]
	return nodeInfo, exists
}

// IsNodeOnline returns whether a node is online.
func (nm *NodeManager) IsNodeOnline(nodeID string) bool {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if nodeInfo, exists := nm.nodes[nodeID]; exists {
		return nodeInfo.IsOnline
	}
	return false
}

// GetOnlineNodes returns all online node IDs.
func (nm *NodeManager) GetOnlineNodes() []string {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	onlineNodes := make([]string, 0)
	for nodeID, nodeInfo := range nm.nodes {
		if nodeInfo.IsOnline {
			onlineNodes = append(onlineNodes, nodeID)
		}
	}
	return onlineNodes
}

// CalculateSafeTimestamp computes GC safe time using all known node clocks.
// This is intentionally conservative: offline nodes are still considered so
// GC does not advance past their last known observation point.
func (nm *NodeManager) CalculateSafeTimestamp() int64 {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	localClock := nm.db.Clock().Now()
	minClock := localClock
	for _, nodeInfo := range nm.nodes {
		if nodeInfo.ID == nm.localNodeID {
			continue
		}
		if nodeInfo.LastKnownClock <= 0 {
			continue
		}
		if nodeInfo.LastKnownClock < minClock {
			minClock = nodeInfo.LastKnownClock
		}
	}

	return minClock - defaultSafeTimestampOffset.Milliseconds()
}

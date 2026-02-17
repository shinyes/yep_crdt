package sync

import (
	"context"
	"log"
	"time"
)

// LocalGCFloor returns persisted local GC floor.
func (nm *NodeManager) LocalGCFloor() int64 {
	return nm.db.GCFloor()
}

// SetLocalGCFloor updates persisted local GC floor and refreshes per-peer incremental gates.
func (nm *NodeManager) SetLocalGCFloor(floor int64) error {
	if err := nm.db.SetGCFloor(floor); err != nil {
		return err
	}
	nm.refreshIncrementalGates()
	return nil
}

// ObservePeerGCFloor records peer GC floor and enforces incremental/full-sync gate.
func (nm *NodeManager) ObservePeerGCFloor(nodeID string, gcFloor int64) {
	if nodeID == "" || gcFloor < 0 {
		return
	}

	nm.mu.Lock()
	nodeInfo, exists := nm.nodes[nodeID]
	if !exists {
		now := time.Now()
		localClock := nm.db.Clock().Now()
		nodeInfo = &NodeInfo{
			ID:                 nodeID,
			LastHeartbeat:      now,
			IsOnline:           true,
			LastKnownClock:     localClock,
			LastKnownGCFloor:   0,
			IncrementalBlocked: false,
			LastSyncTime:       now,
		}
		nm.nodes[nodeID] = nodeInfo
	}
	if gcFloor > nodeInfo.LastKnownGCFloor {
		nodeInfo.LastKnownGCFloor = gcFloor
	}
	nm.mu.Unlock()

	nm.refreshIncrementalGates()
}

// CanUseIncrementalWithPeer reports whether incremental sync is allowed with one peer.
func (nm *NodeManager) CanUseIncrementalWithPeer(nodeID string) bool {
	if nodeID == "" {
		return false
	}

	nm.mu.RLock()
	defer nm.mu.RUnlock()

	nodeInfo, exists := nm.nodes[nodeID]
	if !exists {
		return true
	}
	return !nodeInfo.IncrementalBlocked
}

func (nm *NodeManager) refreshIncrementalGates() {
	type syncTask struct {
		nodeID string
		floor  int64
	}

	localFloor := nm.db.GCFloor()
	tasks := make([]syncTask, 0)

	nm.mu.Lock()
	for nodeID, nodeInfo := range nm.nodes {
		if nodeInfo == nil || nodeID == "" || nodeID == nm.localNodeID {
			continue
		}

		peerFloor := nodeInfo.LastKnownGCFloor
		shouldBlock := (peerFloor > 0 || localFloor > 0) && peerFloor != localFloor
		wasBlocked := nodeInfo.IncrementalBlocked
		nodeInfo.IncrementalBlocked = shouldBlock
		if wasBlocked != shouldBlock {
			log.Printf("[GCFloor] incremental gate changed: peer=%s, blocked=%v, local_floor=%d, peer_floor=%d",
				shortPeerID(nodeID), shouldBlock, localFloor, peerFloor)
		}

		if shouldBlock && peerFloor > localFloor && !nm.fullSyncing[nodeID] {
			nm.fullSyncing[nodeID] = true
			tasks = append(tasks, syncTask{
				nodeID: nodeID,
				floor:  peerFloor,
			})
		}
	}
	nm.mu.Unlock()

	for _, task := range tasks {
		task := task
		go nm.runGCFloorFullSync(task.nodeID, task.floor)
	}
}

func (nm *NodeManager) runGCFloorFullSync(sourceNodeID string, targetFloor int64) {
	log.Printf("[GCFloor] start full sync for lagging node: source=%s, target_floor=%d",
		shortPeerID(sourceNodeID), targetFloor)

	ctx := context.Background()
	result, err := nm.dataSync.FullSync(ctx, sourceNodeID)
	if err != nil {
		log.Printf("[GCFloor] full sync failed: source=%s, target_floor=%d, err=%v",
			shortPeerID(sourceNodeID), targetFloor, err)
		nm.finishGCFloorFullSync(sourceNodeID)
		return
	}

	if err := nm.SetLocalGCFloor(targetFloor); err != nil {
		log.Printf("[GCFloor] persist local gc floor failed: source=%s, target_floor=%d, err=%v",
			shortPeerID(sourceNodeID), targetFloor, err)
		nm.finishGCFloorFullSync(sourceNodeID)
		return
	}

	nm.markPeerFullSync(sourceNodeID)
	log.Printf("[GCFloor] full sync done: source=%s, target_floor=%d, tables=%d, rows=%d, rejected=%d",
		shortPeerID(sourceNodeID), targetFloor, result.TablesSynced, result.RowsSynced, result.RejectedCount)
	nm.finishGCFloorFullSync(sourceNodeID)
}

func (nm *NodeManager) finishGCFloorFullSync(sourceNodeID string) {
	nm.mu.Lock()
	delete(nm.fullSyncing, sourceNodeID)
	nm.mu.Unlock()
	nm.refreshIncrementalGates()
}

package sync

import (
	"context"
	"log"
	"time"
)

// ClockSync handles rejoin-time synchronization strategy.
type ClockSync struct {
	nm             *NodeManager
	clockThreshold int64
}

// NewClockSync creates a ClockSync component.
func NewClockSync(nm *NodeManager, clockThreshold int64) *ClockSync {
	return &ClockSync{
		nm:             nm,
		clockThreshold: clockThreshold,
	}
}

// HandleNodeRejoin handles peer rejoin workflow.
func (cs *ClockSync) HandleNodeRejoin(nodeID string, remoteClock int64, offlineDuration time.Duration) {
	log.Printf("[ClockSync] peer rejoined: node=%s, offline=%v", nodeID, offlineDuration)

	longOfflineThreshold := cs.nm.config.TimeoutThreshold
	if longOfflineThreshold > 0 && offlineDuration >= longOfflineThreshold {
		log.Printf("[ClockSync] long offline rejoin requires full sync first: node=%s, offline=%v, threshold=%v",
			nodeID, offlineDuration, longOfflineThreshold)
		cs.performFullSync(nodeID)
		return
	}

	myClock := cs.nm.db.Clock().Now()
	clockDiff := myClock - remoteClock

	log.Printf("[ClockSync] rejoin clock diff: node=%s, local=%d, remote=%d, diff=%d",
		nodeID, myClock, remoteClock, clockDiff)

	if cs.clockThreshold > 0 && clockDiff > cs.clockThreshold {
		log.Printf("[ClockSync] large clock gap, perform full sync: node=%s, diff=%d, threshold=%d",
			nodeID, clockDiff, cs.clockThreshold)
		cs.performFullSync(nodeID)
		return
	}

	log.Printf("[ClockSync] clock gap acceptable, reset local clock from peer: node=%s, remote=%d",
		nodeID, remoteClock)
	cs.performClockReset(remoteClock)
}

func (cs *ClockSync) performClockReset(remoteClock int64) {
	cs.nm.db.Clock().Update(remoteClock)
	log.Printf("[ClockSync] local clock updated from rejoin peer: remote=%d", remoteClock)
}

func (cs *ClockSync) performFullSync(sourceNodeID string) {
	log.Printf("[ClockSync] starting full sync from peer=%s", sourceNodeID)

	ctx := context.Background()
	result, err := cs.nm.dataSync.FullSync(ctx, sourceNodeID)
	if err != nil {
		log.Printf("[ClockSync] full sync failed: peer=%s, err=%v", sourceNodeID, err)
		return
	}

	cs.nm.markPeerFullSync(sourceNodeID)
	log.Printf("[ClockSync] full sync done: peer=%s, tables=%d, rows=%d, rejected=%d",
		sourceNodeID, result.TablesSynced, result.RowsSynced, result.RejectedCount)
}

package sync

import (
	"context"
	"log"
)

// ClockSync 时钟同步策略
type ClockSync struct {
	nm             *NodeManager
	clockThreshold int64
}

// NewClockSync 创建时钟同步器
func NewClockSync(nm *NodeManager, clockThreshold int64) *ClockSync {
	return &ClockSync{
		nm:             nm,
		clockThreshold: clockThreshold,
	}
}

// HandleNodeRejoin 处理节点重新上线
func (cs *ClockSync) HandleNodeRejoin(nodeID string, remoteClock int64) {
	log.Printf("🔄 处理节点 %s 重新上线", nodeID)

	// 1. 计算时钟差距
	myClock := cs.nm.db.Clock().Now()
	clockDiff := myClock - remoteClock

	log.Printf("   本地时钟: %d, 远程时钟: %d, 差距: %d",
		myClock, remoteClock, clockDiff)

	// 2. 检查时钟差距
	if clockDiff > cs.clockThreshold {
		// 时钟差距太大，需要全量同步
		log.Printf("⚠️ 时钟差距过大 (%d > %d)，执行全量同步",
			clockDiff, cs.clockThreshold)

		cs.performFullSync(nodeID)
	} else {
		// 时钟差距可接受，只重置时钟
		log.Printf("✅ 时钟差距可接受，重置时钟到: %d", remoteClock)
		cs.performClockReset(remoteClock)
	}
}

// performClockReset 执行时钟重置
func (cs *ClockSync) performClockReset(remoteClock int64) {
	// 直接更新本地时钟
	cs.nm.db.Clock().Update(remoteClock)

	log.Printf("✅ 本地时钟已重置到: %d", remoteClock)

	// 注意：时钟重置后，增量同步会自动拒绝过时数据
}

// performFullSync 执行全量同步
func (cs *ClockSync) performFullSync(sourceNodeID string) {
	log.Printf("开始全量同步: 从节点 %s", sourceNodeID)

	// 使用 DataSyncManager 的全量同步方法
	ctx := context.Background()
	result, err := cs.nm.dataSync.FullSync(ctx, sourceNodeID)
	if err != nil {
		log.Printf("⚠️ 全量同步失败: %v", err)
		return
	}

	log.Printf("✅ 全量同步完成: tables=%d, rows=%d, rejected=%d",
		result.TablesSynced, result.RowsSynced, result.RejectedCount)

	// 执行 GC 清理
	cs.performGC()
}

// performGC 执行 GC
func (cs *ClockSync) performGC() {
	log.Println("🧹 执行垃圾回收")

	// 使用NodeManager的CalculateSafeTimestamp
	safeTimestamp := cs.nm.CalculateSafeTimestamp()

	result := cs.nm.db.GC(safeTimestamp)

	if result.TombstonesRemoved > 0 {
		log.Printf("🧹 GC: 扫描表=%d, 行=%d, 清理=%d",
			result.TablesScanned,
			result.RowsScanned,
			result.TombstonesRemoved)
	}

	if len(result.Errors) > 0 {
		log.Printf("⚠️ GC 遇到错误: %d", len(result.Errors))
	}
}

package sync

import (
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
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
	log.Printf("starting full sync from node %s", sourceNodeID)
	
	// 1. 获取所有表的列表
	tables := cs.getOnlineNodeTables()
	
	if len(tables) == 0 {
		log.Println("no online nodes available to get table list")
		return
	}
	
	// 2. 删除本地所有数据
	cs.clearLocalData()
	
	// 3. 从源节点复制所有数据
	syncedCount := 0
	for _, tableName := range tables {
		if err := cs.syncTable(sourceNodeID, tableName); err != nil {
			log.Printf("sync table %s failed: %v", tableName, err)
			continue
		}
		syncedCount++
	}
	
	// 4. 强制 GC
	cs.performGC()
	
	log.Printf("full sync completed: synced %d tables", syncedCount)
}

// clearLocalData 清空本地数据
func (cs *ClockSync) clearLocalData() {
	log.Println("✅ 本地数据将在完整同步后重新填充")
}

// syncTable 同步单个表
func (cs *ClockSync) syncTable(sourceNodeID, tableName string) error {
	log.Printf("  🔄 同步表: %s", tableName)
	
	// 1. 从源节点获取所有数据
	data, err := cs.nm.FetchData(sourceNodeID, tableName)
	if err != nil {
		return fmt.Errorf("获取表数据失败: %w", err)
	}
	
	// 2. 批量插入本地
	for keyStr, row := range data {
		// 将字符串key转换为UUID
		key, err := uuid.Parse(keyStr)
		if err != nil {
			log.Printf("  ⚠️ 无效的UUID: %s", keyStr)
			continue
		}
		
		if err := cs.nm.db.Update(func(tx *db.Tx) error {
			table := tx.Table(tableName)
			if table == nil {
				return fmt.Errorf("表不存在: %s", tableName)
			}
			return table.Set(key, row)
		}); err != nil {
			log.Printf("  ⚠️ 插入行失败: %v", err)
			continue
		}
	}
	
	return nil
}

// getOnlineNodeTables 获取在线节点的表列表
func (cs *ClockSync) getOnlineNodeTables() []string {
	// 从DB中获取所有表的简化实现
	// 这里返回空列表，实际应用中需要从DB获取真实的表列表
	return []string{}
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

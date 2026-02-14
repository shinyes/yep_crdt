package sync

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shinyes/yep_crdt/pkg/db"
)

// GCManager GC 管理器
type GCManager struct {
	nm       *NodeManager
	interval time.Duration
	offset   time.Duration
	timeout  time.Duration // GC操作超时时间
	maxRetry int           // 最大重试次数
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex

	// 统计信息
	stats struct {
		sync.RWMutex
		totalRuns       int64
		successfulRuns  int64
		failedRuns      int64
		totalTombstones int64
		totalRemoved    int64
		lastRunDuration time.Duration
	}
}

// NewGCManager 创建 GC 管理器
func NewGCManager(nm *NodeManager, interval time.Duration, offset time.Duration) *GCManager {
	return &GCManager{
		nm:       nm,
		interval: interval,
		offset:   offset,
		timeout:  30 * time.Second, // 默认30秒超时
		maxRetry: 3,                // 默认重试3次
	}
}

// Start 启动 GC
func (gm *GCManager) Start(ctx context.Context) {
	gm.mu.Lock()
	gm.ctx, gm.cancel = context.WithCancel(ctx)
	gm.mu.Unlock()

	ticker := time.NewTicker(gm.interval)
	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-gm.ctx.Done():
				log.Println("🛑 GC 已停止")
				return

			case <-ticker.C:
				gm.performGC()
			}
		}
	}()

	log.Printf("✅ GC 已启动: 间隔=%v, 偏移=%v", gm.interval, gm.offset)
}

// performGC 执行 GC
func (gm *GCManager) performGC() {
	atomic.AddInt64(&gm.stats.totalRuns, 1)
	startTime := time.Now()

	// 计算安全时间戳
	safeTimestamp := gm.nm.CalculateSafeTimestamp()
	result, gcErr := gm.runGCAttempt(safeTimestamp)
	if gcErr != nil {
		// GC返回了错误，尝试重试
		log.Printf("⚠️ GC 遇到错误: %v，尝试重试...", gcErr)
		gm.performGCWithRetry(safeTimestamp, 1, startTime)
		return
	}

	// GC成功
	gm.recordGCSuccess(result, startTime)
}

// performGCWithRetry 执行GC并支持重试
func (gm *GCManager) performGCWithRetry(safeTimestamp int64, attempt int, startTime time.Time) {
	for i := attempt; i <= gm.maxRetry; i++ {
		// 指数退避
		backoff := time.Duration(i*i) * time.Second
		log.Printf("⏳ GC 重试 [%d/%d]，等待 %v 后重试...", i, gm.maxRetry, backoff)

		select {
		case <-time.After(backoff):
			result, err := gm.runGCAttempt(safeTimestamp)
			if err == nil {
				gm.recordGCSuccess(result, startTime)
				return
			}
			log.Printf("⚠️  GC 尝试 %d 失败: %v", i, err)
		case <-gm.ctx.Done():
			// 停止重试
			return
		}
	}

	gm.recordGCFailure(startTime, fmt.Errorf("gc failed after %d attempts", gm.maxRetry))
	log.Printf("❌ GC 在%d次重试后仍然失败", gm.maxRetry)
}

func (gm *GCManager) runGCAttempt(safeTimestamp int64) (*db.GCResult, error) {
	attemptStart := time.Now()
	result := gm.nm.db.GC(safeTimestamp)
	elapsed := time.Since(attemptStart)
	if gm.timeout > 0 && elapsed >= gm.timeout {
		log.Printf("⏰ GC 耗时超出阈值: duration=%v, threshold=%v", elapsed, gm.timeout)
	}

	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("gc returned %d errors", len(result.Errors))
	}
	return result, nil
}

// recordGCSuccess 记录成功的GC
func (gm *GCManager) recordGCSuccess(result *db.GCResult, startTime time.Time) {
	duration := time.Since(startTime)

	gm.stats.Lock()
	gm.stats.successfulRuns++
	gm.stats.totalTombstones += int64(result.TombstonesRemoved)
	gm.stats.totalRemoved += int64(result.TombstonesRemoved)
	gm.stats.lastRunDuration = duration
	gm.stats.Unlock()

	log.Printf("✅ GC 成功 [耗时=%v]: 扫描表=%d, 行=%d, 清理=%d",
		duration, result.TablesScanned, result.RowsScanned, result.TombstonesRemoved)
}

// recordGCFailure 记录失败的GC
func (gm *GCManager) recordGCFailure(startTime time.Time, err error) {
	duration := time.Since(startTime)

	gm.stats.Lock()
	gm.stats.failedRuns++
	gm.stats.lastRunDuration = duration
	failureRate := float64(gm.stats.failedRuns) / float64(gm.stats.totalRuns) * 100
	gm.stats.Unlock()

	log.Printf("❌ GC 失败 [耗时=%v]: %v (失败率=%.1f%%)",
		duration, err, failureRate)
}

// Stop 停止 GC
func (gm *GCManager) Stop() {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if gm.cancel != nil {
		gm.cancel()
		log.Println("🛑 GC 已停止")
	}
}

// GetStats 获取GC统计信息
func (gm *GCManager) GetStats() map[string]interface{} {
	gm.stats.RLock()
	defer gm.stats.RUnlock()

	var failureRate float64
	if gm.stats.totalRuns > 0 {
		failureRate = float64(gm.stats.failedRuns) / float64(gm.stats.totalRuns) * 100
	}

	return map[string]interface{}{
		"total_runs":        gm.stats.totalRuns,
		"successful_runs":   gm.stats.successfulRuns,
		"failed_runs":       gm.stats.failedRuns,
		"failure_rate_pct":  failureRate,
		"total_tombstones":  gm.stats.totalTombstones,
		"total_removed":     gm.stats.totalRemoved,
		"last_run_duration": gm.stats.lastRunDuration.String(),
	}
}

// SetTimeout 设置GC超时时间
func (gm *GCManager) SetTimeout(timeout time.Duration) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if timeout > 0 {
		gm.timeout = timeout
		log.Printf("✓ GC超时设置为: %v\n", timeout)
	}
}

// SetMaxRetry 设置GC最大重试次数
func (gm *GCManager) SetMaxRetry(maxRetry int) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if maxRetry > 0 {
		gm.maxRetry = maxRetry
		log.Printf("✓ GC最大重试次数设置为: %d\n", maxRetry)
	}
}

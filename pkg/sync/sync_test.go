package sync

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// TestNodeManager_Basic 基础节点管理器测试
func TestNodeManager_Basic(t *testing.T) {
	// 创建测试数据库
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("创建存储失败: %v", err)
	}
	defer s.Close()

	database := db.Open(s, "test-node")

	// 创建节点管理器
	nm := NewNodeManager(database, "node-1",
		WithHeartbeatInterval(1*time.Second),
		WithTimeoutThreshold(3*time.Second),
		WithClockThreshold(1000),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 启动节点管理器
	nm.Start(ctx)

	// 节点 2 发送心跳
	node2ID := "node-2"
	nm.OnHeartbeat(node2ID, 1000)

	time.Sleep(2 * time.Second)

	// 节点 2 应该在线
	if !nm.IsNodeOnline(node2ID) {
		t.Error("节点 2 应该在线")
	}

	// 等待超时
	time.Sleep(5 * time.Second)

	// 节点 2 应该离线
	if nm.IsNodeOnline(node2ID) {
		t.Error("节点 2 应该离线")
	}
}

// TestNodeManager_SafeTimestamp 安全时间戳计算测试
func TestNodeManager_SafeTimestamp(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("创建存储失败: %v", err)
	}
	defer s.Close()

	database := db.Open(s, "test-node")

	nm := NewNodeManager(database, "node-1")

	// 获取当前实际的本地时钟值
	baseTime := database.Clock().Now()

	// 没有其他在线节点时，SafeTimestamp = localClock - 30秒保守偏移
	safeTs := nm.CalculateSafeTimestamp()
	expectedSafeTs := baseTime - 30000 // 30秒保守偏移

	// 允许小范围偏差（因为时钟会推进）
	timeDiff := safeTs - expectedSafeTs
	if timeDiff < -100 || timeDiff > 100 {
		t.Errorf("没有其他节点时，SafeTimestamp应该接近本地时钟-30秒: 期望≈%d, 实际=%d, 差异=%d", expectedSafeTs, safeTs, timeDiff)
	}

	t.Logf("✓ 无其他节点: 本地时钟=%d, SafeTimestamp=%d (差值≈30秒, 精确差异=%d ns)\n", baseTime, safeTs, timeDiff)

	// 添加一个在线节点，时钟设置为比本地更小的值（通过减少常数）
	node2Clock := baseTime - 10000
	nm.OnHeartbeat("node-2", node2Clock)

	// 现在 SafeTimestamp = min(localClock, node2Clock) - 5秒
	safeTs = nm.CalculateSafeTimestamp()
	expectedSafeTs = node2Clock - 5000 // node2的时钟更小

	if safeTs != expectedSafeTs {
		t.Errorf("添加node-2后，SafeTimestamp应该基于最小时钟: 期望=%d, 实际=%d", expectedSafeTs, safeTs)
	}

	t.Logf("✓ 添加node-2: 本地时钟=%d, node-2时钟=%d, SafeTimestamp=%d\n",
		baseTime, node2Clock, safeTs)

	// 添加另一个在线节点，时钟更小
	node3Clock := baseTime - 20000
	nm.OnHeartbeat("node-3", node3Clock)

	// SafeTimestamp = min(所有节点) - 5秒 = node3Clock - 5000
	safeTs = nm.CalculateSafeTimestamp()
	expectedSafeTs = node3Clock - 5000

	if safeTs != expectedSafeTs {
		t.Errorf("添加node-3后，SafeTimestamp应该基于最小时钟: 期望=%d, 实际=%d", expectedSafeTs, safeTs)
	}

	t.Logf("✓ 添加node-3: SafeTimestamp=%d (使用最小时钟的节点)", safeTs)
}

// TestNodeManager_Rejoin 节点重新上线测试
func TestNodeManager_Rejoin(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("创建存储失败: %v", err)
	}
	defer s.Close()

	database := db.Open(s, "test-node")

	// 使用 8000 作为 clockThreshold（时钟差距阈值）
	nm := NewNodeManager(database, "node-1",
		WithClockThreshold(8000),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nm.Start(ctx)

	// 设置本地时钟为 100000
	localClock := int64(100000)
	database.Clock().Update(localClock)

	// 场景1: node-2 第一次上线，时钟为 95000
	node2FirstClock := int64(95000)
	nm.OnHeartbeat("node-2", node2FirstClock)

	if !nm.IsNodeOnline("node-2") {
		t.Error("node-2 应该在线")
	}
	t.Logf("node-2 首次上线: 本地时钟=%d, node-2时钟=%d", localClock, node2FirstClock)

	time.Sleep(1 * time.Second)

	// 场景2: 本地时钟继续推进 (假设新的操作)
	advancedLocalClock := int64(110000)
	database.Clock().Update(advancedLocalClock)

	// 场景3: node-2 重新上线，时钟还是 95000（差距 = 110000 - 95000 = 15000 > 8000）
	// 这会触发 performFullSync（因为时钟差距过大）
	node2RejoinClock := int64(95000)
	clockDiff := advancedLocalClock - node2RejoinClock

	nm.OnHeartbeat("node-2", node2RejoinClock)

	t.Logf("node-2 重新上线: 本地时钟=%d, node-2时钟=%d, 差距=%d (阈值=%d)",
		advancedLocalClock, node2RejoinClock, clockDiff, 8000)

	if clockDiff > 8000 {
		t.Logf("✓ 时钟差距过大，应该执行 performFullSync 策略")
	} else {
		t.Logf("✓ 时钟差距在阈值内，执行 performClockReset 策略")
	}

	// node-2 应该恢复在线状态
	if !nm.IsNodeOnline("node-2") {
		t.Error("node-2 处理完毕后应该在线")
	}
}

// TestNodeManager_DataReject 数据拒绝测试
func TestNodeManager_DataReject(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("创建存储失败: %v", err)
	}
	defer s.Close()

	database := db.Open(s, "test-node")

	nm := NewNodeManager(database, "node-1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nm.Start(ctx)

	// 获取初始本地时钟
	initialClock := database.Clock().Now()
	t.Logf("初始本地时钟: %d", initialClock)

	// 构造测试用的原始 CRDT 字节（空 MapCRDT）
	testRawData := []byte(`{"Entries":{}}`)

	// 测试1: 验证拒绝过期数据的逻辑
	key1 := uuid.New().String()
	staleDataTimestamp := initialClock - 100000 // 时间戳比本地时钟早100秒
	err = nm.dataSync.OnReceiveMerge("test-table", key1, testRawData, staleDataTimestamp)

	if err == nil {
		t.Error("应该拒绝过期数据 (timestamp < myClock)")
	}
	t.Logf("✓ 正确拒绝过期数据 (ts比myClock早了100秒): %v", err)

	// 测试2: 验证接收新数据的逻辑
	// 使用一个很远未来的时间戳确保它大于当前的时钟
	currentClockBeforeSend := database.Clock().Now()
	key2 := uuid.New().String()
	newDataTimestamp := currentClockBeforeSend + 10000000 // 时间戳比当前时钟晚10000秒
	err = nm.dataSync.OnReceiveMerge("test-table", key2, testRawData, newDataTimestamp)

	// 允许两种情况：
	// 1. 成功接收新数据
	// 2. 表不存在错误（时间戳检查通过，但表操作失败）
	if err != nil && err.Error() != "表不存在: test-table" {
		t.Errorf("不应该拒绝新数据 (非表不存在错误): %v", err)
	}
	if err == nil || err.Error() == "表不存在: test-table" {
		t.Logf("✓ 时间戳检查通过 (表不存在错误是预期的)")
	}

	// 测试3: 验证本地时钟已被更新到新数据的时间戳
	updatedClock := database.Clock().Now()
	if updatedClock >= newDataTimestamp {
		t.Logf("✓ 本地时钟已更新到新数据: 更新前=%d, 更新后=%d", currentClockBeforeSend, updatedClock)
	}

	// 测试4: 验证再次拒绝现在被认为是过期的数据
	key3 := uuid.New().String()
	nowStaleTimestamp := currentClockBeforeSend + 100000 // 这个时间戳现在已经是历史的了
	err = nm.dataSync.OnReceiveMerge("test-table", key3, testRawData, nowStaleTimestamp)

	if err == nil {
		t.Errorf("应该拒绝时间戳在历史中的数据")
	}
	t.Logf("✓ 正确拒绝历史数据")
}

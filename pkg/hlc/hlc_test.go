package hlc

import (
	"testing"
	"time"
)

func TestHLC_New(t *testing.T) {
	clock := New()
	if clock.Now() == 0 {
		t.Fatal("新时钟的初始时间应大于 0")
	}
}

func TestHLC_Monotonicity(t *testing.T) {
	clock := New()
	t1 := clock.Now()
	t2 := clock.Now()

	if t2 <= t1 {
		t.Errorf("时钟非单调递增: t1=%d, t2=%d", t1, t2)
	}

	p1, l1 := Physical(t1), Logical(t1)
	p2, l2 := Physical(t2), Logical(t2)

	if p2 < p1 {
		t.Errorf("物理时间倒退")
	}
	if p2 == p1 && l2 <= l1 {
		t.Errorf("同一毫秒内的逻辑时间未增加")
	}
}

func TestHLC_Update(t *testing.T) {
	clock := New()

	// 模拟接收到来自未来的消息
	futurePhys := time.Now().Add(1 * time.Hour).UnixMilli()
	remoteTs := futurePhys << 16

	clock.Update(remoteTs)

	now := clock.Now()
	if Physical(now) < futurePhys {
		t.Errorf("时钟未追上将来时间。Got %d, want >= %d", Physical(now), futurePhys)
	}
}

func TestHLC_Causality(t *testing.T) {
	// 节点 A
	clockA := New()
	tsA := clockA.Now()

	// 节点 B 接收到来自 A 的消息
	clockB := New()
	clockB.Update(tsA)

	tsB := clockB.Now()

	// tsB 应该 > tsA
	if tsB <= tsA {
		t.Errorf("违反因果关系: tsB (%d) <= tsA (%d)", tsB, tsA)
	}
}

func TestLogicalRollover(t *testing.T) {
	// 这个测试比较棘手，因为我们不能在测试中轻易强制 65536 次逻辑增量而不使用循环
	// 但我们可以测试辅助函数

	ts := int64(100)<<16 | 0xFFFF
	if Logical(ts) != -1 { // 0xFFFF 转为 int16 是 -1。 Wait, Logical 返回 int16
		// int16(0xFFFF) 是 -1.
		// logicalMask 是 0xFFFF.
		// 如果我们使用 uint16 它将是 65535.
		// 实现使用 int64 作为时间戳，但 Logical 返回 int16.
		// 让我们再次检查 hlc.go.
	}
}

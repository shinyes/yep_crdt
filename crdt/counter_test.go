package crdt_test

import (
	"testing"
	"time"

	"github.com/shinyes/yep_crdt/crdt"
)

// TestPNCounter 测试 PNCounter 的基本操作（增加和减少）。
func TestPNCounter(t *testing.T) {
	pn := crdt.NewPNCounter("node1")

	// 增加
	pn.Apply(crdt.PNCounterOp{OriginID: "node1", Amount: 10, Ts: time.Now().UnixNano()})

	// 减少
	pn.Apply(crdt.PNCounterOp{OriginID: "node1", Amount: -3, Ts: time.Now().UnixNano()})

	// 远程减少
	pn.Apply(crdt.PNCounterOp{OriginID: "node2", Amount: -2, Ts: time.Now().UnixNano()})

	if val := pn.Value().(int64); val != 5 { // 10 - 3 - 2 = 5
		t.Errorf("期望值为 5，实际为 %d", val)
	}
}

// TestPNCounterIncrease 测试 PNCounter 的纯增加操作。
func TestPNCounterIncrease(t *testing.T) {
	pn := crdt.NewPNCounter("node1")

	// 本地增加
	op1 := crdt.PNCounterOp{OriginID: "node1", Amount: 5, Ts: time.Now().UnixNano()}
	pn.Apply(op1)

	if val := pn.Value().(int64); val != 5 {
		t.Errorf("期望值为 5，实际为 %d", val)
	}

	// 远程增加
	op2 := crdt.PNCounterOp{OriginID: "node2", Amount: 10, Ts: time.Now().UnixNano()}
	pn.Apply(op2)

	if val := pn.Value().(int64); val != 15 {
		t.Errorf("期望值为 15，实际为 %d", val)
	}
}

// TestPNCounterMerge 测试 PNCounter 的状态合并。
func TestPNCounterMerge(t *testing.T) {
	// 创建两个独立的 PNCounter（模拟两个节点）
	pn1 := crdt.NewPNCounter("node1")
	pn2 := crdt.NewPNCounter("node2")

	// Node 1: +10, -3
	pn1.Apply(crdt.PNCounterOp{OriginID: "node1", Amount: 10, Ts: time.Now().UnixNano()})
	pn1.Apply(crdt.PNCounterOp{OriginID: "node1", Amount: -3, Ts: time.Now().UnixNano()})

	// Node 2: +5, -2
	pn2.Apply(crdt.PNCounterOp{OriginID: "node2", Amount: 5, Ts: time.Now().UnixNano()})
	pn2.Apply(crdt.PNCounterOp{OriginID: "node2", Amount: -2, Ts: time.Now().UnixNano()})

	// 合并前的值
	// pn1: 10 - 3 = 7
	// pn2: 5 - 2 = 3
	if val := pn1.Value().(int64); val != 7 {
		t.Errorf("pn1 合并前期望值为 7，实际为 %d", val)
	}
	if val := pn2.Value().(int64); val != 3 {
		t.Errorf("pn2 合并前期望值为 3，实际为 %d", val)
	}

	// 双向合并
	pn1.Merge(pn2.State())
	pn2.Merge(pn1.State())

	// 合并后两者应该一致: (10 + 5) - (3 + 2) = 10
	if val := pn1.Value().(int64); val != 10 {
		t.Errorf("pn1 合并后期望值为 10，实际为 %d", val)
	}
	if val := pn2.Value().(int64); val != 10 {
		t.Errorf("pn2 合并后期望值为 10，实际为 %d", val)
	}
}

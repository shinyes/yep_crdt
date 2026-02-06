package crdt_test

import (
	"testing"
	"time"

	"github.com/shinyes/yep_crdt/crdt"
)

func TestGCounter(t *testing.T) {
	g := crdt.NewGCounter("node1")

	// 本地增加
	op1 := crdt.GCounterOp{OriginID: "node1", Amount: 5, Ts: time.Now().UnixNano()}
	g.Apply(op1)

	if val := g.Value().(int64); val != 5 {
		t.Errorf("期望值为 5，实际为 %d", val)
	}

	// 远程增加
	op2 := crdt.GCounterOp{OriginID: "node2", Amount: 10, Ts: time.Now().UnixNano()}
	g.Apply(op2)

	if val := g.Value().(int64); val != 15 {
		t.Errorf("期望值为 15，实际为 %d", val)
	}

	// 幂等性（G-Counter 状态合并通常处理此问题，但这里应用的是 Op。
	// 我们的简单 Op 实现只是相加。实际的同步应该过滤掉重复项！）
	// 对于当前测试，假设 Ops 对应唯一的增量。
}

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

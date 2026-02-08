package crdt

import (
	"testing"
)

func TestPNCounter_Basic(t *testing.T) {
	c := NewPNCounter("node1")

	// 初始值 0
	if c.Value().(int64) != 0 {
		t.Fatalf("预期 0, 实际得到 %v", c.Value())
	}

	// 增加 10
	c.Apply(OpPNCounterInc{Val: 10})
	if c.Value().(int64) != 10 {
		t.Fatalf("预期 10, 实际得到 %v", c.Value())
	}

	// 减少 5
	c.Apply(OpPNCounterInc{Val: -5})
	if c.Value().(int64) != 5 {
		t.Fatalf("预期 5, 实际得到 %v", c.Value())
	}
}

func TestPNCounter_Merge(t *testing.T) {
	c1 := NewPNCounter("node1")
	c2 := NewPNCounter("node2")

	// c1: +10
	c1.Apply(OpPNCounterInc{Val: 10})

	// c2: +20
	c2.Apply(OpPNCounterInc{Val: 20})

	// 合并 c2 到 c1
	c1.Merge(c2)

	// 预期: 10 (来自 node1) + 20 (来自 node2) = 30
	if c1.Value().(int64) != 30 {
		t.Errorf("预期 30, 实际得到 %v", c1.Value())
	}

	// c2: -5
	c2.Apply(OpPNCounterInc{Val: -5}) // 这会给 node2 的 Dec 映射增加 5

	// 合并 c2 到 c1
	c1.Merge(c2)

	// 预期: 30 - 5 = 25
	if c1.Value().(int64) != 25 {
		t.Errorf("预期 25, 实际得到 %v", c1.Value())
	}
}

func TestPNCounter_Convergence(t *testing.T) {
	c1 := NewPNCounter("A")
	c2 := NewPNCounter("B")

	c1.Apply(OpPNCounterInc{Val: 100})
	c2.Apply(OpPNCounterInc{Val: 50})
	c2.Apply(OpPNCounterInc{Val: -10})

	// 双向合并
	c1.Merge(c2)
	c2.Merge(c1)

	if c1.Value().(int64) != c2.Value().(int64) {
		t.Errorf("计数器未收敛: %v vs %v", c1.Value(), c2.Value())
	}

	// 值应该是 100 + 50 - 10 = 140
	if c1.Value().(int64) != 140 {
		t.Errorf("预期 140, 实际得到 %v", c1.Value())
	}
}

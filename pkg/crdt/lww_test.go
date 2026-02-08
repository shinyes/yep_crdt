package crdt

import (
	"testing"
)

func TestLWWRegister_Basic(t *testing.T) {
	// 初始值 "A", 时间戳 100
	reg := NewLWWRegister([]byte("A"), 100)

	if string(reg.Value().([]byte)) != "A" {
		t.Fatalf("预期初始值为 A")
	}

	// 更新为 "B", 时间戳 200 (更大，应该胜出)
	op1 := OpLWWSet{Value: []byte("B"), Timestamp: 200}
	reg.Apply(op1)

	if string(reg.Value().([]byte)) != "B" {
		t.Fatalf("预期更新为 B")
	}

	// 尝试用旧时间戳更新 "C", 时间戳 150 (更小，应该被忽略)
	op2 := OpLWWSet{Value: []byte("C"), Timestamp: 150}
	reg.Apply(op2)

	if string(reg.Value().([]byte)) != "B" {
		t.Fatalf("预期值仍为 B, 但变成了 %s", reg.Value())
	}
}

func TestLWWRegister_Merge(t *testing.T) {
	r1 := NewLWWRegister([]byte("Node1"), 100)
	r2 := NewLWWRegister([]byte("Node2"), 200)

	// Merge r2 into r1
	r1.Merge(r2)

	// r2 时间戳更大，应该覆盖 r1
	if string(r1.Value().([]byte)) != "Node2" {
		t.Errorf("Merge 后预期 Node2, 实际得到 %s", r1.Value())
	}

	// 修改 r1 为更新的值
	r1.Apply(OpLWWSet{Value: []byte("Node1_Updated"), Timestamp: 300})

	// Merge r1 into r2
	r2.Merge(r1)

	if string(r2.Value().([]byte)) != "Node1_Updated" {
		t.Errorf("Merge 后预期 Node1_Updated, 实际得到 %s", r2.Value())
	}
}

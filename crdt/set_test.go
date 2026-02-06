package crdt_test

import (
	"testing"
	"time"

	"github.com/shinyes/yep_crdt/crdt"
)

func TestGSet(t *testing.T) {
	s := crdt.NewGSet()
	s.Apply(crdt.SetOp{OriginID: "node1", Val: "A", Add: true, Ts: time.Now().UnixNano()})
	s.Apply(crdt.SetOp{OriginID: "node2", Val: "B", Add: true, Ts: time.Now().UnixNano()})
	// 重复
	s.Apply(crdt.SetOp{OriginID: "node1", Val: "A", Add: true, Ts: time.Now().UnixNano()})

	val := s.Value().([]interface{})
	if len(val) != 2 {
		t.Errorf("期望 2 个元素，实际为 %d", len(val))
	}
}

func TestORSet(t *testing.T) {
	s := crdt.NewORSet()

	// 添加 "A"
	s.Apply(crdt.ORSetOp{OriginID: "node1", Val: "A", Add: true, Tag: "t1", Ts: time.Now().UnixNano()})

	// 并发添加 "A" (标签不同)
	s.Apply(crdt.ORSetOp{OriginID: "node2", Val: "A", Add: true, Tag: "t2", Ts: time.Now().UnixNano()})

	// 移除 "A" (如果使用标准 Observed-Remove，或指定了标签，则只应移除 t1)
	// 我们的 ORSetOp 实现接受 RemTags。
	s.Apply(crdt.ORSetOp{OriginID: "node1", Val: "A", Add: false, RemTags: []string{"t1"}, Ts: time.Now().UnixNano()})

	// "A" 应该仍然存在，因为 t2 还在
	val := s.Value().([]interface{})
	if len(val) != 1 || val[0] != "A" {
		t.Errorf("期望结果为 [A]，实际为 %v", val)
	}

	// 移除剩余的标签
	s.Apply(crdt.ORSetOp{OriginID: "node2", Val: "A", Add: false, RemTags: []string{"t2"}, Ts: time.Now().UnixNano()})

	val = s.Value().([]interface{})
	if len(val) != 0 {
		t.Errorf("期望集合为空，实际为 %v", val)
	}
}

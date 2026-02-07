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
	ts := time.Now().UnixNano()

	// 添加 "A" (使用新的嵌套结构)
	s.Apply(crdt.ORSetOp{
		OriginID: "node1",
		TypeCode: 0,
		ElemID:   "e1",
		InitType: crdt.TypeRegister,
		InitVal:  "A",
		Tag:      "t1",
		Ts:       ts,
	})

	// 并发添加 "A" (创建新元素，标签不同)
	s.Apply(crdt.ORSetOp{
		OriginID: "node2",
		TypeCode: 0,
		ElemID:   "e2",
		InitType: crdt.TypeRegister,
		InitVal:  "A",
		Tag:      "t2",
		Ts:       ts + 1,
	})

	// 移除 e1 (只移除 t1 标签)
	s.Apply(crdt.ORSetOp{
		OriginID: "node1",
		TypeCode: 1,
		RemoveID: "e1",
		RemTags:  []string{"t1"},
		Ts:       ts + 2,
	})

	// 应该只剩下 e2
	val := s.Value().([]interface{})
	if len(val) != 1 || val[0] != "A" {
		t.Errorf("期望结果为 [A]，实际为 %v", val)
	}

	// 移除剩余的元素
	s.Apply(crdt.ORSetOp{
		OriginID: "node2",
		TypeCode: 1,
		RemoveID: "e2",
		RemTags:  []string{"t2"},
		Ts:       ts + 3,
	})

	val = s.Value().([]interface{})
	if len(val) != 0 {
		t.Errorf("期望集合为空，实际为 %v", val)
	}
}

func TestORSetNestedCRDT(t *testing.T) {
	s := crdt.NewORSet()
	ts := time.Now().UnixNano()

	// 添加一个 Map 类型的元素
	err := s.Apply(crdt.ORSetOp{
		OriginID: "n1",
		TypeCode: 0,
		ElemID:   "item1",
		InitType: crdt.TypeMap,
		Tag:      "tag1",
		Ts:       ts,
	})
	if err != nil {
		t.Fatalf("添加 Map 元素失败: %v", err)
	}

	// 获取嵌套元素
	child := s.GetElement("item1")
	if child == nil {
		t.Fatal("无法获取嵌套元素")
	}
	if child.Type() != crdt.TypeMap {
		t.Errorf("期望类型为 Map，实际为 %v", child.Type())
	}

	// 向嵌套 Map 添加字段
	mapCRDT := child.(*crdt.MapCRDT)
	mapCRDT.Apply(crdt.MapOp{
		OriginID: "n1",
		Key:      "name",
		IsInit:   true,
		InitType: crdt.TypeRegister,
		Ts:       ts + 1,
	})
	mapCRDT.Apply(crdt.MapOp{
		OriginID: "n1",
		Key:      "name",
		ChildOp:  crdt.LWWOp{OriginID: "n1", Value: "Alice", Ts: ts + 2},
		Ts:       ts + 2,
	})

	// 验证值
	val := s.Value().([]interface{})
	if len(val) != 1 {
		t.Fatalf("期望 1 个元素，实际为 %d", len(val))
	}

	mapVal := val[0].(map[string]interface{})
	if mapVal["name"] != "Alice" {
		t.Errorf("期望 name=Alice，实际为 %v", mapVal["name"])
	}
}

func TestORSetChildOpForward(t *testing.T) {
	s := crdt.NewORSet()
	ts := time.Now().UnixNano()

	// 添加一个 Map 元素
	s.Apply(crdt.ORSetOp{
		OriginID: "n1",
		TypeCode: 0,
		ElemID:   "item1",
		InitType: crdt.TypeMap,
		Tag:      "tag1",
		Ts:       ts,
	})

	// 初始化 Map 内的 Counter
	child := s.GetElement("item1")
	mapCRDT := child.(*crdt.MapCRDT)
	mapCRDT.Apply(crdt.MapOp{
		OriginID: "n1",
		Key:      "count",
		IsInit:   true,
		InitType: crdt.TypeCounter,
		Ts:       ts + 1,
	})

	// 使用子操作转发
	childOp := crdt.MapOp{
		OriginID: "n1",
		Key:      "count",
		ChildOp:  crdt.PNCounterOp{OriginID: "n1", Amount: 10, Ts: ts + 2},
		Ts:       ts + 2,
	}

	err := s.Apply(crdt.ORSetOp{
		OriginID: "n1",
		TypeCode: 2, // 子操作转发
		TargetID: "item1",
		ChildOp:  childOp,
		Ts:       ts + 2,
	})
	if err != nil {
		t.Fatalf("子操作转发失败: %v", err)
	}

	// 验证
	val := s.Value().([]interface{})
	mapVal := val[0].(map[string]interface{})
	if mapVal["count"] != int64(10) {
		t.Errorf("期望 count=10，实际为 %v", mapVal["count"])
	}
}

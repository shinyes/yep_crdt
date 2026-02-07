package crdt_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/shinyes/yep_crdt/crdt"
)

func TestRGA(t *testing.T) {
	r := crdt.NewRGA()
	ts := time.Now().UnixNano()

	// 使用 TypeRegister 插入元素 A B C
	r.Apply(crdt.RGAOp{OriginID: "n1", TypeCode: 0, PrevID: "start", ElemID: "1", InitType: crdt.TypeRegister, InitVal: "A", Ts: ts})
	r.Apply(crdt.RGAOp{OriginID: "n1", TypeCode: 0, PrevID: "1", ElemID: "2", InitType: crdt.TypeRegister, InitVal: "B", Ts: ts + 1})
	r.Apply(crdt.RGAOp{OriginID: "n1", TypeCode: 0, PrevID: "2", ElemID: "3", InitType: crdt.TypeRegister, InitVal: "C", Ts: ts + 2})

	vals := r.Value().([]interface{})
	if fmt.Sprintf("%v%v%v", vals[0], vals[1], vals[2]) != "ABC" {
		t.Errorf("期望结果为 ABC，实际为 %v", vals)
	}

	// 在 A 之后插入 D(ts=ts+10)
	r.Apply(crdt.RGAOp{OriginID: "n2", TypeCode: 0, PrevID: "1", ElemID: "4", InitType: crdt.TypeRegister, InitVal: "D", Ts: ts + 10})

	vals = r.Value().([]interface{}) // A D B C
	str := fmt.Sprintf("%v%v%v%v", vals[0], vals[1], vals[2], vals[3])
	if str != "ADBC" {
		t.Errorf("期望结果为 ADBC，实际为 %s", str)
	}

	// 删除 B
	r.Apply(crdt.RGAOp{OriginID: "n1", TypeCode: 1, RemoveID: "2", Ts: time.Now().UnixNano()})

	vals = r.Value().([]interface{}) // A D C
	str = fmt.Sprintf("%v%v%v", vals[0], vals[1], vals[2])
	if str != "ADC" {
		t.Errorf("期望结果为 ADC，实际为 %s", str)
	}
}

func TestRGANestedMap(t *testing.T) {
	r := crdt.NewRGA()
	ts := time.Now().UnixNano()

	// 插入一个 MapCRDT 类型的元素
	err := r.Apply(crdt.RGAOp{
		OriginID: "n1",
		TypeCode: 0,
		PrevID:   "start",
		ElemID:   "item1",
		InitType: crdt.TypeMap,
		Ts:       ts,
	})
	if err != nil {
		t.Fatalf("插入 Map 元素失败: %v", err)
	}

	// 获取嵌套的 Map 实例
	child := r.GetElement("item1")
	if child == nil {
		t.Fatal("无法获取嵌套元素")
	}
	if child.Type() != crdt.TypeMap {
		t.Errorf("期望类型为 Map，实际为 %v", child.Type())
	}

	// 向嵌套 Map 添加子元素
	mapCRDT, ok := child.(*crdt.MapCRDT)
	if !ok {
		t.Fatal("无法转换为 MapCRDT")
	}

	// 初始化并设置值
	initOp := crdt.MapOp{
		OriginID: "n1",
		Key:      "name",
		IsInit:   true,
		InitType: crdt.TypeRegister,
		Ts:       ts + 1,
	}
	mapCRDT.Apply(initOp)

	setOp := crdt.MapOp{
		OriginID: "n1",
		Key:      "name",
		ChildOp: crdt.LWWOp{
			OriginID: "n1",
			Value:    "Alice",
			Ts:       ts + 2,
		},
		Ts: ts + 2,
	}
	mapCRDT.Apply(setOp)

	// 验证值
	val := r.Value().([]interface{})
	if len(val) != 1 {
		t.Fatalf("期望 1 个元素，实际为 %d", len(val))
	}

	mapVal, ok := val[0].(map[string]interface{})
	if !ok {
		t.Fatalf("期望 map 类型，实际为 %T", val[0])
	}

	if mapVal["name"] != "Alice" {
		t.Errorf("期望 name=Alice，实际为 %v", mapVal["name"])
	}
}

func TestRGAChildOpForward(t *testing.T) {
	r := crdt.NewRGA()
	ts := time.Now().UnixNano()

	// 插入一个 Map 元素
	r.Apply(crdt.RGAOp{
		OriginID: "n1",
		TypeCode: 0,
		PrevID:   "start",
		ElemID:   "item1",
		InitType: crdt.TypeMap,
		Ts:       ts,
	})

	// 先初始化子字段
	child := r.GetElement("item1")
	mapCRDT := child.(*crdt.MapCRDT)
	mapCRDT.Apply(crdt.MapOp{
		OriginID: "n1",
		Key:      "count",
		IsInit:   true,
		InitType: crdt.TypeCounter,
		Ts:       ts + 1,
	})

	// 使用子操作转发增加计数器
	childOp := crdt.MapOp{
		OriginID: "n1",
		Key:      "count",
		ChildOp: crdt.PNCounterOp{
			OriginID: "n1",
			Amount:   5,
			Ts:       ts + 2,
		},
		Ts: ts + 2,
	}

	err := r.Apply(crdt.RGAOp{
		OriginID: "n1",
		TypeCode: 2, // 子操作转发
		TargetID: "item1",
		ChildOp:  childOp,
		Ts:       ts + 2,
	})
	if err != nil {
		t.Fatalf("子操作转发失败: %v", err)
	}

	// 验证计数器值
	val := r.Value().([]interface{})
	mapVal := val[0].(map[string]interface{})
	if mapVal["count"] != int64(5) {
		t.Errorf("期望 count=5，实际为 %v", mapVal["count"])
	}
}

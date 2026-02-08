package crdt

import (
	"testing"
)

func TestMapCRDT_Basic(t *testing.T) {
	m := NewMapCRDT()

	// 1. 设置一个 LWWRegister
	lww := NewLWWRegister([]byte("User1"), 100)
	opSet := OpMapSet{
		Key:   "name",
		Value: lww,
	}
	if err := m.Apply(opSet); err != nil {
		t.Fatalf("Apply Set 失败: %v", err)
	}

	// 验证值
	val := m.GetCRDT("name")
	if val == nil {
		t.Fatal("获取 name 失败")
	}
	if string(val.Value().([]byte)) != "User1" {
		t.Fatalf("预期 User1, 实际得到 %v", val.Value())
	}

	// 2. 更新现有的 LWWRegister
	// 我们需要构造针对内部 CRDT 的 Op
	opInner := OpLWWSet{
		Value:     []byte("User1_Updated"),
		Timestamp: 200,
	}
	opUpdate := OpMapUpdate{
		Key: "name",
		Op:  opInner,
	}
	if err := m.Apply(opUpdate); err != nil {
		t.Fatalf("Apply Update 失败: %v", err)
	}

	// 验证更新
	val = m.GetCRDT("name")
	if string(val.Value().([]byte)) != "User1_Updated" {
		t.Fatalf("预期 User1_Updated, 实际得到 %v", val.Value())
	}
}

func TestMapCRDT_Merge(t *testing.T) {
	m1 := NewMapCRDT()
	m2 := NewMapCRDT()

	// m1: name=Alice (ts=100)
	lww1 := NewLWWRegister([]byte("Alice"), 100)
	m1.Apply(OpMapSet{Key: "name", Value: lww1})

	// m2: name=Bob (ts=200), age=30 (ts=100)
	lww2Name := NewLWWRegister([]byte("Bob"), 200)
	lww2Age := NewLWWRegister([]byte("30"), 100)
	m2.Apply(OpMapSet{Key: "name", Value: lww2Name})
	m2.Apply(OpMapSet{Key: "age", Value: lww2Age})

	// Merge m2 into m1
	if err := m1.Merge(m2); err != nil {
		t.Fatalf("Merge 失败: %v", err)
	}

	// 验证 m1
	// name 应该是 Bob (ts=200 > ts=100)
	nameCRDT := m1.GetCRDT("name")
	if string(nameCRDT.Value().([]byte)) != "Bob" {
		t.Errorf("Merge 后 name 应该是 Bob, 实际是 %v", nameCRDT.Value())
	}

	// age 应该存在
	ageCRDT := m1.GetCRDT("age")
	if ageCRDT == nil {
		t.Error("Merge 后 age 应该存在")
	} else if string(ageCRDT.Value().([]byte)) != "30" {
		t.Errorf("Merge 后 age 应该是 30, 实际是 %v", ageCRDT.Value())
	}
}

func TestMapCRDT_NestedMerge(t *testing.T) {
	// 测试嵌套 CRDT 的合并逻辑 (不仅仅是覆盖)
	m1 := NewMapCRDT()
	m2 := NewMapCRDT()

	// m1: counters={"A": 10}
	c1 := NewPNCounter("node1")
	c1.Apply(OpPNCounterInc{Val: 10})
	m1.Apply(OpMapSet{Key: "stats", Value: c1})

	// m2: counters={"B": 20}
	// 注意：为了模拟同一个 key 下的合并，我们需要确保它们是同类型的 CRDT
	// 在 MapCRDT.Merge 中，如果 Key 相同且 Type 相同，会调用 Bytes/Deserialize 然后 Merge
	c2 := NewPNCounter("node2")
	c2.Apply(OpPNCounterInc{Val: 20})

	// 这里有个问题：虽然我们在逻辑上认为它们是同一个计数器，但 OpMapSet 会直接覆盖 Entry。
	// 如果我们要合并，我们需要让 m2 也是基于 m1 的状态或者是对同一个 Key 的并行操作。
	// 在 MapCRDT 中，Entry 存储的是 Data []byte。
	// 如果 m2 执行 OpMapSet，它会创建一个新的 Entry 覆盖旧的？
	// 让我们看 MapCRDT.Merge:
	// if localEntry.Type != remoteEntry.Type { m.Entries[k] = remoteEntry }
	// else { recursive merge }
	// 所以只要 Type 相同，就会尝试 Merge。

	m2.Apply(OpMapSet{Key: "stats", Value: c2})

	// Merge m2 into m1
	m1.Merge(m2)

	// Result should be 10 + 20 = 30?
	// Wait, PNCounter merge merges the maps.
	// c1 has {node1: 10}
	// c2 has {node2: 20}
	// Merged should have {node1: 10, node2: 20} => sum 30.

	stats := m1.GetCRDT("stats")
	if stats == nil {
		t.Fatal("stats 丢失")
	}

	val := stats.Value().(int64)
	if val != 30 {
		t.Errorf("嵌套计数器合并失败。预期 30, 实际 %d", val)
	}
}

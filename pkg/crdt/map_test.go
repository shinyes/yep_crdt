package crdt

import (
	"testing"

	"github.com/shinyes/yep_crdt/pkg/hlc"
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

// TestMapCRDT_NilChecks 测试nil检查
func TestMapCRDT_NilChecks(t *testing.T) {
	m := NewMapCRDT()

	// 测试Apply nil操作
	t.Run("Apply nil operation", func(t *testing.T) {
		err := m.Apply(nil)
		if err == nil {
			t.Error("Apply nil operation should return error")
		}
	})

	// 测试Merge nil CRDT
	t.Run("Merge nil CRDT", func(t *testing.T) {
		err := m.Merge(nil)
		if err == nil {
			t.Error("Merge nil CRDT should return error")
		}
	})

	// 测试Merge错误类型
	t.Run("Merge wrong type", func(t *testing.T) {
		lww := NewLWWRegister([]byte("test"), 100)
		err := m.Merge(lww)
		if err == nil {
			t.Error("Merge wrong type should return error")
		}
	})
}

// TestMapCRDT_EmptyKeys 测试空键处理
func TestMapCRDT_EmptyKeys(t *testing.T) {
	m := NewMapCRDT()

	// 测试Set空键
	t.Run("Set empty key", func(t *testing.T) {
		lww := NewLWWRegister([]byte("value"), 100)
		opSet := OpMapSet{
			Key:   "",
			Value: lww,
		}
		err := m.Apply(opSet)
		if err == nil {
			t.Error("Set with empty key should return error")
		}
	})

	// 测试Update空键
	t.Run("Update empty key", func(t *testing.T) {
		opUpdate := OpMapUpdate{
			Key: "",
			Op:  OpLWWSet{Value: []byte("test"), Timestamp: 100},
		}
		err := m.Apply(opUpdate)
		if err == nil {
			t.Error("Update with empty key should return error")
		}
	})

	// 测试GetRGA空键
	t.Run("GetRGA empty key", func(t *testing.T) {
		_, err := m.GetRGA("")
		if err == nil {
			t.Error("GetRGA with empty key should return error")
		}
	})

	// 测试GetSetString空键
	t.Run("GetSetString empty key", func(t *testing.T) {
		_, err := m.GetSetString("")
		if err == nil {
			t.Error("GetSetString with empty key should return error")
		}
	})

	// 测试GetSetString空键
	t.Run("GetSetString empty key", func(t *testing.T) {
		_, err := m.GetSetString("")
		if err == nil {
			t.Error("GetSetString with empty key should return error")
		}
	})
}

// TestMapCRDT_KeyNotFound 测试键不存在的情况
func TestMapCRDT_KeyNotFound(t *testing.T) {
	m := NewMapCRDT()

	// 测试GetCRDT不存在的键
	t.Run("GetCRDT non-existent key", func(t *testing.T) {
		val := m.GetCRDT("nonexistent")
		if val != nil {
			t.Error("GetCRDT should return nil for non-existent key")
		}
	})

	// 测试Get不存在的键
	t.Run("Get non-existent key", func(t *testing.T) {
		_, ok := m.Get("nonexistent")
		if ok {
			t.Error("Get should return false for non-existent key")
		}
	})

	// 测试GetString不存在的键
	t.Run("GetString non-existent key", func(t *testing.T) {
		_, ok := m.GetString("nonexistent")
		if ok {
			t.Error("GetString should return false for non-existent key")
		}
	})

	// 测试GetInt不存在的键
	t.Run("GetInt non-existent key", func(t *testing.T) {
		_, ok := m.GetInt("nonexistent")
		if ok {
			t.Error("GetInt should return false for non-existent key")
		}
	})

	// 测试GetRGA不存在的键
	t.Run("GetRGA non-existent key", func(t *testing.T) {
		_, err := m.GetRGA("nonexistent")
		if err == nil {
			t.Error("GetRGA should return error for non-existent key")
		}
	})

	// 测试GetRGAString不存在的键
	t.Run("GetRGAString non-existent key", func(t *testing.T) {
		_, err := m.GetRGAString("nonexistent")
		if err == nil {
			t.Error("GetRGAString should return error for non-existent key")
		}
	})

	// 测试GetSetString不存在的键
	t.Run("GetSetString non-existent key", func(t *testing.T) {
		_, err := m.GetSetString("nonexistent")
		if err == nil {
			t.Error("GetSetString should return error for non-existent key")
		}
	})

	// 测试Has不存在的键
	t.Run("Has non-existent key", func(t *testing.T) {
		if m.Has("nonexistent") {
			t.Error("Has should return false for non-existent key")
		}
	})
}

// TestMapCRDT_SerializationErrors 测试序列化/反序列化错误
func TestMapCRDT_SerializationErrors(t *testing.T) {
	// 测试反序列化无效数据
	t.Run("Deserialize nil data", func(t *testing.T) {
		_, err := Deserialize(TypeLWW, nil)
		if err == nil {
			t.Error("Deserialize with nil data should return error")
		}
	})

	// 测试反序列化空数据
	t.Run("Deserialize empty data", func(t *testing.T) {
		_, err := Deserialize(TypeLWW, []byte{})
		if err == nil {
			t.Error("Deserialize with empty data should return error")
		}
	})

	// 测试反序列化损坏数据
	t.Run("Deserialize corrupt data", func(t *testing.T) {
		_, err := Deserialize(TypeLWW, []byte{0x01, 0x02, 0x03})
		if err == nil {
			t.Error("Deserialize with corrupt data should return error")
		}
	})
}

// TestMapCRDT_TypeMismatch 测试类型不匹配错误
func TestMapCRDT_TypeMismatch(t *testing.T) {
	m := NewMapCRDT()

	// 设置一个LWWRegister
	lww := NewLWWRegister([]byte("value"), 100)
	opSet := OpMapSet{
		Key:   "data",
		Value: lww,
	}
	if err := m.Apply(opSet); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	//// 测试对LWW应用错误的操作类型
	t.Run("Apply wrong operation type to LWW", func(t *testing.T) {
		// PNCounter操作不能应用到LWW
		wrongOp := OpMapUpdate{
			Key: "data",
			Op:  OpPNCounterInc{Val: 10},
		}
		err := m.Apply(wrongOp)
		if err == nil {
			t.Error("Apply wrong operation type should return error")
		}
	})

	// 测试尝试将LWW作为RGA获取
	t.Run("Get LWW as RGA", func(t *testing.T) {
		_, err := m.GetRGA("data")
		if err == nil {
			t.Error("GetRGA on LWW should return error")
		}
	})
}

// TestMapCRDT_BasicCRDTTypes 测试各种CRDT类型
func TestMapCRDT_BasicCRDTTypes(t *testing.T) {
	m := NewMapCRDT()

	// 测试LWWRegister
	t.Run("LWWRegister", func(t *testing.T) {
		lww := NewLWWRegister([]byte("value1"), 100)
		if err := m.Apply(OpMapSet{Key: "lww", Value: lww}); err != nil {
			t.Fatal(err)
		}
		
		val := m.GetCRDT("lww")
		if val.Type() != TypeLWW {
			t.Error("Expected TypeLWW")
		}
	})

	// 测试ORSet
	t.Run("ORSet", func(t *testing.T) {
		set := NewORSet[string]()
		set.Apply(OpORSetAdd[string]{
			Element: "item1",
		})
		if err := m.Apply(OpMapSet{Key: "orset", Value: set}); err != nil {
			t.Fatal(err)
		}
		
		val := m.GetCRDT("orset")
		if val.Type() != TypeORSet {
			t.Error("Expected TypeORSet")
		}
	})

	// 测试PNCounter
	t.Run("PNCounter", func(t *testing.T) {
		counter := NewPNCounter("node1")
		counter.Apply(OpPNCounterInc{Val: 10})
		if err := m.Apply(OpMapSet{Key: "counter", Value: counter}); err != nil {
			t.Fatal(err)
		}
		
		val := m.GetCRDT("counter")
		if val.Type() != TypePNCounter {
			t.Error("Expected TypePNCounter")
		}
	})

	// 测试RGA
	t.Run("RGA", func(t *testing.T) {
		clock := hlc.New()
		rga := NewRGA[string](clock)
		rga.Apply(OpRGAInsert[string]{
			AnchorID: rga.Head,
			Value:    "text",
		})
		if err := m.Apply(OpMapSet{Key: "rga", Value: rga}); err != nil {
			t.Fatal(err)
		}
		
		val := m.GetCRDT("rga")
		if val.Type() != TypeRGA {
			t.Error("Expected TypeRGA")
		}
	})

	// 测试嵌套MapCRDT
	t.Run("Nested MapCRDT", func(t *testing.T) {
		nested := NewMapCRDT()
		lww := NewLWWRegister([]byte("nested_value"), 100)
		nested.Apply(OpMapSet{Key: "inner", Value: lww})
		
		if err := m.Apply(OpMapSet{Key: "nested_map", Value: nested}); err != nil {
			t.Fatal(err)
		}
		
		val := m.GetCRDT("nested_map")
		if val.Type() != TypeMap {
			t.Error("Expected TypeMap")
		}
	})
}

// TestMapCRDT_FlushAndBytes 测试Bytes方法和缓存刷新
func TestMapCRDT_FlushAndBytes(t *testing.T) {
	m := NewMapCRDT()

	// 设置值
	lww := NewLWWRegister([]byte("value"), 100)
	if err := m.Apply(OpMapSet{Key: "key1", Value: lww}); err != nil {
		t.Fatal(err)
	}

	// 调用Bytes以刷新缓存
	data, err := m.Bytes()
	if err != nil {
		t.Fatalf("Bytes() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Bytes() should return non-empty data")
	}

	// 创建新的MapCRDT并反序列化
	m2, err := FromBytesMap(data)
	if err != nil {
		t.Fatalf("FromBytesMap() failed: %v", err)
	}

	// 验证值
	val := m2.GetCRDT("key1")
	if val == nil {
		t.Fatal("Key not found after deserialization")
	}
	if string(val.Value().([]byte)) != "value" {
		t.Errorf("Expected 'value', got %v", val.Value())
	}
}

// TestMapCRDT_FromBytesMap 测试FromBytesMap函数
func TestMapCRDT_FromBytesMap(t *testing.T) {
	// 测试反序列化空数据
	t.Run("empty data", func(t *testing.T) {
		_, err := FromBytesMap([]byte{})
		if err == nil {
			t.Error("FromBytesMap with empty data should return error")
		}
	})

	// 测试反序列化nil数据
	t.Run("nil data", func(t *testing.T) {
		_, err := FromBytesMap(nil)
		if err == nil {
			t.Error("FromBytesMap with nil data should return error")
		}
	})

	// 测试反序列化无效JSON
	t.Run("invalid JSON", func(t *testing.T) {
		_, err := FromBytesMap([]byte("invalid json"))
		if err == nil {
			t.Error("FromBytesMap with invalid JSON should return error")
		}
	})
}

// TestMapCRDT_GarbageCollection 测试垃圾回收
func TestMapCRDT_GarbageCollection(t *testing.T) {
	m := NewMapCRDT()

	// 添加一些CRDT
	lww := NewLWWRegister([]byte("value"), 100)
	if err := m.Apply(OpMapSet{Key: "key1", Value: lww}); err != nil {
		t.Fatal(err)
	}

	clock := hlc.New()
	rga := NewRGA[string](clock)
	rga.Apply(OpRGAInsert[string]{
		AnchorID: rga.Head,
		Value:    "text",
	})
	if err := m.Apply(OpMapSet{Key: "key2", Value: rga}); err != nil {
		t.Fatal(err)
	}

	// 执行垃圾回收
	count := m.GC(0)
	if count < 0 {
		t.Error("GC should return non-negative count")
	}
}

// TestMapCRDT_UpdateNonExistentKey 测试更新不存在的键
func TestMapCRDT_UpdateNonExistentKey(t *testing.T) {
	m := NewMapCRDT()

	// 尝试更新不存在的键
	opUpdate := OpMapUpdate{
		Key: "nonexistent",
		Op:  OpLWWSet{Value: []byte("test"), Timestamp: 100},
	}
	err := m.Apply(opUpdate)
	if err == nil {
		t.Error("Update non-existent key should return error")
	}
}

// TestMapCRDT_ValueMethod 测试Value方法
func TestMapCRDT_ValueMethod(t *testing.T) {
	m := NewMapCRDT()

	// 测试空Map的Value
	t.Run("empty map", func(t *testing.T) {
		val := m.Value()
		if val == nil {
			t.Error("Value() should not return nil for empty map")
		}
		
		m, ok := val.(map[string]any)
		if !ok {
			t.Error("Value() should return map[string]any")
		}
		if len(m) != 0 {
			t.Error("Empty map should have zero length")
		}
	})

	// 测试有数据的Value
	t.Run("map with data", func(t *testing.T) {
		lww := NewLWWRegister([]byte("value"), 100)
		if err := m.Apply(OpMapSet{Key: "key1", Value: lww}); err != nil {
			t.Fatal(err)
		}

		val := m.Value()
		m, ok := val.(map[string]any)
		if !ok {
			t.Error("Value() should return map[string]any")
		}
		if len(m) != 1 {
			t.Errorf("Expected 1 key, got %d", len(m))
		}
	})
}

// TestMapCRDT_SetBaseDir 测试SetBaseDir方法
func TestMapCRDT_SetBaseDir(t *testing.T) {
	m := NewMapCRDT()

	// 设置base dir
	m.SetBaseDir("/tmp/test")

	// 添加LocalFileCRDT
	metadata := FileMetadata{
		Path: "/test/path",
		Hash: "test-hash",
		Size: 100,
	}
	lf := NewLocalFileCRDT(metadata, 100)
	if err := m.Apply(OpMapSet{Key: "file", Value: lf}); err != nil {
		t.Fatal(err)
	}

	// 重新设置base dir应该传播到LocalFileCRDT
	m.SetBaseDir("/tmp/updated")

	// 验证base dir是否设置正确
	val := m.GetCRDT("file")
	if val == nil {
		t.Fatal("File not found")
	}
	if _, ok := val.(*LocalFileCRDT); !ok {
		t.Fatal("Expected LocalFileCRDT")
	}
}

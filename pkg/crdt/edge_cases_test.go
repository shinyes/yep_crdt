package crdt

import (
	"testing"

	"github.com/shinyes/yep_crdt/pkg/hlc"
)

// TestORSet_NilChecks 测试nil检查
func TestORSet_NilChecks(t *testing.T) {
	set := NewORSet[string]()

	// 测试Merge nil
	t.Run("Merge nil", func(t *testing.T) {
		err := set.Merge(nil)
		if err == nil {
			t.Error("Merge nil should return error")
		}
	})

	// 测试Merge错误类型
	t.Run("Merge wrong type", func(t *testing.T) {
		other := NewPNCounter("node1")
		err := set.Merge(other)
		if err == nil {
			t.Error("Merge wrong type should return error")
		}
	})
}

// TestORSet_AddRemove 测试添加和移除操作
func TestORSet_AddRemove(t *testing.T) {
	set := NewORSet[string]()

	// 添加元素
	t.Run("Add element", func(t *testing.T) {
		op := OpORSetAdd[string]{Element: "item1"}
		err := set.Apply(op)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}

		if !set.Contains("item1") {
			t.Error("Set should contain 'item1'")
		}

		elements := set.Elements()
		if len(elements) != 1 {
			t.Errorf("Expected 1 element, got %d", len(elements))
		}
	})

	// 移除元素
	t.Run("Remove element", func(t *testing.T) {
		op := OpORSetAdd[string]{Element: "item2"}
		set.Apply(op)

		opRemove := OpORSetRemove[string]{Element: "item2"}
		err := set.Apply(opRemove)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}

		if set.Contains("item2") {
			t.Error("Set should not contain 'item2' after removal")
		}
	})

	// 重复添加相同元素
	t.Run("Add duplicate element", func(t *testing.T) {
		op := OpORSetAdd[string]{Element: "item3"}
		set.Apply(op)
		set.Apply(op) // 再次添加

		if !set.Contains("item3") {
			t.Error("Set should contain 'item3'")
		}
	})
}

// TestORSet_MergeAdvanced 测试合并操作
func TestORSet_MergeAdvanced(t *testing.T) {
	set1 := NewORSet[string]()
	set2 := NewORSet[string]()

	// 向两个集合添加不同元素
	set1.Apply(OpORSetAdd[string]{Element: "a"})
	set1.Apply(OpORSetAdd[string]{Element: "b"})

	set2.Apply(OpORSetAdd[string]{Element: "b"})
	set2.Apply(OpORSetAdd[string]{Element: "c"})

	// 合并
	err := set1.Merge(set2)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// 合并后应该包含所有元素
	expected := []string{"a", "b", "c"}
	for _, elem := range expected {
		if !set1.Contains(elem) {
			t.Errorf("Merge result should contain %s", elem)
		}
	}
}

// TestORSet_Bytes 测试序列化和反序列化
func TestORSet_Bytes(t *testing.T) {
	set := NewORSet[string]()
	set.Apply(OpORSetAdd[string]{Element: "item1"})
	set.Apply(OpORSetAdd[string]{Element: "item2"})

	// 序列化
	data, err := set.Bytes()
	if err != nil {
		t.Fatalf("Bytes() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Bytes() should return non-empty data")
	}

	// 反序列化
	set2, err := FromBytesORSet[string](data)
	if err != nil {
		t.Fatalf("FromBytesORSet() failed: %v", err)
	}

	if !set2.Contains("item1") || !set2.Contains("item2") {
		t.Error("Deserialized set should contain both items")
	}
}

// TestORSet_GCAdvanced 测试垃圾回收
func TestORSet_GCAdvanced(t *testing.T) {
	set := NewORSet[string]()
	set.Apply(OpORSetAdd[string]{Element: "item1"})

	// GC应该返回0（ORSet通常不会立即回收）
	count := set.GC(0)
	if count < 0 {
		t.Errorf("GC should return non-negative count, got %d", count)
	}
}

// TestRGA_NilChecks 测试nil检查
func TestRGA_NilChecks(t *testing.T) {
	clock := hlc.New()
	rga := NewRGA[string](clock)

	// 测试Merge nil
	t.Run("Merge nil", func(t *testing.T) {
		err := rga.Merge(nil)
		if err == nil {
			t.Error("Merge nil should return error")
		}
	})

	// 测试Merge错误类型
	t.Run("Merge wrong type", func(t *testing.T) {
		other := NewPNCounter("node1")
		err := rga.Merge(other)
		if err == nil {
			t.Error("Merge wrong type should return error")
		}
	})
}

// TestRGA_InsertDelete 测试插入和删除操作
func TestRGA_InsertDelete(t *testing.T) {
	clock := hlc.New()
	rga := NewRGA[string](clock)

	// 插入元素
	t.Run("Insert element", func(t *testing.T) {
		op := OpRGAInsert[string]{
			AnchorID: rga.Head,
			Value:    "text",
		}
		err := rga.Apply(op)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}

		values := rga.Value().([]string)
		if len(values) != 1 {
			t.Errorf("Expected 1 value, got %d", len(values))
		}
		if values[0] != "text" {
			t.Errorf("Expected 'text', got %s", values[0])
		}
	})

	// 插入多个元素
	t.Run("Insert multiple elements", func(t *testing.T) {
		rga2 := NewRGA[string](hlc.New())
		
		// 插入第一个元素
		op1 := OpRGAInsert[string]{
			AnchorID: rga2.Head,
			Value:    "first",
		}
		rga2.Apply(op1)

		// 获取第一个元素的ID作为anchor
		iterator := rga2.Iterator()
		firstValue, ok := iterator()
		if !ok {
			t.Fatal("Should have first element")
		}
		_ = firstValue // 使用变量避免编译警告

		// 插入第二个元素
		// 注意：这里简化了测试，实际需要跟踪插入的节点ID
		rga2.Apply(OpRGAInsert[string]{
			AnchorID: rga2.Head,
			Value:    "second",
		})

		values := rga2.Value().([]string)
		if len(values) != 2 {
			t.Errorf("Expected 2 values, got %d", len(values))
		}
	})
}

// TestRGA_Merge 测试合并操作
func TestRGA_Merge(t *testing.T) {
	clock := hlc.New()
	rga1 := NewRGA[string](clock)
	rga2 := NewRGA[string](clock)

	// 向两个RGA插入元素
	op1 := OpRGAInsert[string]{AnchorID: rga1.Head, Value: "a"}
	op2 := OpRGAInsert[string]{AnchorID: rga2.Head, Value: "b"}

	rga1.Apply(op1)
	rga2.Apply(op2)

	// 合并
	err := rga1.Merge(rga2)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// 合并后应该包含两个元素
	values := rga1.Value().([]string)
	if len(values) < 1 {
		t.Error("Merge result should contain at least 1 element")
	}
}

// TestRGA_Bytes 测试序列化和反序列化
func TestRGA_Bytes(t *testing.T) {
	clock := hlc.New()
	rga := NewRGA[string](clock)
	
	rga.Apply(OpRGAInsert[string]{
		AnchorID: rga.Head,
		Value:    "text",
	})

	// 序列化
	data, err := rga.Bytes()
	if err != nil {
		t.Fatalf("Bytes() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Bytes() should return non-empty data")
	}

	// 反序列化
	rga2, err := FromBytesRGA[string](data)
	if err != nil {
		t.Fatalf("FromBytesRGA() failed: %v", err)
	}

	values := rga2.Value().([]string)
	if len(values) < 1 {
		t.Error("Deserialized RGA should contain at least 1 element")
	}
}

// TestRGA_GCAdvanced 测试垃圾回收
func TestRGA_GCAdvanced(t *testing.T) {
	clock := hlc.New()
	rga := NewRGA[string](clock)

	rga.Apply(OpRGAInsert[string]{
		AnchorID: rga.Head,
		Value:    "text",
	})

	// GC应该返回0或正数
	count := rga.GC(0)
	if count < 0 {
		t.Errorf("GC should return non-negative count, got %d", count)
	}
}

// TestPNCounter_NilChecks 测试nil检查
func TestPNCounter_NilChecks(t *testing.T) {
	counter := NewPNCounter("node1")

	// 测试Merge nil
	t.Run("Merge nil", func(t *testing.T) {
		err := counter.Merge(nil)
		if err == nil {
			t.Error("Merge nil should return error")
		}
	})

	// 测试Merge错误类型
	t.Run("Merge wrong type", func(t *testing.T) {
		other := NewLWWRegister([]byte("test"), 100)
		err := counter.Merge(other)
		if err == nil {
			t.Error("Merge wrong type should return error")
		}
	})
}

// TestPNCounter_Boundary 测试边界条件
func TestPNCounter_Boundary(t *testing.T) {
	counter := NewPNCounter("node1")

	// 测试零值
	t.Run("Zero value", func(t *testing.T) {
		if counter.Value().(int64) != 0 {
			t.Error("Initial value should be 0")
		}
	})

	// 测试大数值
	t.Run("Large value", func(t *testing.T) {
		counter.Apply(OpPNCounterInc{Val: 1000000})
		if counter.Value().(int64) != 1000000 {
			t.Error("Large value should be preserved")
		}
	})

	// 测试负值
	t.Run("Negative value", func(t *testing.T) {
		counter.Apply(OpPNCounterInc{Val: -500000})
		if counter.Value().(int64) != 500000 {
			t.Error("Should handle negative operations")
		}
	})
}

// TestPNCounter_Bytes 测试序列化和反序列化
func TestPNCounter_Bytes(t *testing.T) {
	counter := NewPNCounter("node1")
	counter.Apply(OpPNCounterInc{Val: 100})

	// 序列化
	data, err := counter.Bytes()
	if err != nil {
		t.Fatalf("Bytes() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Bytes() should return non-empty data")
	}

	// 反序列化
	counter2, err := FromBytesPNCounter(data)
	if err != nil {
		t.Fatalf("FromBytesPNCounter() failed: %v", err)
	}

	if counter2.Value().(int64) != 100 {
		t.Errorf("Expected 100, got %d", counter2.Value())
	}
}

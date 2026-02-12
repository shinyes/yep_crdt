package crdt

import (
	"testing"
)

// TestORSet_TombstonePersistence 测试 Tombstones 是否会被持久化
func TestORSet_TombstonePersistence(t *testing.T) {
	// 创建一个 ORSet 并添加元素
	set1 := NewORSet[string]()
	set1.Apply(OpORSetAdd[string]{Element: "A"})
	set1.Apply(OpORSetAdd[string]{Element: "B"})
	
	t.Logf("Before Remove - AddSet size: %d, Tombstones size: %d", 
		len(set1.AddSet), len(set1.Tombstones))
	
	// 删除元素 A
	set1.Apply(OpORSetRemove[string]{Element: "A"})
	
	t.Logf("After Remove - AddSet size: %d, Tombstones size: %d", 
		len(set1.AddSet), len(set1.Tombstones))
	t.Logf("After Remove - Elements: %v", set1.Elements())
	
	// 序列化
	bytes, err := set1.Bytes()
	if err != nil {
		t.Fatalf("Bytes() failed: %v", err)
	}
	
	t.Logf("Serialized size: %d bytes", len(bytes))
	
	// 反序列化
	set2, err := FromBytesORSet[string](bytes)
	if err != nil {
		t.Fatalf("FromBytesORSet failed: %v", err)
	}
	
	t.Logf("After Deserialize - AddSet size: %d, Tombstones size: %d", 
		len(set2.AddSet), len(set2.Tombstones))
	t.Logf("After Deserialize - Elements: %v", set2.Elements())
	
	// 验证 AddSet 中仍然包含 A（延迟删除）
	if _, ok := set2.AddSet["A"]; !ok {
		t.Error("Expected 'A' to be in AddSet after deserialization")
	}
	
	// 验证 Tombstones 中有删除标记
	if len(set2.Tombstones) == 0 {
		t.Error("Expected Tombstones to contain deletion markers")
	}
	
	// 验证 Elements() 只返回 B
	elements := set2.Elements()
	if len(elements) != 1 || elements[0] != "B" {
		t.Errorf("Expected Elements() to return [B], got %v", elements)
	}
	
	// 验证 Contains 的正确性
	if set2.Contains("A") {
		t.Error("Expected Contains('A') to return false")
	}
	if !set2.Contains("B") {
		t.Error("Expected Contains('B') to return true")
	}
	
	t.Log("✅ Tombstones are persisted correctly!")
}

// TestORSet_BytesBeforeGC 测试 GC 前后的 Bytes() 结果
func TestORSet_BytesBeforeGC(t *testing.T) {
	set := NewORSet[string]()
	clock := set.Clock
	
	// 添加和删除元素
	set.Apply(OpORSetAdd[string]{Element: "X"})
	set.Apply(OpORSetAdd[string]{Element: "Y"})
	
	t1 := clock.Now()
	set.Apply(OpORSetRemove[string]{Element: "X"})
	
	// GC 前序列化
	bytesBeforeGC, err := set.Bytes()
	if err != nil {
		t.Fatalf("Bytes() failed before GC: %v", err)
	}
	
	t.Logf("Before GC - serialized size: %d bytes", len(bytesBeforeGC))
	
	// 执行 GC
	removed := set.GC(t1 + 1)
	t.Logf("GC removed %d items", removed)
	
	// GC 后序列化
	bytesAfterGC, err := set.Bytes()
	if err != nil {
		t.Fatalf("Bytes() failed after GC: %v", err)
	}
	
	t.Logf("After GC - serialized size: %d bytes", len(bytesAfterGC))
	
	// GC 后的序列化数据应该更小（因为 Tombstones 被清理了）
	if len(bytesAfterGC) >= len(bytesBeforeGC) {
		t.Logf("⚠️ After GC, data size is not smaller: %d -> %d", 
			len(bytesBeforeGC), len(bytesAfterGC))
	} else {
		t.Logf("✅ After GC, data size reduced: %d -> %d", 
			len(bytesBeforeGC), len(bytesAfterGC))
	}
	
	// 验证功能正确性
	setAfterGC, err := FromBytesORSet[string](bytesAfterGC)
	if err != nil {
		t.Fatalf("FromBytesORSet failed: %v", err)
	}
	
	elements := setAfterGC.Elements()
	if len(elements) != 1 || elements[0] != "Y" {
		t.Errorf("Expected [Y], got %v", elements)
	}
}

package crdt

import (
	"testing"
)

// TestLWWRegister_NilChecks 测试nil检查
func TestLWWRegister_NilChecks(t *testing.T) {
	lww := NewLWWRegister([]byte("value"), 100)

	// 测试Merge nil
	t.Run("Merge nil", func(t *testing.T) {
		err := lww.Merge(nil)
		if err == nil {
			t.Error("Merge nil should return error")
		}
	})

	// 测试Merge错误类型
	t.Run("Merge wrong type", func(t *testing.T) {
		other := NewPNCounter("node1")
		err := lww.Merge(other)
		if err == nil {
			t.Error("Merge wrong type should return error")
		}
	})
}

// TestLWWRegister_Updates 测试更新操作
func TestLWWRegister_Updates(t *testing.T) {
	lww := NewLWWRegister([]byte("initial"), 100)

	// 测试低时间戳更新（应被忽略）
	t.Run("Update with lower timestamp", func(t *testing.T) {
		op := OpLWWSet{
			Value:     []byte("should_be_ignored"),
			Timestamp: 50, // 比初始时间戳低
		}
		err := lww.Apply(op)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
		
		if string(lww.Value().([]byte)) != "initial" {
			t.Error("Value should remain 'initial'")
		}
	})

	// 测试相同时间戳更新（LWW语义：相同时间戳不更新）
	t.Run("Update with same timestamp", func(t *testing.T) {
		op := OpLWWSet{
			Value:     []byte("same_timestamp"),
			Timestamp: 100, // 与初始时间戳相同
		}
		err := lww.Apply(op)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
		
		// 相同时间戳，值不应该被更新（LWW语义：只有更大时间戳才更新）
		if string(lww.Value().([]byte)) != "initial" {
			t.Error("Value should remain unchanged for same timestamp")
		}
	})

	// 测试高时间戳更新
	t.Run("Update with higher timestamp", func(t *testing.T) {
		op := OpLWWSet{
			Value:     []byte("higher_timestamp"),
			Timestamp: 200,
		}
		err := lww.Apply(op)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
		
		if string(lww.Value().([]byte)) != "higher_timestamp" {
			t.Error("Value should be updated for higher timestamp")
		}
	})
}

// TestLWWRegister_MergeAdvanced 测试更复杂的合并操作
func TestLWWRegister_MergeAdvanced(t *testing.T) {
	t.Run("Merge with higher timestamp", func(t *testing.T) {
		lww1 := NewLWWRegister([]byte("value1"), 100)
		lww2 := NewLWWRegister([]byte("value2"), 200)
		
		err := lww1.Merge(lww2)
		if err != nil {
			t.Fatalf("Merge failed: %v", err)
		}
		
		if string(lww1.Value().([]byte)) != "value2" {
			t.Error("Should adopt value with higher timestamp")
		}
	})

	t.Run("Merge with lower timestamp", func(t *testing.T) {
		lww1 := NewLWWRegister([]byte("value1"), 200)
		lww2 := NewLWWRegister([]byte("value2"), 100)
		
		err := lww1.Merge(lww2)
		if err != nil {
			t.Fatalf("Merge failed: %v", err)
		}
		
		if string(lww1.Value().([]byte)) != "value1" {
			t.Error("Should keep value with higher timestamp")
		}
	})

	t.Run("Bidirectional merge convergence", func(t *testing.T) {
		lww1 := NewLWWRegister([]byte("value1"), 100)
		lww2 := NewLWWRegister([]byte("value2"), 200)
		
		lww1.Merge(lww2)
		lww2.Merge(lww1)
		
		if string(lww1.Value().([]byte)) != string(lww2.Value().([]byte)) {
			t.Error("Values should converge after bidirectional merge")
		}
	})
}

// TestLWWRegister_Bytes 测试序列化和反序列化
func TestLWWRegister_Bytes(t *testing.T) {
	lww := NewLWWRegister([]byte("test_value"), 100)
	
	// 序列化
	data, err := lww.Bytes()
	if err != nil {
		t.Fatalf("Bytes() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Bytes() should return non-empty data")
	}
	
	// 反序列化
	lww2, err := FromBytesLWW(data)
	if err != nil {
		t.Fatalf("FromBytesLWW() failed: %v", err)
	}
	
	if string(lww2.Value().([]byte)) != "test_value" {
		t.Errorf("Expected 'test_value', got %v", lww2.Value())
	}
}

// TestLWWRegister_GC 测试垃圾回收
func TestLWWRegister_GC(t *testing.T) {
	lww := NewLWWRegister([]byte("value"), 100)
	
	// LWW通常没有GC，但应该有接口
	count := lww.GC(0)
	// GC应该返回0（没有垃圾可收集）
	if count != 0 {
		t.Errorf("GC should return 0 for LWW, got %d", count)
	}
}

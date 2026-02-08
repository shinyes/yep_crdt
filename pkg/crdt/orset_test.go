package crdt

import (
	"sort"
	"testing"
)

func TestORSet_Basic(t *testing.T) {
	s := NewORSet[string]()

	// 添加 "A"
	op1 := OpORSetAdd[string]{Element: "A"}
	if err := s.Apply(op1); err != nil {
		t.Fatalf("应用 op1 失败: %v", err)
	}

	// 验证 "A" 存在
	vals := s.Value().([]string)
	if len(vals) != 1 || vals[0] != "A" {
		t.Fatalf("预期 [A], 实际得到 %v", vals)
	}

	// 添加 "B"
	op2 := OpORSetAdd[string]{Element: "B"}
	s.Apply(op2)

	// 验证 "A", "B" 存在
	vals = s.Value().([]string)
	sort.Strings(vals) // 顺序不保证，所以排序后检查
	if len(vals) != 2 || vals[0] != "A" || vals[1] != "B" {
		t.Fatalf("预期 [A, B], 实际得到 %v", vals)
	}

	// 移除 "A"
	op3 := OpORSetRemove[string]{Element: "A"}
	s.Apply(op3)

	// 验证只有 "B" 存在
	vals = s.Value().([]string)
	if len(vals) != 1 || vals[0] != "B" {
		t.Fatalf("预期 [B], 实际得到 %v", vals)
	}
}

func TestORSet_Merge(t *testing.T) {
	s1 := NewORSet[string]()
	s2 := NewORSet[string]()

	// s1 添加 "A"
	s1.Apply(OpORSetAdd[string]{Element: "A"})

	// s2 添加 "B"
	s2.Apply(OpORSetAdd[string]{Element: "B"})

	// 合并 s2 到 s1
	if err := s1.Merge(s2); err != nil {
		t.Fatalf("合并失败: %v", err)
	}

	// s1 应该有 "A" 和 "B"
	vals := s1.Value().([]string)
	sort.Strings(vals)
	if len(vals) != 2 || vals[0] != "A" || vals[1] != "B" {
		t.Errorf("合并后预期 [A, B], 实际得到 %v", vals)
	}

	// 合并 s1 到 s2
	s2.Merge(s1)
	vals2 := s2.Value().([]string)
	sort.Strings(vals2)
	if len(vals2) != 2 || vals2[0] != "A" || vals2[1] != "B" {
		t.Errorf("合并后预期 [A, B], 实际得到 %v", vals2)
	}
}

func TestORSet_Merge_Remove(t *testing.T) {
	s1 := NewORSet[string]()
	s2 := NewORSet[string]()

	// s1 添加 "A"
	s1.Apply(OpORSetAdd[string]{Element: "A"})

	// 合并 s1 到 s2, 所以两者都有 "A"
	// 我们需要序列化/反序列化或者直接合并如果它们在内存中
	// 但是简单地合并它们会分发 ID。
	// 然而，ORSet 添加在本地生成一个唯一 ID。
	// 如果我们合并 s1->s2, s2 从 s1 获得 "A" 的 ID。
	s2.Merge(s1)

	// s2 移除 "A"
	s2.Apply(OpORSetRemove[string]{Element: "A"})

	// 验证 s2 为空
	if len(s2.Value().([]string)) != 0 {
		t.Fatal("s2 应该为空")
	}

	// 合并 s2 回 s1 (s1 应该看到墓碑)
	s1.Merge(s2)

	// 验证 s1 为空
	if len(s1.Value().([]string)) != 0 {
		t.Fatalf("合并墓碑后 s1 应该为空, 实际得到 %v", s1.Value())
	}
}

func TestORSet_AddRemoveAdd(t *testing.T) {
	s := NewORSet[string]()

	// 添加 "A"
	s.Apply(OpORSetAdd[string]{Element: "A"})
	// 移除 "A"
	s.Apply(OpORSetRemove[string]{Element: "A"})

	if len(s.Value().([]string)) != 0 {
		t.Fatal("预期为空集")
	}

	// 再次添加 "A" (Observed-Remove 应该允许用新 ID 重新添加)
	s.Apply(OpORSetAdd[string]{Element: "A"})

	vals := s.Value().([]string)
	if len(vals) != 1 || vals[0] != "A" {
		t.Fatalf("重新添加后预期 [A], 实际得到 %v", vals)
	}
}

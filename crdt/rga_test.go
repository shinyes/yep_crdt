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

	// A B C
	r.Apply(crdt.RGAOp{OriginID: "n1", TypeCode: 0, PrevID: "start", ElemID: "1", Value: "A", Ts: ts})
	r.Apply(crdt.RGAOp{OriginID: "n1", TypeCode: 0, PrevID: "1", ElemID: "2", Value: "B", Ts: ts + 1})
	r.Apply(crdt.RGAOp{OriginID: "n1", TypeCode: 0, PrevID: "2", ElemID: "3", Value: "C", Ts: ts + 2})

	vals := r.Value().([]interface{})
	if fmt.Sprintf("%v%v%v", vals[0], vals[1], vals[2]) != "ABC" {
		t.Errorf("期望结果为 ABC，实际为 %v", vals)
	}

	// 在 A 之后插入 X (与 B 并发)
	// 如果 X 的时间戳高于 B，它应该在 A 之后？
	// RGA 规则：兄弟节点按时间戳排序（降序？还是升序？需要检查实现）
	// 实现：如果 next.Timestamp > rOp.Ts -> 跳过。因此更新的节点在兄弟列表的开头？
	// 等等，如果我们想在 prev 之后插入。
	// 我们向前扫描，跳过更新（更大的 TS）的节点。
	// 所以是 [Prev] -> [较新] -> [较旧]。
	// 我们的代码：`if next.Timestamp > rOp.Ts`。
	// 意味着如果严格历史记录是：A(ts=0) -> B(ts=2)。
	// 我们在 A 之后插入 C(ts=1)。
	// A.Next 是 B。B.Ts(2) > C.Ts(1)。
	// 所以我们跳过 B。
	// A -> B -> C。
	// 等等，RGA 通常强制执行：时间戳更高 = 靠左（更靠近头部）？
	// 还是靠右？
	// 通常是：[Prev] -> [高 Ts] -> [低 Ts]。
	// 我们的代码在 next.Ts > op.Ts 时跳过。
	// 所以我们会越过 B。
	// A -> B -> C。
	// 因此 C 在 B 之后。
	// 这意味着较低的时间戳更靠右。
	// 所以顺序是时间戳降序。

	// 让我们测试在 A 和 B 之间插入 D。
	// A(0) -> B(2) -> C(1)？等等。
	// 如果我们在 A 之后插入 B。
	// 然后在 A 之后插入 C。
	// 如果 B.Ts=2, C.Ts=1。
	// 在 A 之后插入 C。
	// 检查 A.Next (B)。B.Ts(2) > C.Ts(1)。
	// 跳过 B。
	// 列表：A -> B -> C。
	// 正确。

	// 现在在 A 之后插入 D(ts=3)。
	// 检查 A.Next (B)。B.ts(2) < D.ts(3)。
	// 不跳过。
	// 在 B 之前插入 D。
	// A -> D -> B -> C。

	r.Apply(crdt.RGAOp{OriginID: "n2", TypeCode: 0, PrevID: "1", ElemID: "4", Value: "D", Ts: ts + 10}) // 更大的时间戳

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

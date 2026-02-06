package crdt_test

import (
	"testing"
	"time"

	"github.com/shinyes/yep_crdt/crdt"
)

func TestLWWRegister(t *testing.T) {
	r := crdt.NewLWWRegister("initial", 0)

	ts1 := time.Now().UnixNano()
	r.Apply(crdt.LWWOp{OriginID: "node1", Value: "update1", Ts: ts1})

	if val := r.Value().(string); val != "update1" {
		t.Errorf("期望 update1，实际为 %v", val)
	}

	// 旧更新（应该被忽略）
	r.Apply(crdt.LWWOp{OriginID: "node2", Value: "old_update", Ts: ts1 - 1000})

	if val := r.Value().(string); val != "update1" {
		t.Errorf("期望 update1 保持不变，实际为 %v", val)
	}

	// 新更新
	r.Apply(crdt.LWWOp{OriginID: "node3", Value: "new_update", Ts: ts1 + 1000})

	if val := r.Value().(string); val != "new_update" {
		t.Errorf("期望 new_update，实际为 %v", val)
	}
}

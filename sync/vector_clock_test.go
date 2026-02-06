package sync_test

import (
	"testing"

	"github.com/shinyes/yep_crdt/sync"
)

func TestVectorClock(t *testing.T) {
	vc1 := sync.NewVectorClock()
	vc1.Increment("A")
	vc1.Increment("B") // {A:1, B:1}

	vc2 := sync.NewVectorClock()
	vc2.Increment("A") // {A:1}

	if !vc1.Descends(vc2) {
		t.Errorf("vc1 应该涵盖 vc2")
	}

	if vc2.Descends(vc1) {
		t.Errorf("vc2 不应该涵盖 vc1")
	}

	vc2.Increment("C") // {A:1, C:1}

	// 并发
	if vc1.Descends(vc2) || vc2.Descends(vc1) {
		t.Errorf("应该是并发关系")
	}

	vc1.Merge(vc2) // {A:1, B:1, C:1}
	if !vc1.Descends(vc2) {
		t.Errorf("合并后的 vc1 应该涵盖 vc2")
	}
}

package crdt

import "testing"

func TestLWWRegister_ApplySameTimestampUsesValueTieBreaker(t *testing.T) {
	reg := NewLWWRegister([]byte("m"), 100)

	if err := reg.Apply(OpLWWSet{Value: []byte("a"), Timestamp: 100}); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if got := string(reg.Value().([]byte)); got != "a" {
		t.Fatalf("expected a to win over m at same timestamp, got %s", got)
	}

	if err := reg.Apply(OpLWWSet{Value: []byte("z"), Timestamp: 100}); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if got := string(reg.Value().([]byte)); got != "a" {
		t.Fatalf("expected a to keep winning over z at same timestamp, got %s", got)
	}
}

func TestLWWRegister_MergeSameTimestampConverges(t *testing.T) {
	a := NewLWWRegister([]byte("alpha"), 100)
	b := NewLWWRegister([]byte("beta"), 100)

	if err := a.Merge(b); err != nil {
		t.Fatalf("merge a<-b failed: %v", err)
	}
	if err := b.Merge(a); err != nil {
		t.Fatalf("merge b<-a failed: %v", err)
	}

	av := string(a.Value().([]byte))
	bv := string(b.Value().([]byte))
	if av != bv {
		t.Fatalf("expected convergence, got a=%s b=%s", av, bv)
	}
	if av != "alpha" {
		t.Fatalf("expected alpha to win by tie-breaker, got %s", av)
	}
}

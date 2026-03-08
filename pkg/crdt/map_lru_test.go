package crdt

import "testing"

func TestMapCRDT_WithMaxCacheSizeOption(t *testing.T) {
	m := NewMapCRDT(WithMaxCacheSize(2))

	if err := m.Apply(OpMapSet{Key: "a", Value: NewLWWRegister([]byte("a"), 1)}); err != nil {
		t.Fatalf("set a failed: %v", err)
	}
	if err := m.Apply(OpMapSet{Key: "b", Value: NewLWWRegister([]byte("b"), 2)}); err != nil {
		t.Fatalf("set b failed: %v", err)
	}
	if err := m.Apply(OpMapSet{Key: "c", Value: NewLWWRegister([]byte("c"), 3)}); err != nil {
		t.Fatalf("set c failed: %v", err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	if got := len(m.cache); got != 2 {
		t.Fatalf("unexpected cache size: want=2 got=%d", got)
	}
	if _, ok := m.cache["a"]; ok {
		t.Fatalf("expected oldest key a to be evicted")
	}
}

func TestMapCRDT_LRURefreshOnHitEvictsColdKey(t *testing.T) {
	m := NewMapCRDT(WithMaxCacheSize(2))

	if err := m.Apply(OpMapSet{Key: "a", Value: NewLWWRegister([]byte("a"), 1)}); err != nil {
		t.Fatalf("set a failed: %v", err)
	}
	if err := m.Apply(OpMapSet{Key: "b", Value: NewLWWRegister([]byte("b"), 2)}); err != nil {
		t.Fatalf("set b failed: %v", err)
	}

	if got := m.GetCRDT("a"); got == nil {
		t.Fatalf("expected cache hit on a")
	}

	if err := m.Apply(OpMapSet{Key: "c", Value: NewLWWRegister([]byte("c"), 3)}); err != nil {
		t.Fatalf("set c failed: %v", err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.cache["a"]; !ok {
		t.Fatalf("expected key a to stay in cache after hit refresh")
	}
	if _, ok := m.cache["b"]; ok {
		t.Fatalf("expected cold key b to be evicted")
	}
}

func TestMapCRDT_SetMaxCacheSizeShrinksImmediately(t *testing.T) {
	m := NewMapCRDT(WithMaxCacheSize(4))

	for i, key := range []string{"a", "b", "c"} {
		if err := m.Apply(OpMapSet{Key: key, Value: NewLWWRegister([]byte(key), int64(i+1))}); err != nil {
			t.Fatalf("set %s failed: %v", key, err)
		}
	}

	m.SetMaxCacheSize(1)

	m.mu.RLock()
	defer m.mu.RUnlock()
	if got := len(m.cache); got != 1 {
		t.Fatalf("unexpected cache size after shrink: want=1 got=%d", got)
	}
}

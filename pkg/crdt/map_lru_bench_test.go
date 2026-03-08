package crdt

import (
	"fmt"
	"testing"
)

func BenchmarkMapCRDT_LRU_Hit(b *testing.B) {
	m := NewMapCRDT(WithMaxCacheSize(128))
	if err := m.Apply(OpMapSet{
		Key:   "hot",
		Value: NewLWWRegister([]byte("value"), 1),
	}); err != nil {
		b.Fatalf("setup failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if m.GetCRDT("hot") == nil {
			b.Fatal("missing key hot")
		}
	}
}

func BenchmarkMapCRDT_LRU_MissLoad(b *testing.B) {
	m := NewMapCRDT(WithMaxCacheSize(128))
	if err := m.Apply(OpMapSet{
		Key:   "cold",
		Value: NewLWWRegister([]byte("value"), 1),
	}); err != nil {
		b.Fatalf("setup failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.mu.Lock()
		m.removeCacheKeyLocked("cold")
		m.mu.Unlock()
		if m.GetCRDT("cold") == nil {
			b.Fatal("missing key cold")
		}
	}
}

func BenchmarkMapCRDT_LRU_Evict(b *testing.B) {
	const (
		cacheSize = 64
		keyCount  = 256
	)
	m := NewMapCRDT(WithMaxCacheSize(cacheSize))
	keys := make([]string, keyCount)

	sample := NewLWWRegister([]byte("value"), 1)
	raw, err := sample.Bytes()
	if err != nil {
		b.Fatalf("encode sample failed: %v", err)
	}
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("k-%d", i)
		keys[i] = key
		m.Entries[key] = &Entry{
			Type: TypeLWW,
			Data: append([]byte(nil), raw...),
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%keyCount]
		if m.GetCRDT(key) == nil {
			b.Fatalf("missing key %s", key)
		}
	}
}

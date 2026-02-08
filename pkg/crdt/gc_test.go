package crdt

import (
	"testing"
	"time"

	"github.com/shinyes/yep_crdt/pkg/hlc"
)

func TestORSet_GC(t *testing.T) {
	s := NewORSet[string]()
	clock := hlc.New()
	s.Clock = clock

	// 1. Add "A" at T1
	s.Apply(OpORSetAdd[string]{Element: "A"})
	t1 := clock.Now()

	// 2. Remove "A" at T2
	time.Sleep(1 * time.Millisecond)
	s.Apply(OpORSetRemove[string]{Element: "A"})
	// t2 := clock.Now() // stored in tombstone

	if len(s.Tombstones) != 1 {
		t.Fatalf("Expected 1 tombstone, got %d", len(s.Tombstones))
	}

	// 3. GC with safeTimestamp < T2. Should keep tombstone.
	removed := s.GC(t1)
	if removed != 0 {
		t.Fatalf("Expected 0 removed, got %d", removed)
	}
	if len(s.Tombstones) != 1 {
		t.Fatalf("Expected 1 tombstone, got %d", len(s.Tombstones))
	}

	// 4. GC with safeTimestamp > T2. Should remove tombstone.
	time.Sleep(1 * time.Millisecond)
	t3 := clock.Now()
	removed = s.GC(t3)
	if removed != 1 {
		t.Fatalf("Expected 1 removed, got %d", removed)
	}
	if len(s.Tombstones) != 0 {
		t.Fatalf("Expected 0 tombstones, got %d", len(s.Tombstones))
	}
}

func TestRGA_GC(t *testing.T) {
	clock := hlc.New()
	r := NewRGA[[]byte](clock)

	// 1. Insert "A" after Head (T1)
	r.Apply(OpRGAInsert[[]byte]{AnchorID: r.Head, Value: []byte("A")})
	// Get A's ID
	var idA string
	for id, v := range r.Vertices {
		if string(v.Value) == "A" {
			idA = id
			break
		}
	}
	// t1 := r.Vertices[idA].Timestamp

	// 2. Insert "B" after "A" (T2)
	time.Sleep(1 * time.Millisecond)
	r.Apply(OpRGAInsert[[]byte]{AnchorID: idA, Value: []byte("B")})
	// Get B's ID
	var idB string
	for id, v := range r.Vertices {
		if string(v.Value) == "B" {
			idB = id
			break
		}
	}
	// t2 := r.Vertices[idB].Timestamp

	// 3. Remove "A" (T3)
	time.Sleep(1 * time.Millisecond)
	r.Apply(OpRGARemove{ID: idA})
	t3 := r.Vertices[idA].DeletedAt

	// 4. GC with T < T3. Should keep A.
	count := r.GC(t3)
	if count != 0 {
		t.Fatalf("Expected 0 removed, got %d", count)
	}
	if v, ok := r.Vertices[idA]; !ok || !v.Deleted {
		t.Fatalf("Expected A to be present and deleted")
	}

	// 5. GC with T > T3. Should remove A?
	// But A has child B! A cannot be removed if it has children.
	time.Sleep(1 * time.Millisecond)
	t4 := clock.Now()

	count = r.GC(t4)
	if count != 0 {
		t.Fatalf("Expected 0 removed (A has child B), got %d", count)
	}

	// 6. Remove B (T5)
	time.Sleep(1 * time.Millisecond)
	r.Apply(OpRGARemove{ID: idB})
	// t5 := r.Vertices[idB].DeletedAt

	// 7. GC with T > T5. Should remove B (leaf).
	time.Sleep(1 * time.Millisecond)
	t6 := clock.Now()

	count = r.GC(t6)
	if count != 1 {
		t.Fatalf("Expected 1 removed (B), got %d", count)
	}
	if _, ok := r.Vertices[idB]; ok {
		t.Fatalf("Expected B to be removed")
	}

	// 8. Run GC again. Now A should be leaf.
	count = r.GC(t6)
	if count != 1 {
		t.Fatalf("Expected 1 removed (A), got %d", count)
	}
	if _, ok := r.Vertices[idA]; ok {
		t.Fatalf("Expected A to be removed")
	}
}

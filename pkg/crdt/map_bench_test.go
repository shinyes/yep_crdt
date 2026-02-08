package crdt

import (
	"testing"

	"github.com/shinyes/yep_crdt/pkg/hlc"
)

// BenchmarkMapCRDT_Apply measures the performance of applying updates to a MapCRDT.
// We expect significant improvement due to cache optimization.
func BenchmarkMapCRDT_Apply(b *testing.B) {
	clock := hlc.New()
	m := NewMapCRDT()

	// Initialize with an RGA (simulating a document)
	rga := NewRGA[[]byte](clock)
	m.Apply(OpMapSet{Key: "doc", Value: rga})

	// Pre-fill RGA with some data to make serialization costly
	// If optimization works, "Apply" shouldn't re-serialize this 1000 items every time.
	// If it doesn't work, each op is O(N). If works, it's O(1) or O(log N).
	for i := 0; i < 1000; i++ {
		op := OpMapUpdate{
			Key: "doc",
			Op:  OpRGAInsert[[]byte]{AnchorID: rga.Head, Value: []byte("x")},
		}
		m.Apply(op)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		op := OpMapUpdate{
			Key: "doc",
			Op:  OpRGAInsert[[]byte]{AnchorID: rga.Head, Value: []byte("y")},
		}
		if err := m.Apply(op); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRGA_Value vs Iterator
func BenchmarkRGA_Traversal(b *testing.B) {
	clock := hlc.New()
	r := NewRGA[int](clock)

	// Fill with 10k items
	for i := 0; i < 10000; i++ {
		r.Apply(OpRGAInsert[int]{AnchorID: r.Head, Value: i})
	}

	b.Run("Value", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = r.Value()
		}
	})

	b.Run("Iterator", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			it := r.Iterator()
			for {
				_, ok := it()
				if !ok {
					break
				}
			}
		}
	})
}

package crdt

import (
	"testing"

	"github.com/shinyes/yep_crdt/pkg/hlc"
)

func BenchmarkRGA_Apply(b *testing.B) {
	clock := hlc.New()
	r := NewRGA(clock)
	anchor := r.Head

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := OpRGAInsert{
			AnchorID: anchor,
			Value:    []byte("x"),
		}
		r.Apply(op)
		// Update anchor to append? Or keep inserting at Head?
		// Appending is more realistic for "typing".
		// But getting last ID is O(N) traversal in our implementation.
		// So we just insert at Head to test Apply overhead.
	}
}

func BenchmarkRGA_Merge_Incremental(b *testing.B) {
	// Setup: Two large independent RGAs
	// Merging one into another.
	size := 1000
	clock1 := hlc.New()
	r1 := NewRGA(clock1)

	clock2 := hlc.New()
	r2 := NewRGA(clock2)

	// Fill r2
	last := r2.Head
	for i := 0; i < size; i++ {
		op := OpRGAInsert{AnchorID: last, Value: []byte("y")}
		r2.Apply(op)
		// Find new ID?
		// Inspect r2 vertices to find the one we just added?
		// Simpler: iterate edges[last]
		children := r2.edges[last]
		last = children[0].ID
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset r1
		r1 = NewRGA(clock1)
		r1.Merge(r2)
	}
}

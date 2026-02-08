package crdt

import (
	"fmt"
	"testing"
	"time"

	"github.com/shinyes/yep_crdt/pkg/hlc"
)

func TestRGABasic(t *testing.T) {
	clock := hlc.New()
	r := NewRGA[[]byte](clock)

	// Insert "A" after Head
	op1 := OpRGAInsert[[]byte]{AnchorID: r.Head, Value: []byte("A")}
	if err := r.Apply(op1); err != nil {
		t.Fatalf("Apply op1 failed: %v", err)
	}

	// Verify "A" is next to Head
	valsBytes := r.Value().([][]byte)
	if len(valsBytes) != 1 || string(valsBytes[0]) != "A" {
		t.Fatalf("Expected [A], got %v", valsBytes)
	}

	// Insert "B" after "A"
	// We need to find ID of "A".
	var idA string
	for id, v := range r.Vertices {
		if string(v.Value) == "A" {
			idA = id
			break
		}
	}

	op2 := OpRGAInsert[[]byte]{AnchorID: idA, Value: []byte("B")}
	r.Apply(op2)

	valsBytes = r.Value().([][]byte)
	if len(valsBytes) != 2 || string(valsBytes[1]) != "B" {
		t.Fatalf("Expected [A, B], got %v", valsBytes)
	}
}

func TestRGAConcurrentInsert(t *testing.T) {
	// Simulate two replicas starting state
	clock1 := hlc.New()
	r1 := NewRGA[[]byte](clock1)

	// Create r2 from r1 (clone)
	r2Bytes, _ := r1.Bytes()
	r2, _ := FromBytesRGA[[]byte](r2Bytes)
	r2.Clock = hlc.New() // Give r2 its own clock

	// Replica 1 inserts "A" after Head at T1
	op1 := OpRGAInsert[[]byte]{AnchorID: r1.Head, Value: []byte("A")}
	r1.Apply(op1)

	// Replica 2 inserts "B" after Head at T2 (T2 > T1)
	time.Sleep(1 * time.Millisecond)
	op2 := OpRGAInsert[[]byte]{AnchorID: r2.Head, Value: []byte("B")}
	r2.Apply(op2)

	// Merge r2 into r1
	if err := r1.Merge(r2); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// Merge r1 into r2
	if err := r2.Merge(r1); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	vals1 := r1.Value().([][]byte)
	vals2 := r2.Value().([][]byte)

	str1 := fmt.Sprintf("%s, %s", vals1[0], vals1[1])
	str2 := fmt.Sprintf("%s, %s", vals2[0], vals2[1])

	if str1 != str2 {
		t.Fatalf("Divergence! R1: %v, R2: %v", vals1, vals2)
	}

	// Verify "B" is first because T_B > T_A
	if string(vals1[0]) != "B" {
		t.Errorf("Expected B first (newer), got %s", vals1[0])
	}
}

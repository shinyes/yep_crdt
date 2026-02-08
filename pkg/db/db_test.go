package db_test

import (
	"os"
	"testing"

	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/hlc"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestNodeIDPersistence(t *testing.T) {
	dbPath := "./tmp/test_node_id"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	// First Run: Generate NodeID
	s1, err := store.NewBadgerStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	d1 := db.Open(s1)
	nodeID1 := d1.NodeID
	if nodeID1 == "" {
		t.Fatal("NodeID should not be empty")
	}
	d1.Close()
	s1.Close()

	// Second Run: Load persistence
	s2, err := store.NewBadgerStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	d2 := db.Open(s2)
	nodeID2 := d2.NodeID
	d2.Close()
	s2.Close()

	if nodeID1 != nodeID2 {
		t.Errorf("NodeID mismatch: %s vs %s", nodeID1, nodeID2)
	}
}

func TestHLCMonotonicity(t *testing.T) {
	c := hlc.New()
	prev := c.Now()
	for i := 0; i < 1000; i++ {
		curr := c.Now()
		if curr <= prev {
			t.Errorf("HLC not monotonic: prev %d, curr %d", prev, curr)
		}
		prev = curr
	}
}

func TestHLCCausality(t *testing.T) {
	c1 := hlc.New()
	c2 := hlc.New()

	// C1 generates an event
	ts1 := c1.Now()

	// C2 receives it and updates
	c2.Update(ts1)
	ts2 := c2.Now()

	if ts2 <= ts1 {
		t.Errorf("C2 should be after C1: ts1 %d, ts2 %d", ts1, ts2)
	}

	// Verify physical time didn't jump too far ahead (unless forced)
	phys1 := hlc.Physical(ts1)
	phys2 := hlc.Physical(ts2)

	// They should be close (within same millisecond or +1 if logical overflowed)
	// But since this is a unit test running fast, likely same ms.
	if phys2 < phys1 {
		t.Errorf("Physical time went backwards? %d -> %d", phys1, phys2)
	}
}

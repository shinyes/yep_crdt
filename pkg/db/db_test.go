package db

import (
	"os"
	"testing"
	"time"

	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestDB_HLC(t *testing.T) {
	// Setup temporary DB
	tmpDir := "./tmp_test_db_hlc"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		t.Fatal(err)
	}

	s, err := store.NewBadgerStore(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	myDB := Open(s)
	defer myDB.Close()

	// 1. Test Now()
	t1 := myDB.Now()
	if t1 == 0 {
		t.Error("Expected non-zero timestamp from Now()")
	}

	time.Sleep(1 * time.Millisecond)

	t2 := myDB.Now()
	if t2 <= t1 {
		t.Errorf("Expected time to advance. t1=%d, t2=%d", t1, t2)
	}

	// 2. Test Clock() access
	clock := myDB.Clock()
	if clock == nil {
		t.Fatal("Expected Clock() to return non-nil instance")
	}

	// Update clock manually (simulate receiving message from future)
	futureTs := t2 + 100000
	clock.Update(futureTs)

	t3 := myDB.Now()
	if t3 <= futureTs {
		t.Errorf("Expected clock to catch up to futureTs. t3=%d, futureTs=%d", t3, futureTs)
	}
}

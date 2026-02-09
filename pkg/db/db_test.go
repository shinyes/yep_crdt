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

	myDB := Open(s, "test-db-hlc")
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

func TestDB_DatabaseID(t *testing.T) {
	// Setup
	tmpDir := "./tmp_test_db_id"
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

	// 1. First Open - should succeed and persist ID
	db1 := Open(s, "my-tenant")
	if db1.DatabaseID != "my-tenant" {
		t.Errorf("Expected DatabaseID 'my-tenant', got '%s'", db1.DatabaseID)
	}
	// Do NOT close s, as we want to reuse it. But db1 doesn't hold lock on store, just wraps it.
	// Actually Open(s) reuses s.

	// 2. Re-open with SAME ID - should succeed
	db2 := Open(s, "my-tenant")
	if db2.DatabaseID != "my-tenant" {
		t.Errorf("Expected DatabaseID 'my-tenant', got '%s'", db2.DatabaseID)
	}

	// 3. Re-open with DIFFERENT ID - should panic
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected Open to panic on ID mismatch")
		} else {
			expectedMsg := "Database ID mismatch: expected my-tenant, got other-tenant"
			if r != expectedMsg {
				t.Errorf("Panic mismatch. Want '%s', got '%v'", expectedMsg, r)
			}
		}
	}()

	_ = Open(s, "other-tenant")
}

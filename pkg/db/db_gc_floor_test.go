package db

import (
	"testing"

	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestDB_GCFloor_DefaultZero(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := Open(s, "test-db")
	if got := database.GCFloor(); got != 0 {
		t.Fatalf("expected default gc floor 0, got %d", got)
	}
}

func TestDB_SetGCFloor_Monotonic(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := Open(s, "test-db")
	if err := database.SetGCFloor(100); err != nil {
		t.Fatalf("set gc floor failed: %v", err)
	}
	if got := database.GCFloor(); got != 100 {
		t.Fatalf("expected gc floor 100, got %d", got)
	}

	if err := database.SetGCFloor(80); err != nil {
		t.Fatalf("set gc floor failed: %v", err)
	}
	if got := database.GCFloor(); got != 100 {
		t.Fatalf("gc floor should be monotonic, expected 100, got %d", got)
	}

	if err := database.SetGCFloor(120); err != nil {
		t.Fatalf("set gc floor failed: %v", err)
	}
	if got := database.GCFloor(); got != 120 {
		t.Fatalf("expected gc floor 120, got %d", got)
	}
}

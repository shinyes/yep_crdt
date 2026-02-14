package db

import (
	"testing"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestOnChangeDetailed_ReportsChangedColumns(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := Open(s, "change-event-db")
	defer database.Close()

	err = database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "tags", Type: meta.ColTypeString, CrdtType: meta.CrdtORSet},
		},
	})
	if err != nil {
		t.Fatalf("define table failed: %v", err)
	}

	events := make([]ChangeEvent, 0, 4)
	database.OnChangeDetailed(func(event ChangeEvent) {
		events = append(events, event)
	})

	key, _ := uuid.NewV7()
	if err := database.Update(func(tx *Tx) error {
		return tx.Table("users").Set(key, map[string]any{"name": "Alice"})
	}); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	if err := database.Update(func(tx *Tx) error {
		return tx.Table("users").Add(key, "tags", "dev")
	}); err != nil {
		t.Fatalf("add failed: %v", err)
	}

	if err := database.Update(func(tx *Tx) error {
		return tx.Table("users").Remove(key, "tags", "dev")
	}); err != nil {
		t.Fatalf("remove failed: %v", err)
	}

	if len(events) < 3 {
		t.Fatalf("expected at least 3 events, got %d", len(events))
	}

	if events[0].TableName != "users" || events[0].Key != key || !contains(events[0].Columns, "name") {
		t.Fatalf("unexpected first event: %+v", events[0])
	}

	last := events[len(events)-1]
	if last.TableName != "users" || last.Key != key || !contains(last.Columns, "tags") {
		t.Fatalf("unexpected last event: %+v", last)
	}
}

func contains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

package db

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestObservedChange_LocalWriteUpdatesWaiterAndCallback(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database, err := Open(s, "observed-local-db")
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	defer database.Close()

	if err := database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}); err != nil {
		t.Fatalf("define table failed: %v", err)
	}

	callbackCh := make(chan ChangeEvent, 1)
	database.OnObservedChangeDetailed(func(event ChangeEvent) {
		callbackCh <- event
	})

	key, _ := uuid.NewV7()
	done := make(chan ObservedChange, 1)
	go func() {
		change, ok := database.WaitObservedChange(0, 2*time.Second)
		if ok {
			done <- change
		}
	}()

	if err := database.Update(func(tx *Tx) error {
		return tx.Table("users").Set(key, map[string]any{"name": "Alice"})
	}); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	select {
	case change := <-done:
		if change.Sequence == 0 {
			t.Fatal("expected observed sequence to advance")
		}
		if change.Event.TableName != "users" || change.Event.Key != key {
			t.Fatalf("unexpected observed change: %+v", change)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for observed change")
	}

	select {
	case event := <-callbackCh:
		if event.TableName != "users" || event.Key != key {
			t.Fatalf("unexpected callback event: %+v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for observed callback")
	}
}

func TestWaitObservedChange_TimesOutWithoutNewEvent(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database, err := Open(s, "observed-timeout-db")
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	defer database.Close()

	change, ok := database.WaitObservedChange(0, 50*time.Millisecond)
	if ok {
		t.Fatalf("expected timeout without observed change, got %+v", change)
	}
	if change.Sequence != 0 {
		t.Fatalf("expected zero observed sequence on timeout, got %d", change.Sequence)
	}
}

func TestWaitObservedChangeForTable_IgnoresUnrelatedTables(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database, err := Open(s, "observed-filter-db")
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	defer database.Close()

	for _, schema := range []*meta.TableSchema{
		{
			Name: "users",
			Columns: []meta.ColumnSchema{
				{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			},
		},
		{
			Name: "docs",
			Columns: []meta.ColumnSchema{
				{Name: "title", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			},
		},
	} {
		if err := database.DefineTable(schema); err != nil {
			t.Fatalf("define table %s failed: %v", schema.Name, err)
		}
	}

	done := make(chan ObservedChange, 1)
	go func() {
		change, ok := database.WaitObservedChangeForTable("users", 0, 2*time.Second)
		if ok {
			done <- change
		}
	}()

	docKey, _ := uuid.NewV7()
	if err := database.Update(func(tx *Tx) error {
		return tx.Table("docs").Set(docKey, map[string]any{"title": "Unrelated"})
	}); err != nil {
		t.Fatalf("write docs failed: %v", err)
	}

	select {
	case change := <-done:
		t.Fatalf("unexpected filtered wakeup from unrelated table: %+v", change)
	case <-time.After(150 * time.Millisecond):
	}

	userKey, _ := uuid.NewV7()
	if err := database.Update(func(tx *Tx) error {
		return tx.Table("users").Set(userKey, map[string]any{"name": "Alice"})
	}); err != nil {
		t.Fatalf("write users failed: %v", err)
	}

	select {
	case change := <-done:
		if change.Event.TableName != "users" || change.Event.Key != userKey {
			t.Fatalf("unexpected filtered change: %+v", change)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for filtered observed change")
	}
}

func TestWaitObservedChangeForTableColumns_IgnoresUnrelatedColumnsButAcceptsFullRow(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database, err := Open(s, "observed-column-filter-db")
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	defer database.Close()

	if err := database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "email", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}); err != nil {
		t.Fatalf("define users failed: %v", err)
	}

	done := make(chan ObservedChange, 1)
	go func() {
		change, ok := database.WaitObservedChangeForTableColumns("users", []string{"name"}, 0, 2*time.Second)
		if ok {
			done <- change
		}
	}()

	key, _ := uuid.NewV7()
	if err := database.Update(func(tx *Tx) error {
		return tx.Table("users").Set(key, map[string]any{"email": "alice@example.com"})
	}); err != nil {
		t.Fatalf("write users email failed: %v", err)
	}

	select {
	case change := <-done:
		t.Fatalf("unexpected filtered wakeup from unrelated column: %+v", change)
	case <-time.After(150 * time.Millisecond):
	}

	// columns=nil represents a full-row or unknown-column observed change and must match.
	database.NotifyObservedChangeDetailed("users", key, nil)

	select {
	case change := <-done:
		if change.Event.TableName != "users" || change.Event.Key != key {
			t.Fatalf("unexpected observed change after full-row notify: %+v", change)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for filtered observed change after full-row notify")
	}
}

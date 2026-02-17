package sync

import (
	"testing"

	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func mustOpenDB(t *testing.T, s store.Store, databaseID string, opts ...db.Option) *db.DB {
	t.Helper()
	database, err := db.Open(s, databaseID, opts...)
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	return database
}

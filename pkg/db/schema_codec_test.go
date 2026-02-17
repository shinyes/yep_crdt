package db

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestSchemaAwareCodec_GetAndFind(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database, err := Open(s, "schema-aware-codec-db")
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	defer database.Close()

	err = database.DefineTable(&meta.TableSchema{
		Name: "events",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "payload", Type: meta.ColTypeBytes, CrdtType: meta.CrdtLWW},
			{Name: "score", Type: meta.ColTypeFloat, CrdtType: meta.CrdtLWW},
			{Name: "active", Type: meta.ColTypeBool, CrdtType: meta.CrdtLWW},
			{Name: "created_at", Type: meta.ColTypeTimestamp, CrdtType: meta.CrdtLWW},
		},
		Indexes: []meta.IndexSchema{
			{Name: "idx_score", Columns: []string{"score"}},
			{Name: "idx_active", Columns: []string{"active"}},
			{Name: "idx_created_at", Columns: []string{"created_at"}},
		},
	})
	if err != nil {
		t.Fatalf("define table failed: %v", err)
	}

	table := database.Table("events")
	if table == nil {
		t.Fatalf("table events not found")
	}

	id1, _ := uuid.NewV7()
	id2, _ := uuid.NewV7()
	ts1 := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	ts2 := ts1.Add(2 * time.Hour)

	if err := table.Set(id1, map[string]any{
		"name":       "first",
		"payload":    []byte{0x00, 0x01, 0x02},
		"score":      1.5,
		"active":     false,
		"created_at": ts1,
	}); err != nil {
		t.Fatalf("set row1 failed: %v", err)
	}

	if err := table.Set(id2, map[string]any{
		"name":       "second",
		"payload":    []byte{0xFF, 0x10, 0x00, 0x20},
		"score":      3.75,
		"active":     true,
		"created_at": ts2,
	}); err != nil {
		t.Fatalf("set row2 failed: %v", err)
	}

	row, err := table.Get(id2)
	if err != nil {
		t.Fatalf("get row2 failed: %v", err)
	}

	if _, ok := row["name"].(string); !ok {
		t.Fatalf("name should decode to string, got %T", row["name"])
	}
	payload, ok := row["payload"].([]byte)
	if !ok {
		t.Fatalf("payload should decode to []byte, got %T", row["payload"])
	}
	if !bytes.Equal(payload, []byte{0xFF, 0x10, 0x00, 0x20}) {
		t.Fatalf("payload mismatch: got %v", payload)
	}
	if _, ok := row["score"].(float64); !ok {
		t.Fatalf("score should decode to float64, got %T", row["score"])
	}
	if active, ok := row["active"].(bool); !ok || !active {
		t.Fatalf("active should decode to bool(true), got %v (%T)", row["active"], row["active"])
	}
	createdAt, ok := row["created_at"].(time.Time)
	if !ok {
		t.Fatalf("created_at should decode to time.Time, got %T", row["created_at"])
	}
	if !createdAt.Equal(ts2) {
		t.Fatalf("created_at mismatch: got %s, want %s", createdAt, ts2)
	}
	rows, err := table.Where("score", OpGt, 2.0).
		And("active", OpEq, true).
		And("created_at", OpGte, ts2.Add(-time.Minute)).
		Find()
	if err != nil {
		t.Fatalf("find by typed conditions failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 typed row, got %d", len(rows))
	}
	if _, ok := rows[0]["created_at"].(time.Time); !ok {
		t.Fatalf("find result created_at should be time.Time, got %T", rows[0]["created_at"])
	}

	rows, err = table.Where("created_at", OpEq, ts2.Format(time.RFC3339Nano)).Find()
	if err != nil {
		t.Fatalf("find by timestamp string failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row for timestamp string query, got %d", len(rows))
	}
}

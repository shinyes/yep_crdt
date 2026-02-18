package db

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestQueryNormalization_InvalidTypedConditionReturnsError(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewBadgerStore failed: %v", err)
	}
	defer s.Close()

	database, err := Open(s, "query-normalize-invalid")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer database.Close()

	if err := database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "age", Type: meta.ColTypeInt, CrdtType: meta.CrdtLWW},
		},
	}); err != nil {
		t.Fatalf("DefineTable failed: %v", err)
	}

	users := database.Table("users")
	id := uuid.New()
	if err := users.Set(id, map[string]any{"age": 20}); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	_, err = users.Where("age", OpGt, struct{}{}).Find()
	if err == nil {
		t.Fatalf("expected typed normalization error")
	}
	if !strings.Contains(err.Error(), "normalize condition") {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = users.Where("age", OpIn, "20").FindCRDTs()
	if err == nil {
		t.Fatalf("expected OpIn normalization error")
	}
	if !strings.Contains(err.Error(), "OpIn expects []any") {
		t.Fatalf("unexpected OpIn error: %v", err)
	}
}

func TestQueryNormalization_OpInNormalizesItems(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewBadgerStore failed: %v", err)
	}
	defer s.Close()

	database, err := Open(s, "query-normalize-opin")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer database.Close()

	if err := database.DefineTable(&meta.TableSchema{
		Name: "scores",
		Columns: []meta.ColumnSchema{
			{Name: "score", Type: meta.ColTypeInt, CrdtType: meta.CrdtLWW},
		},
		Indexes: []meta.IndexSchema{
			{Name: "idx_score", Columns: []string{"score"}},
		},
	}); err != nil {
		t.Fatalf("DefineTable failed: %v", err)
	}

	scores := database.Table("scores")
	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()

	if err := scores.Set(id1, map[string]any{"score": 10}); err != nil {
		t.Fatalf("Set score 10 failed: %v", err)
	}
	if err := scores.Set(id2, map[string]any{"score": 20}); err != nil {
		t.Fatalf("Set score 20 failed: %v", err)
	}
	if err := scores.Set(id3, map[string]any{"score": 30}); err != nil {
		t.Fatalf("Set score 30 failed: %v", err)
	}

	rows, err := scores.Where("score", OpIn, []any{"10", int64(20)}).Find()
	if err != nil {
		t.Fatalf("Find with normalized OpIn failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows for OpIn query, got %d", len(rows))
	}
}

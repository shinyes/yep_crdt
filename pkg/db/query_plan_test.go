package db

import (
	"testing"

	"github.com/shinyes/yep_crdt/pkg/meta"
)

func TestSelectPlan_PrefersEqualityForSameField(t *testing.T) {
	schema := &meta.TableSchema{
		Name: "users",
		Indexes: []meta.IndexSchema{
			{ID: 1, Name: "idx_age", Columns: []string{"age"}},
		},
		Columns: []meta.ColumnSchema{
			{Name: "age", Type: meta.ColTypeInt, CrdtType: meta.CrdtLWW},
		},
	}

	q := &Query{
		table: &Table{schema: schema},
		conditions: []Condition{
			{Field: "age", Op: OpGt, Value: 20},
			{Field: "age", Op: OpEq, Value: 25},
		},
		columnTypes: buildColumnTypeMap(schema),
	}

	bestIndexID, bestPrefix, bestScore, bestRangeCond := q.selectPlan()
	if bestIndexID != 1 {
		t.Fatalf("unexpected best index: got=%d want=%d", bestIndexID, 1)
	}
	if bestScore <= 0 {
		t.Fatalf("expected index scan plan score > 0, got=%d", bestScore)
	}
	if len(bestPrefix) != 1 || bestPrefix[0] != 25 {
		t.Fatalf("expected equality prefix [25], got=%v", bestPrefix)
	}
	if bestRangeCond != nil {
		t.Fatalf("range condition should not be selected when equality exists, got=%+v", *bestRangeCond)
	}
}

func TestSelectPlan_PrefersLongerEqualityPrefix(t *testing.T) {
	schema := &meta.TableSchema{
		Name: "users",
		Indexes: []meta.IndexSchema{
			{ID: 1, Name: "idx_a", Columns: []string{"a"}},
			{ID: 2, Name: "idx_ab", Columns: []string{"a", "b"}},
		},
		Columns: []meta.ColumnSchema{
			{Name: "a", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "b", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}

	q := &Query{
		table: &Table{schema: schema},
		conditions: []Condition{
			{Field: "a", Op: OpEq, Value: "x"},
			{Field: "b", Op: OpEq, Value: "y"},
		},
		columnTypes: buildColumnTypeMap(schema),
	}

	bestIndexID, bestPrefix, bestScore, _ := q.selectPlan()
	if bestIndexID != 2 {
		t.Fatalf("expected idx_ab to be selected, got=%d", bestIndexID)
	}
	if bestScore <= 0 {
		t.Fatalf("expected index scan plan score > 0, got=%d", bestScore)
	}
	if len(bestPrefix) != 2 || bestPrefix[0] != "x" || bestPrefix[1] != "y" {
		t.Fatalf("unexpected best prefix: %v", bestPrefix)
	}
}

func TestSelectPlan_DoesNotUseIndexForOnlyNonMonotonicCondition(t *testing.T) {
	schema := &meta.TableSchema{
		Name: "users",
		Indexes: []meta.IndexSchema{
			{ID: 1, Name: "idx_age", Columns: []string{"age"}},
		},
		Columns: []meta.ColumnSchema{
			{Name: "age", Type: meta.ColTypeInt, CrdtType: meta.CrdtLWW},
		},
	}

	q := &Query{
		table: &Table{schema: schema},
		conditions: []Condition{
			{Field: "age", Op: OpNe, Value: 18},
		},
		columnTypes: buildColumnTypeMap(schema),
	}

	bestIndexID, bestPrefix, bestScore, bestRangeCond := q.selectPlan()
	if bestScore >= 0 {
		t.Fatalf("expected no usable index plan, got score=%d index=%d prefix=%v range=%v", bestScore, bestIndexID, bestPrefix, bestRangeCond)
	}
}

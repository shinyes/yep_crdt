package db

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func BenchmarkRangeQueryIntIndex_PreFilterOn(b *testing.B) {
	table := mustPrepareRangeBenchTable(b, "bench_int", "score", meta.ColTypeInt, 20000, func(i int) any {
		return i
	})
	runRangeQueryBench(b, table, false, func(t *Table) *Query {
		return t.Where("score", OpLt, 1000).OrderBy("score", true).Limit(200)
	})
}

func BenchmarkRangeQueryIntIndex_PreFilterOff(b *testing.B) {
	table := mustPrepareRangeBenchTable(b, "bench_int_no_prefilter", "score", meta.ColTypeInt, 20000, func(i int) any {
		return i
	})
	runRangeQueryBench(b, table, true, func(t *Table) *Query {
		return t.Where("score", OpLt, 1000).OrderBy("score", true).Limit(200)
	})
}

func BenchmarkRangeQueryStringIndex_PreFilterOn(b *testing.B) {
	table := mustPrepareRangeBenchTable(b, "bench_string", "name", meta.ColTypeString, 20000, func(i int) any {
		return fmt.Sprintf("k%05d", i)
	})
	runRangeQueryBench(b, table, false, func(t *Table) *Query {
		return t.Where("name", OpLt, "k01000").OrderBy("name", true).Limit(200)
	})
}

func BenchmarkRangeQueryStringIndex_PreFilterOff(b *testing.B) {
	table := mustPrepareRangeBenchTable(b, "bench_string_no_prefilter", "name", meta.ColTypeString, 20000, func(i int) any {
		return fmt.Sprintf("k%05d", i)
	})
	runRangeQueryBench(b, table, true, func(t *Table) *Query {
		return t.Where("name", OpLt, "k01000").OrderBy("name", true).Limit(200)
	})
}

func runRangeQueryBench(b *testing.B, table *Table, disablePreFilter bool, makeQuery func(*Table) *Query) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := makeQuery(table)
		q.disableIndexRangePreFilter = disablePreFilter
		rows, err := q.Find()
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		if len(rows) == 0 {
			b.Fatalf("query returned no rows")
		}
	}
}

func mustPrepareRangeBenchTable(
	b *testing.B,
	tableName string,
	colName string,
	colType meta.ColumnType,
	rowCount int,
	valueForRow func(i int) any,
) *Table {
	b.Helper()

	s, err := store.NewBadgerStore(b.TempDir())
	if err != nil {
		b.Fatalf("NewBadgerStore failed: %v", err)
	}
	b.Cleanup(func() { _ = s.Close() })

	database, err := Open(s, "bench-"+tableName)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	b.Cleanup(func() { _ = database.Close() })

	if err := database.DefineTable(&meta.TableSchema{
		Name: tableName,
		Columns: []meta.ColumnSchema{
			{Name: colName, Type: colType, CrdtType: meta.CrdtLWW},
		},
		Indexes: []meta.IndexSchema{
			{Name: "idx_" + colName, Columns: []string{colName}, Unique: false},
		},
	}); err != nil {
		b.Fatalf("DefineTable failed: %v", err)
	}

	table := database.Table(tableName)
	if table == nil {
		b.Fatalf("table %s not found", tableName)
	}

	for i := 0; i < rowCount; i++ {
		id, err := uuid.NewV7()
		if err != nil {
			b.Fatalf("uuid.NewV7 failed: %v", err)
		}
		if err := table.Set(id, map[string]any{
			colName: valueForRow(i),
		}); err != nil {
			b.Fatalf("seed row %d failed: %v", i, err)
		}
	}

	return table
}

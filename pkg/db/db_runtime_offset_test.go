package db

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/hlc"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestGCSafeTimestampByOffset_UsesPhysicalMillis(t *testing.T) {
	now := hlc.Pack(1_700_000_000_000, 12_345)
	offset := 30 * time.Second

	got := gcSafeTimestampByOffset(now, offset)
	want := hlc.SubPhysical(now, offset.Milliseconds())
	if got != want {
		t.Fatalf("unexpected safe timestamp: want=%d got=%d", want, got)
	}
}

func TestGCSafeTimestampByOffset_NonPositiveOffsetNoop(t *testing.T) {
	now := hlc.Pack(1_700_000_000_000, 1)

	if got := gcSafeTimestampByOffset(now, 0); got != now {
		t.Fatalf("zero offset should be no-op: want=%d got=%d", now, got)
	}
	if got := gcSafeTimestampByOffset(now, -10*time.Second); got != now {
		t.Fatalf("negative offset should be no-op: want=%d got=%d", now, got)
	}
}

func TestDB_GCByTimeOffset_RespectsPhysicalOffsetWindow(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	db, err := Open(s, "test-db-offset")
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}

	if err := db.DefineTable(&meta.TableSchema{
		Name: "docs",
		Columns: []meta.ColumnSchema{
			{Name: "tags", Type: meta.ColTypeString, CrdtType: meta.CrdtORSet},
		},
	}); err != nil {
		t.Fatalf("define table failed: %v", err)
	}

	table := db.Table("docs")
	if table == nil {
		t.Fatal("table not found")
	}

	key, _ := uuid.NewV7()
	tagSet := crdt.NewORSet[string]()
	tagSet.Clock = db.Clock()
	if err := tagSet.Apply(crdt.OpORSetAdd[string]{Element: "tag-1"}); err != nil {
		t.Fatalf("add tag failed: %v", err)
	}
	if err := tagSet.Apply(crdt.OpORSetRemove[string]{Element: "tag-1"}); err != nil {
		t.Fatalf("remove tag failed: %v", err)
	}
	if err := table.Set(key, map[string]any{"tags": tagSet}); err != nil {
		t.Fatalf("set row failed: %v", err)
	}

	baseline := db.Clock().Now()
	db.Clock().Update(hlc.AddPhysical(baseline, 10_000))
	result := db.GCByTimeOffset(30 * time.Second)
	if result.TombstonesRemoved != 0 {
		t.Fatalf("gc removed tombstones too early: removed=%d", result.TombstonesRemoved)
	}

	db.Clock().Update(hlc.AddPhysical(baseline, 40_000))
	result = db.GCByTimeOffset(30 * time.Second)
	if result.TombstonesRemoved == 0 {
		t.Fatalf("gc should remove tombstones after enough physical time elapses")
	}
}

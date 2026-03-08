package db

import (
	"testing"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestTableMerkle_BuildAndQuery(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database, err := Open(s, "merkle-db")
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

	users := database.Table("users")
	if users == nil {
		t.Fatal("users table not found")
	}

	k1, _ := uuid.NewV7()
	k2, _ := uuid.NewV7()
	if err := users.Set(k1, map[string]any{"name": "alice"}); err != nil {
		t.Fatalf("set k1 failed: %v", err)
	}
	if err := users.Set(k2, map[string]any{"name": "bob"}); err != nil {
		t.Fatalf("set k2 failed: %v", err)
	}

	root1, err := users.MerkleRootHash()
	if err != nil {
		t.Fatalf("merkle root failed: %v", err)
	}
	if root1 == "" {
		t.Fatal("expected non-empty merkle root")
	}

	children, err := users.MerkleChildren(0, "")
	if err != nil {
		t.Fatalf("merkle children failed: %v", err)
	}
	if len(children) == 0 {
		t.Fatal("expected non-empty root children")
	}

	leafPrefix := nibblePrefixFromUUID(k1, MerkleMaxLevel())
	leafRows, err := users.MerkleLeafRows(leafPrefix)
	if err != nil {
		t.Fatalf("merkle leaf rows failed: %v", err)
	}
	if _, ok := leafRows[k1.String()]; !ok {
		t.Fatalf("expected k1 in leaf bucket %s", leafPrefix)
	}

	if err := users.Set(k1, map[string]any{"name": "alice-v2"}); err != nil {
		t.Fatalf("update k1 failed: %v", err)
	}
	root2, err := users.MerkleRootHash()
	if err != nil {
		t.Fatalf("merkle root after update failed: %v", err)
	}
	if root1 == root2 {
		t.Fatal("expected merkle root to change after row update")
	}
}

func TestTableMerkle_RebuildOnFirstRead(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database, err := Open(s, "merkle-rebuild-db")
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

	users := database.Table("users")
	k, _ := uuid.NewV7()
	if err := users.Set(k, map[string]any{"name": "alice"}); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	// Marker is absent before first read-triggered rebuild.
	if err := s.View(func(tx store.Tx) error {
		if _, err := tx.Get(merkleBuiltKey("users")); err == nil {
			t.Fatal("unexpected merkle built marker before first merkle read")
		}
		return nil
	}); err != nil {
		t.Fatalf("view marker failed: %v", err)
	}

	root, err := users.MerkleRootHash()
	if err != nil {
		t.Fatalf("merkle root failed: %v", err)
	}
	if root == "" {
		t.Fatal("expected non-empty root after rebuild")
	}

	if err := s.View(func(tx store.Tx) error {
		val, err := tx.Get(merkleBuiltKey("users"))
		if err != nil {
			return err
		}
		if string(val) != merkleBuiltVersion {
			t.Fatalf("unexpected merkle marker version: %s", string(val))
		}
		return nil
	}); err != nil {
		t.Fatalf("verify marker failed: %v", err)
	}
}

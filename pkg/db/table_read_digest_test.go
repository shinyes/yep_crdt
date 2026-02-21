package db

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestScanRowDigest_UsesSHA256(t *testing.T) {
	s, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database, err := Open(s, "digest-db")
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

	key, _ := uuid.NewV7()
	if err := database.Update(func(tx *Tx) error {
		return tx.Table("users").Set(key, map[string]any{"name": "alice"})
	}); err != nil {
		t.Fatalf("seed row failed: %v", err)
	}

	raw, err := database.Table("users").GetRawRow(key)
	if err != nil {
		t.Fatalf("get raw row failed: %v", err)
	}
	sum := sha256.Sum256(raw)
	expected := hex.EncodeToString(sum[:])

	digests, err := database.Table("users").ScanRowDigest()
	if err != nil {
		t.Fatalf("scan row digest failed: %v", err)
	}

	if len(digests) != 1 {
		t.Fatalf("expected exactly one digest, got=%d", len(digests))
	}
	if digests[0].Key != key {
		t.Fatalf("unexpected digest key: got=%s want=%s", digests[0].Key.String(), key.String())
	}
	if digests[0].Hash != expected {
		t.Fatalf("unexpected digest hash: got=%s want=%s", digests[0].Hash, expected)
	}
	if len(digests[0].Hash) != 64 {
		t.Fatalf("digest hash should be 64 hex chars, got=%d", len(digests[0].Hash))
	}
}

func TestScanRowDigest_FailsOnMalformedRowKey(t *testing.T) {
	s, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database, err := Open(s, "digest-malformed-db")
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

	// Inject one malformed row key under users prefix.
	if err := s.Update(func(tx store.Tx) error {
		return tx.Set([]byte("/d/users/bad-key"), []byte("x"), 0)
	}); err != nil {
		t.Fatalf("inject malformed key failed: %v", err)
	}

	_, err = database.Table("users").ScanRowDigest()
	if err == nil {
		t.Fatal("expected scan row digest to fail on malformed key")
	}
	if !strings.Contains(err.Error(), "invalid key length") {
		t.Fatalf("unexpected error: %v", err)
	}
}

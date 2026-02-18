package store

import (
	"path/filepath"
	"testing"
)

func TestMultiStore_GetRejectsInvalidTenantID(t *testing.T) {
	ms := NewMultiStore(t.TempDir())

	cases := []string{
		"",
		" ",
		".",
		"..",
		"../escape",
		"tenant/a",
		`tenant\b`,
		"tenant:bad",
		filepath.Join(t.TempDir(), "abs"),
	}

	for _, tenantID := range cases {
		if _, err := ms.Get(tenantID); err == nil {
			t.Fatalf("expected tenant id %q to be rejected", tenantID)
		}
	}
}

func TestMultiStore_CloseRejectsInvalidTenantID(t *testing.T) {
	ms := NewMultiStore(t.TempDir())

	if err := ms.Close("../escape"); err == nil {
		t.Fatal("expected Close to reject traversal tenant id")
	}
}

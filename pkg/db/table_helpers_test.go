package db

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestExtractInt64_RejectsInvalidValues(t *testing.T) {
	cases := []any{
		uint64(math.MaxInt64) + 1,
		float32(1.5),
		float64(-3.14),
		math.NaN(),
		math.Inf(1),
	}

	for _, input := range cases {
		if _, err := extractInt64(input); err == nil {
			t.Fatalf("expected input %v (%T) to fail", input, input)
		}
	}
}

func TestExtractInt64_AcceptsFiniteIntegerValues(t *testing.T) {
	cases := []struct {
		input any
		want  int64
	}{
		{input: int(3), want: 3},
		{input: uint32(9), want: 9},
		{input: uint64(math.MaxInt64), want: math.MaxInt64},
		{input: float32(12), want: 12},
		{input: float64(-42), want: -42},
	}

	for _, tc := range cases {
		got, err := extractInt64(tc.input)
		if err != nil {
			t.Fatalf("extractInt64(%v/%T) failed: %v", tc.input, tc.input, err)
		}
		if got != tc.want {
			t.Fatalf("extractInt64(%v/%T) = %d, want %d", tc.input, tc.input, got, tc.want)
		}
	}
}

func TestStageFileImport_UsesTemporaryPath(t *testing.T) {
	root := t.TempDir()
	srcPath := filepath.Join(root, "src.txt")
	destPath := filepath.Join(root, "files", "docs", "a.txt")

	if err := os.WriteFile(srcPath, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write src failed: %v", err)
	}

	staged, err := stageFileImport(srcPath, destPath, "docs/a.txt")
	if err != nil {
		t.Fatalf("stageFileImport failed: %v", err)
	}
	defer cleanupStagedFileImports([]stagedFileImport{staged})

	if staged.TempPath == "" || staged.DestPath != destPath {
		t.Fatalf("unexpected staged result: %+v", staged)
	}
	if _, err := os.Stat(staged.TempPath); err != nil {
		t.Fatalf("staged temp file should exist: %v", err)
	}
	if _, err := os.Stat(destPath); !os.IsNotExist(err) {
		t.Fatalf("destination should not be written during staging: %v", err)
	}
}

func TestPromoteAndRollbackStagedFileImports(t *testing.T) {
	root := t.TempDir()
	srcPath := filepath.Join(root, "src.txt")
	destPath := filepath.Join(root, "files", "docs", "a.txt")

	if err := os.WriteFile(srcPath, []byte("new-data"), 0o644); err != nil {
		t.Fatalf("write src failed: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		t.Fatalf("mkdir dest dir failed: %v", err)
	}
	if err := os.WriteFile(destPath, []byte("old-data"), 0o644); err != nil {
		t.Fatalf("write existing dest failed: %v", err)
	}

	staged, err := stageFileImport(srcPath, destPath, "docs/a.txt")
	if err != nil {
		t.Fatalf("stageFileImport failed: %v", err)
	}
	defer cleanupStagedFileImports([]stagedFileImport{staged})

	promoted, err := promoteStagedFileImports([]stagedFileImport{staged})
	if err != nil {
		t.Fatalf("promoteStagedFileImports failed: %v", err)
	}
	if len(promoted) != 1 {
		t.Fatalf("unexpected promoted count: %d", len(promoted))
	}
	if promoted[0].BackupPath == "" {
		t.Fatalf("expected backup path when destination existed: %+v", promoted[0])
	}

	got, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("read promoted destination failed: %v", err)
	}
	if string(got) != "new-data" {
		t.Fatalf("unexpected destination content after promote: %q", string(got))
	}

	rollbackPromotedFileImports(promoted)
	restored, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("read restored destination failed: %v", err)
	}
	if string(restored) != "old-data" {
		t.Fatalf("unexpected destination content after rollback: %q", string(restored))
	}

	cleanupPromotedBackupFiles(promoted)
	if _, err := os.Stat(promoted[0].BackupPath); !os.IsNotExist(err) {
		t.Fatalf("backup file should be cleaned: %v", err)
	}
}

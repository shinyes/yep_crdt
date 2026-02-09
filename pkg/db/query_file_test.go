package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestQueryLocalFile(t *testing.T) {
	// 1. Setup DB and FileStorageDir
	tmpDir, err := os.MkdirTemp("", "db_test_files")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	fileStoreDir := filepath.Join(tmpDir, "files")
	if err := os.Mkdir(fileStoreDir, 0755); err != nil {
		t.Fatal(err)
	}

	dbStoreDir := filepath.Join(tmpDir, "db")
	s, err := store.NewBadgerStore(dbStoreDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	d := Open(s, "test-files")
	defer d.Close()

	d.SetFileStorageDir(fileStoreDir)

	// 2. Define Table
	schema := &meta.TableSchema{
		Name: "documents",
		Columns: []meta.ColumnSchema{
			{Name: "id", Type: meta.ColTypeString},   // Primary is implicit/handled by set logic
			{Name: "file", Type: meta.ColTypeString}, // Actually stores LocalFileCRDT
		},
		Indexes: []meta.IndexSchema{},
	}
	if err := d.DefineTable(schema); err != nil {
		t.Fatal(err)
	}

	table := d.Table("documents")

	// 3. Create a physical file (in a separate tmp location, not in fileStoreDir)
	fileName := "doc1.txt"
	fileContent := []byte("Hello CRDT File Content!")
	srcPath := filepath.Join(tmpDir, "src_"+fileName)
	if err := os.WriteFile(srcPath, fileContent, 0644); err != nil {
		t.Fatal(err)
	}

	// 4. Insert Record with FileImport (auto copies and calculates metadata)
	id := uuid.New()

	// Save to DB using Table.Set with FileImport
	err = table.Set(id, map[string]any{
		"id": id.String(),
		"file": FileImport{
			LocalPath:    srcPath,  // Source file
			RelativePath: fileName, // Target path in storage dir
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// 5. Query
	results, err := table.Where("id", OpEq, id.String()).FindCRDTs()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	readOnlyRow := results[0]

	// 6. Access LocalFileCRDT
	lf, err := readOnlyRow.GetLocalFile("file")
	if err != nil {
		t.Fatal(err)
	}
	if lf == nil {
		t.Fatal("expected LocalFileCRDT, got nil")
	}

	// 7. Verify Content Read
	readBytes, err := lf.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if string(readBytes) != string(fileContent) {
		t.Errorf("content mismatch: got %s, want %s", string(readBytes), string(fileContent))
	}
}

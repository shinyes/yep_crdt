package crdt

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestLocalFileCRDT(t *testing.T) {
	// 1. 设置测试环境 (临时目录)
	tmpDir, err := os.MkdirTemp("", "crdt_file_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	fileName := "test.txt"
	content := []byte("Hello, World! This is a test file.")
	filePath := filepath.Join(tmpDir, fileName)

	if err := os.WriteFile(filePath, content, 0644); err != nil {
		t.Fatal(err)
	}

	// 2. 创建 CRDT
	meta := FileMetadata{
		Path: fileName,
		Size: int64(len(content)),
		Hash: "dummy-hash",
	}
	ts := time.Now().UnixNano()
	crdt := NewLocalFileCRDT(meta, ts)
	crdt.SetBaseDir(tmpDir)

	// 3. 测试 ReadAll
	readContent, err := crdt.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(readContent) != string(content) {
		t.Errorf("ReadAll content mismatch. Got %s, want %s", readContent, content)
	}

	// 4. 测试 ReadAt
	// 读取 "World" (offset 7, length 5)
	readPart, err := crdt.ReadAt(7, 5)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if string(readPart) != "World" {
		t.Errorf("ReadAt content mismatch. Got %s, want World", readPart)
	}

	// 测试 ReadAt 超出范围
	readPart, err = crdt.ReadAt(int64(len(content))-4, 10) // 读取最后 4 个字节，加上多余的长度
	if err != nil {
		t.Fatalf("ReadAt (partial) failed: %v", err)
	}
	if string(readPart) != "ile." {
		t.Errorf("ReadAt (partial) content mismatch. Got %s, want ile.", readPart)
	}

	// 5. 测试 Merge (LWW)
	olderMeta := FileMetadata{Path: "old.txt", Hash: "old"}
	olderCRDT := NewLocalFileCRDT(olderMeta, ts-1000)

	// Merge older into newer -> 应该不改变
	if err := crdt.Merge(olderCRDT); err != nil {
		t.Fatal(err)
	}
	if crdt.metadata.Path != fileName {
		t.Errorf("Merge older should not change metadata")
	}

	newerMeta := FileMetadata{Path: "new.txt", Hash: "new"}
	newerCRDT := NewLocalFileCRDT(newerMeta, ts+1000)

	// Merge newer into current -> 应该更新
	if err := crdt.Merge(newerCRDT); err != nil {
		t.Fatal(err)
	}
	if crdt.metadata.Path != "new.txt" {
		t.Errorf("Merge newer should update metadata")
	}

	// 6. 测试序列化
	bytes, err := crdt.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := FromBytesLocalFile(bytes)
	if err != nil {
		t.Fatal(err)
	}
	if restored.metadata.Path != "new.txt" {
		t.Errorf("Deserialization failed, path mismatch")
	}
	if restored.timestamp != ts+1000 {
		t.Errorf("Deserialization failed, timestamp mismatch")
	}
}

func TestLocalFileCRDT_RejectPathTraversalRead(t *testing.T) {
	baseDir := t.TempDir()
	meta := FileMetadata{
		Path: filepath.Join("..", "outside.txt"),
		Size: 8,
		Hash: "dummy",
	}
	lf := NewLocalFileCRDT(meta, time.Now().UnixNano())
	lf.SetBaseDir(baseDir)

	if _, err := lf.ReadAll(); err == nil {
		t.Fatal("expected ReadAll to reject escaped path")
	}
	if _, err := lf.ReadAt(0, 4); err == nil {
		t.Fatal("expected ReadAt to reject escaped path")
	}
}

func TestValidateRelativePath(t *testing.T) {
	absPath := string(filepath.Separator) + filepath.Join("tmp", "abs.txt")
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "normal", input: "docs/a.txt", want: "docs/a.txt"},
		{name: "backslash normalized", input: `docs\sub\a.txt`, want: "docs/sub/a.txt"},
		{name: "traversal", input: "../escape.txt", wantErr: true},
		{name: "absolute", input: absPath, wantErr: true},
		{name: "windows drive", input: `C:\escape.txt`, wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := ValidateRelativePath(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for input %q", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for input %q: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("unexpected normalized path: want=%q got=%q", tc.want, got)
			}
		})
	}

	// Extra guard: Windows absolute path via volume+separator should also fail on Windows.
	if runtime.GOOS == "windows" {
		if _, err := ValidateRelativePath(`C:\tmp\abs.txt`); err == nil {
			t.Fatal("expected windows absolute path to be rejected")
		}
	}
}

func TestLocalFileCRDT_ApplySameTimestampUsesMetadataTieBreaker(t *testing.T) {
	ts := int64(100)
	lf := NewLocalFileCRDT(FileMetadata{
		Path: "m.txt",
		Hash: "m",
		Size: 1,
	}, ts)

	if err := lf.Apply(OpLocalFileSet{
		Metadata: FileMetadata{
			Path: "a.txt",
			Hash: "a",
			Size: 1,
		},
		Timestamp: ts,
	}); err != nil {
		t.Fatalf("apply same timestamp (smaller metadata) failed: %v", err)
	}

	got := lf.Value().(FileMetadata)
	if got.Path != "a.txt" {
		t.Fatalf("expected a.txt to win tie-breaker, got %s", got.Path)
	}

	if err := lf.Apply(OpLocalFileSet{
		Metadata: FileMetadata{
			Path: "z.txt",
			Hash: "z",
			Size: 1,
		},
		Timestamp: ts,
	}); err != nil {
		t.Fatalf("apply same timestamp (larger metadata) failed: %v", err)
	}

	got = lf.Value().(FileMetadata)
	if got.Path != "a.txt" {
		t.Fatalf("expected winner to remain a.txt, got %s", got.Path)
	}
}

func TestLocalFileCRDT_MergeSameTimestampConverges(t *testing.T) {
	ts := int64(200)
	a := NewLocalFileCRDT(FileMetadata{
		Path: "alpha.txt",
		Hash: "aa",
		Size: 10,
	}, ts)
	b := NewLocalFileCRDT(FileMetadata{
		Path: "beta.txt",
		Hash: "bb",
		Size: 20,
	}, ts)

	if err := a.Merge(b); err != nil {
		t.Fatalf("a.Merge(b) failed: %v", err)
	}
	if err := b.Merge(a); err != nil {
		t.Fatalf("b.Merge(a) failed: %v", err)
	}

	av := a.Value().(FileMetadata)
	bv := b.Value().(FileMetadata)
	if av != bv {
		t.Fatalf("expected convergence, got a=%+v b=%+v", av, bv)
	}
}

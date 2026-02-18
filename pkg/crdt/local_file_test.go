package crdt

import (
	"os"
	"path/filepath"
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

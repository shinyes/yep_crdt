package sync

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

type localFileCaptureNetwork struct {
	targetNode string
	msg        *NetworkMessage
	messages   []*NetworkMessage
}

func (n *localFileCaptureNetwork) SendHeartbeat(targetNodeID string, clock int64) error {
	return nil
}

func (n *localFileCaptureNetwork) BroadcastHeartbeat(clock int64) error {
	return nil
}

func (n *localFileCaptureNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	return nil
}

func (n *localFileCaptureNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	return nil
}

func (n *localFileCaptureNetwork) SendRawDelta(targetNodeID string, table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return nil
}

func (n *localFileCaptureNetwork) BroadcastRawDelta(table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return nil
}

func (n *localFileCaptureNetwork) SendMessage(targetNodeID string, msg *NetworkMessage) error {
	n.targetNode = targetNodeID
	if msg == nil {
		n.msg = nil
		n.messages = append(n.messages, nil)
		return nil
	}

	cloned := *msg
	cloned.Columns = append([]string(nil), msg.Columns...)
	cloned.RawData = append([]byte(nil), msg.RawData...)
	cloned.ChunkData = append([]byte(nil), msg.ChunkData...)
	if len(msg.LocalFiles) > 0 {
		cloned.LocalFiles = make([]SyncedLocalFile, 0, len(msg.LocalFiles))
		for _, f := range msg.LocalFiles {
			cloned.LocalFiles = append(cloned.LocalFiles, SyncedLocalFile{
				Path:    f.Path,
				Hash:    f.Hash,
				Size:    f.Size,
				Chunked: f.Chunked,
				Data:    append([]byte(nil), f.Data...),
			})
		}
	}
	n.msg = &cloned
	n.messages = append(n.messages, &cloned)
	return nil
}

func (n *localFileCaptureNetwork) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	return nil, nil
}

func TestSendRowToPeer_AttachesLocalFilePayload(t *testing.T) {
	database, cleanup := openDBWithLocalFileSchema(t, "sender-node")
	defer cleanup()

	key, expectedContent := seedLocalFileRow(t, database, "docs/file-a.txt")

	network := &localFileCaptureNetwork{}
	dsm := NewDataSyncManager(database, "sender-node")
	dsm.SetNetwork(network)

	if err := dsm.SendRowToPeer("peer-1", "docs", key); err != nil {
		t.Fatalf("send row failed: %v", err)
	}

	if network.targetNode != "peer-1" {
		t.Fatalf("unexpected target node: %s", network.targetNode)
	}
	if network.msg == nil {
		t.Fatal("expected message to be captured")
	}
	if network.msg.Type != MsgTypeRawData {
		t.Fatalf("unexpected message type: %s", network.msg.Type)
	}
	if len(network.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(network.messages))
	}
	if len(network.msg.LocalFiles) != 1 {
		t.Fatalf("expected 1 local file payload, got %d", len(network.msg.LocalFiles))
	}

	got := network.msg.LocalFiles[0]
	if got.Path != "docs/file-a.txt" {
		t.Fatalf("unexpected local file path: %s", got.Path)
	}
	if !bytes.Equal(got.Data, expectedContent) {
		t.Fatalf("unexpected local file data: got=%q want=%q", string(got.Data), string(expectedContent))
	}
	if got.Hash != sha256Hex(expectedContent) {
		t.Fatalf("unexpected local file hash: got=%s want=%s", got.Hash, sha256Hex(expectedContent))
	}
	if got.Size != int64(len(expectedContent)) {
		t.Fatalf("unexpected local file size: got=%d want=%d", got.Size, len(expectedContent))
	}
	if got.Chunked {
		t.Fatal("small file should not be chunked")
	}
}

func TestOnReceiveMergeWithFiles_PersistsLocalFile(t *testing.T) {
	database, cleanup := openDBWithLocalFileSchema(t, "receiver-node")
	defer cleanup()

	content := []byte("hello synced file")
	meta := crdt.FileMetadata{
		Path: "nested/synced.txt",
		Hash: sha256Hex(content),
		Size: int64(len(content)),
	}
	row := crdt.NewMapCRDT()
	if err := row.Apply(crdt.OpMapSet{
		Key:   "file",
		Value: crdt.NewLocalFileCRDT(meta, 100),
	}); err != nil {
		t.Fatalf("build row map failed: %v", err)
	}
	rawData, err := row.Bytes()
	if err != nil {
		t.Fatalf("encode row map failed: %v", err)
	}

	key, _ := uuid.NewV7()
	dsm := NewDataSyncManager(database, "receiver-node")
	if err := dsm.OnReceiveMergeWithFiles("docs", key.String(), rawData, database.Clock().Now(), []SyncedLocalFile{
		{
			Path: meta.Path,
			Hash: meta.Hash,
			Size: meta.Size,
			Data: content,
		},
	}); err != nil {
		t.Fatalf("receive merge with local files failed: %v", err)
	}

	fullPath := filepath.Join(database.FileStorageDir, meta.Path)
	got, err := os.ReadFile(fullPath)
	if err != nil {
		t.Fatalf("read synced file failed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("synced file content mismatch: got=%q want=%q", string(got), string(content))
	}
}

func TestOnReceiveMergeWithFiles_RollbacksMaterializedFilesOnMergeFailure(t *testing.T) {
	database, cleanup := openDBWithLocalFileSchema(t, "receiver-rollback-node")
	defer cleanup()

	existingRelPath := "nested/existing.txt"
	existingFullPath := filepath.Join(database.FileStorageDir, existingRelPath)
	if err := os.MkdirAll(filepath.Dir(existingFullPath), 0o755); err != nil {
		t.Fatalf("prepare existing file dir failed: %v", err)
	}
	originalContent := []byte("original-content")
	if err := os.WriteFile(existingFullPath, originalContent, 0o644); err != nil {
		t.Fatalf("prepare existing file failed: %v", err)
	}

	newRelPath := "nested/new.txt"
	newContent := []byte("new-content")
	overwriteContent := []byte("overwrite-content")

	dsm := NewDataSyncManager(database, "receiver-rollback-node")
	key, _ := uuid.NewV7()
	err := dsm.OnReceiveMergeWithFiles("docs", key.String(), []byte("invalid-map-payload"), database.Clock().Now(), []SyncedLocalFile{
		{
			Path: existingRelPath,
			Hash: sha256Hex(overwriteContent),
			Size: int64(len(overwriteContent)),
			Data: overwriteContent,
		},
		{
			Path: newRelPath,
			Hash: sha256Hex(newContent),
			Size: int64(len(newContent)),
			Data: newContent,
		},
	})
	if err == nil {
		t.Fatal("expected merge failure with invalid raw payload")
	}

	gotExisting, err := os.ReadFile(existingFullPath)
	if err != nil {
		t.Fatalf("read existing file after rollback failed: %v", err)
	}
	if !bytes.Equal(gotExisting, originalContent) {
		t.Fatalf("existing file should be restored on rollback: got=%q want=%q", string(gotExisting), string(originalContent))
	}

	if _, err := os.Stat(filepath.Join(database.FileStorageDir, newRelPath)); !os.IsNotExist(err) {
		t.Fatalf("new file should be removed on rollback, stat err=%v", err)
	}
}

func TestExportTableRawData_IncludesLocalFilePayload(t *testing.T) {
	database, cleanup := openDBWithLocalFileSchema(t, "export-node")
	defer cleanup()

	_, expectedContent := seedLocalFileRow(t, database, "docs/file-export.txt")

	dsm := NewDataSyncManager(database, "export-node")
	rows, err := dsm.ExportTableRawData("docs")
	if err != nil {
		t.Fatalf("export raw table data failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if len(rows[0].LocalFiles) != 1 {
		t.Fatalf("expected 1 local file payload, got %d", len(rows[0].LocalFiles))
	}

	got := rows[0].LocalFiles[0]
	if got.Path != "docs/file-export.txt" {
		t.Fatalf("unexpected local file path: %s", got.Path)
	}
	if !bytes.Equal(got.Data, expectedContent) {
		t.Fatalf("unexpected local file content: got=%q want=%q", string(got.Data), string(expectedContent))
	}
	if got.Chunked {
		t.Fatal("small file should not be chunked")
	}
}

func TestSendRowToPeer_ChunksLargeLocalFile(t *testing.T) {
	database, cleanup := openDBWithLocalFileSchema(t, "sender-large-node")
	defer cleanup()

	largeContent := bytes.Repeat([]byte("A"), int(defaultLocalFileInlineLimitBytes)+512*1024)
	key := seedLocalFileRowWithContent(t, database, "docs/large.bin", largeContent)

	network := &localFileCaptureNetwork{}
	dsm := NewDataSyncManager(database, "sender-large-node")
	dsm.SetNetwork(network)

	if err := dsm.SendRowToPeer("peer-large", "docs", key); err != nil {
		t.Fatalf("send row failed: %v", err)
	}

	if len(network.messages) < 2 {
		t.Fatalf("expected chunk messages + row message, got %d", len(network.messages))
	}
	if network.messages[len(network.messages)-1] == nil || network.messages[len(network.messages)-1].Type != MsgTypeRawData {
		t.Fatalf("expected last message to be raw row, got %+v", network.messages[len(network.messages)-1])
	}

	chunkMsgs := make([]*NetworkMessage, 0)
	for _, m := range network.messages {
		if m != nil && m.Type == MsgTypeLocalFileChunk {
			chunkMsgs = append(chunkMsgs, m)
		}
	}
	if len(chunkMsgs) == 0 {
		t.Fatal("expected local file chunk messages")
	}

	expectedTotal := calcChunkCount(int64(len(largeContent)), defaultLocalFileChunkSizeBytes)
	for i, m := range chunkMsgs {
		if m.ChunkIndex != i {
			t.Fatalf("unexpected chunk index at pos %d: got=%d", i, m.ChunkIndex)
		}
		if m.ChunkTotal != expectedTotal {
			t.Fatalf("unexpected chunk total: got=%d want=%d", m.ChunkTotal, expectedTotal)
		}
		if len(m.ChunkData) == 0 {
			t.Fatalf("chunk data should not be empty at idx=%d", i)
		}
		if len(m.ChunkData) > defaultLocalFileChunkSizeBytes {
			t.Fatalf("chunk size exceeds limit at idx=%d: %d", i, len(m.ChunkData))
		}
	}

	rowMsg := network.messages[len(network.messages)-1]
	if len(rowMsg.LocalFiles) != 1 {
		t.Fatalf("expected 1 local file metadata entry, got %d", len(rowMsg.LocalFiles))
	}
	fileMeta := rowMsg.LocalFiles[0]
	if !fileMeta.Chunked {
		t.Fatal("large file should be marked chunked")
	}
	if len(fileMeta.Data) != 0 {
		t.Fatalf("chunked file should not inline data, got %d bytes", len(fileMeta.Data))
	}
}

func TestOnReceiveMergeWithChunkedFiles_PersistsLargeFile(t *testing.T) {
	senderDB, senderCleanup := openDBWithLocalFileSchema(t, "sender-large-merge")
	defer senderCleanup()

	largeContent := bytes.Repeat([]byte("B"), int(defaultLocalFileInlineLimitBytes)+256*1024)
	key := seedLocalFileRowWithContent(t, senderDB, "docs/merge-large.bin", largeContent)

	capture := &localFileCaptureNetwork{}
	senderDSM := NewDataSyncManager(senderDB, "sender-large-merge")
	senderDSM.SetNetwork(capture)
	if err := senderDSM.SendRowToPeer("peer-merge", "docs", key); err != nil {
		t.Fatalf("send row failed: %v", err)
	}

	receiverDB, receiverCleanup := openDBWithLocalFileSchema(t, "receiver-large-merge")
	defer receiverCleanup()
	receiverDSM := NewDataSyncManager(receiverDB, "receiver-large-merge")
	receiverChunks := newLocalFileChunkReceiver()

	var rowMsg *NetworkMessage
	for _, m := range capture.messages {
		if m == nil {
			continue
		}
		switch m.Type {
		case MsgTypeLocalFileChunk:
			if err := receiverChunks.HandleChunk("sender-peer", receiverDB.FileStorageDir, *m); err != nil {
				t.Fatalf("apply chunk failed: %v", err)
			}
		case MsgTypeRawData:
			rowMsg = m
		}
	}
	if rowMsg == nil {
		t.Fatal("missing raw row message")
	}

	if err := receiverDSM.OnReceiveMergeWithFiles(rowMsg.Table, rowMsg.Key, rowMsg.RawData, rowMsg.Timestamp, rowMsg.LocalFiles); err != nil {
		t.Fatalf("merge with chunked files failed: %v", err)
	}

	fullPath := filepath.Join(receiverDB.FileStorageDir, "docs/merge-large.bin")
	got, err := os.ReadFile(fullPath)
	if err != nil {
		t.Fatalf("read merged large file failed: %v", err)
	}
	if !bytes.Equal(got, largeContent) {
		t.Fatalf("large file content mismatch: got=%d bytes want=%d", len(got), len(largeContent))
	}
}

func openDBWithLocalFileSchema(t *testing.T, dbID string) (*db.DB, func()) {
	t.Helper()

	root := t.TempDir()
	s, err := store.NewBadgerStore(filepath.Join(root, "db"))
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}

	database := mustOpenDB(t, s, dbID)
	fileDir := filepath.Join(root, "files")
	if err := os.MkdirAll(fileDir, 0o755); err != nil {
		t.Fatalf("create file storage dir failed: %v", err)
	}
	database.SetFileStorageDir(fileDir)

	if err := database.DefineTable(&meta.TableSchema{
		Name: "docs",
		Columns: []meta.ColumnSchema{
			{Name: "file", Type: meta.ColTypeString, CrdtType: meta.CrdtLocalFile},
		},
	}); err != nil {
		t.Fatalf("define table failed: %v", err)
	}

	cleanup := func() {
		_ = database.Close()
	}
	return database, cleanup
}

func seedLocalFileRow(t *testing.T, database *db.DB, relativePath string) (uuid.UUID, []byte) {
	t.Helper()

	key, _ := uuid.NewV7()
	content := []byte(fmt.Sprintf("payload-%s", relativePath))
	key = seedLocalFileRowWithContent(t, database, relativePath, content)
	return key, content
}

func seedLocalFileRowWithContent(t *testing.T, database *db.DB, relativePath string, content []byte) uuid.UUID {
	t.Helper()

	key, _ := uuid.NewV7()
	sourcePath := filepath.Join(t.TempDir(), "src.txt")
	if err := os.WriteFile(sourcePath, content, 0o644); err != nil {
		t.Fatalf("write source file failed: %v", err)
	}

	if err := database.Update(func(tx *db.Tx) error {
		return tx.Table("docs").Set(key, map[string]any{
			"file": db.FileImport{
				LocalPath:    sourcePath,
				RelativePath: relativePath,
			},
		})
	}); err != nil {
		t.Fatalf("seed local file row failed: %v", err)
	}

	return key
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:])
}

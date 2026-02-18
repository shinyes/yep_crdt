package sync

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestMultiEngine_OnPeerDisconnected_CleansChunkTransfers(t *testing.T) {
	root := t.TempDir()
	kv, err := store.NewBadgerStore(filepath.Join(root, "db"))
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	database := mustOpenDB(t, kv, "tenant-runtime-clean")
	defer database.Close()

	fileDir := filepath.Join(root, "files")
	if err := os.MkdirAll(fileDir, 0o755); err != nil {
		t.Fatalf("create file dir failed: %v", err)
	}
	database.SetFileStorageDir(fileDir)

	receiver := newLocalFileChunkReceiver()
	msgA := NetworkMessage{
		Type:       MsgTypeLocalFileChunk,
		FilePath:   "docs/a.bin",
		FileSize:   4,
		ChunkIndex: 0,
		ChunkTotal: 2,
		ChunkData:  []byte("aa"),
	}
	msgB := NetworkMessage{
		Type:       MsgTypeLocalFileChunk,
		FilePath:   "docs/b.bin",
		FileSize:   4,
		ChunkIndex: 0,
		ChunkTotal: 2,
		ChunkData:  []byte("bb"),
	}
	if err := receiver.HandleChunk("peer-a", fileDir, msgA); err != nil {
		t.Fatalf("prepare peer-a transfer failed: %v", err)
	}
	if err := receiver.HandleChunk("peer-b", fileDir, msgB); err != nil {
		t.Fatalf("prepare peer-b transfer failed: %v", err)
	}

	rt := &tenantRuntime{
		tenantID: "tenant-runtime-clean",
		db:       database,
		nodeMgr:  NewNodeManager(database, "local"),
		chunks:   receiver,
	}
	engine := &MultiEngine{
		tenants: map[string]*tenantRuntime{
			rt.tenantID: rt,
		},
	}

	engine.onPeerDisconnected("peer-a")

	remaining := snapshotChunkTransfers(receiver)
	if len(remaining) != 1 {
		t.Fatalf("expected 1 remaining transfer, got %d", len(remaining))
	}
	if remaining[0].peerID != "peer-b" {
		t.Fatalf("expected remaining transfer from peer-b, got %s", remaining[0].peerID)
	}
}

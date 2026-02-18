package sync

import (
	"os"
	"testing"
	"time"
)

func TestLocalFileChunkReceiver_CleanupExpired(t *testing.T) {
	baseDir := t.TempDir()
	receiver := newLocalFileChunkReceiver()

	now := time.Unix(1000, 0)
	receiver.transferTimeout = 10 * time.Second
	receiver.now = func() time.Time { return now }

	msg := NetworkMessage{
		Type:       MsgTypeLocalFileChunk,
		FilePath:   "docs/expired.bin",
		FileSize:   6,
		ChunkIndex: 0,
		ChunkTotal: 2,
		ChunkData:  []byte("abc"),
	}
	if err := receiver.HandleChunk("peer-expired", baseDir, msg); err != nil {
		t.Fatalf("handle first chunk failed: %v", err)
	}

	transfers := snapshotChunkTransfers(receiver)
	if len(transfers) != 1 {
		t.Fatalf("expected 1 transfer, got %d", len(transfers))
	}
	tempPath := transfers[0].tempPath

	now = now.Add(11 * time.Second)
	removed := receiver.CleanupExpired()
	if removed != 1 {
		t.Fatalf("expected 1 expired transfer cleaned, got %d", removed)
	}

	if got := len(snapshotChunkTransfers(receiver)); got != 0 {
		t.Fatalf("expected no active transfer, got %d", got)
	}
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Fatalf("expected temp chunk file removed, stat err=%v", err)
	}
}

func TestLocalFileChunkReceiver_CleanupPeer(t *testing.T) {
	baseDir := t.TempDir()
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
	if err := receiver.HandleChunk("peer-a", baseDir, msgA); err != nil {
		t.Fatalf("handle peer-a chunk failed: %v", err)
	}
	if err := receiver.HandleChunk("peer-b", baseDir, msgB); err != nil {
		t.Fatalf("handle peer-b chunk failed: %v", err)
	}

	transfers := snapshotChunkTransfers(receiver)
	if len(transfers) != 2 {
		t.Fatalf("expected 2 active transfers, got %d", len(transfers))
	}

	peerATemp := ""
	for _, transfer := range transfers {
		if transfer.peerID == "peer-a" {
			peerATemp = transfer.tempPath
			break
		}
	}
	if peerATemp == "" {
		t.Fatal("missing peer-a transfer temp path")
	}

	removed := receiver.CleanupPeer("peer-a")
	if removed != 1 {
		t.Fatalf("expected 1 peer transfer cleaned, got %d", removed)
	}

	remaining := snapshotChunkTransfers(receiver)
	if len(remaining) != 1 {
		t.Fatalf("expected 1 remaining transfer, got %d", len(remaining))
	}
	if remaining[0].peerID != "peer-b" {
		t.Fatalf("expected remaining transfer from peer-b, got %s", remaining[0].peerID)
	}
	if _, err := os.Stat(peerATemp); !os.IsNotExist(err) {
		t.Fatalf("expected peer-a temp chunk file removed, stat err=%v", err)
	}
}

func snapshotChunkTransfers(receiver *localFileChunkReceiver) []*localFileChunkTransfer {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()

	out := make([]*localFileChunkTransfer, 0, len(receiver.transfers))
	for _, transfer := range receiver.transfers {
		if transfer == nil {
			continue
		}
		copied := *transfer
		out = append(out, &copied)
	}
	return out
}

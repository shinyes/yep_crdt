package sync

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type localFileChunkReceiver struct {
	mu        sync.Mutex
	transfers map[string]*localFileChunkTransfer
}

type localFileChunkTransfer struct {
	key      string
	tempPath string
	fullPath string
	hash     string
	size     int64
	total    int
	next     int
	written  int64
}

func newLocalFileChunkReceiver() *localFileChunkReceiver {
	return &localFileChunkReceiver{
		transfers: make(map[string]*localFileChunkTransfer),
	}
}

func (r *localFileChunkReceiver) HandleChunk(peerID string, baseDir string, msg NetworkMessage) error {
	if r == nil {
		return fmt.Errorf("local file chunk receiver is nil")
	}
	if strings.TrimSpace(baseDir) == "" {
		return fmt.Errorf("FileStorageDir not configured, cannot receive LocalFileCRDT chunks")
	}
	if strings.TrimSpace(msg.FilePath) == "" {
		return fmt.Errorf("chunk file path is empty")
	}
	if msg.ChunkTotal <= 0 {
		return fmt.Errorf("invalid chunk total: %d", msg.ChunkTotal)
	}
	if msg.ChunkIndex < 0 || msg.ChunkIndex >= msg.ChunkTotal {
		return fmt.Errorf("invalid chunk index: %d/%d", msg.ChunkIndex, msg.ChunkTotal)
	}

	fullPath, err := resolveLocalFilePath(baseDir, msg.FilePath)
	if err != nil {
		return fmt.Errorf("resolve file path failed: %w", err)
	}

	key := buildLocalFileChunkTransferKey(peerID, msg)

	r.mu.Lock()
	defer r.mu.Unlock()

	transfer, exists := r.transfers[key]
	if msg.ChunkIndex == 0 {
		if exists {
			r.cleanupTransferLocked(transfer)
		}
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			return fmt.Errorf("create chunk directory failed: %w", err)
		}
		tempPath := fmt.Sprintf("%s.chunk.%d.part", fullPath, time.Now().UnixNano())
		transfer = &localFileChunkTransfer{
			key:      key,
			tempPath: tempPath,
			fullPath: fullPath,
			hash:     msg.FileHash,
			size:     msg.FileSize,
			total:    msg.ChunkTotal,
			next:     0,
			written:  0,
		}
		r.transfers[key] = transfer
		exists = true
	}

	if !exists || transfer == nil {
		return fmt.Errorf("missing chunk transfer state for %q", msg.FilePath)
	}

	if transfer.total != msg.ChunkTotal {
		r.cleanupTransferLocked(transfer)
		return fmt.Errorf("chunk total mismatch for %q: state=%d incoming=%d", msg.FilePath, transfer.total, msg.ChunkTotal)
	}
	if transfer.next != msg.ChunkIndex {
		r.cleanupTransferLocked(transfer)
		return fmt.Errorf("out-of-order chunk for %q: expect=%d got=%d", msg.FilePath, transfer.next, msg.ChunkIndex)
	}

	chunk := msg.ChunkData
	if chunk == nil {
		chunk = []byte{}
	}

	f, err := os.OpenFile(transfer.tempPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		r.cleanupTransferLocked(transfer)
		return fmt.Errorf("open temp chunk file failed: %w", err)
	}

	n, writeErr := f.Write(chunk)
	closeErr := f.Close()
	if writeErr != nil {
		r.cleanupTransferLocked(transfer)
		return fmt.Errorf("write chunk failed: %w", writeErr)
	}
	if closeErr != nil {
		r.cleanupTransferLocked(transfer)
		return fmt.Errorf("close temp chunk file failed: %w", closeErr)
	}

	transfer.written += int64(n)
	transfer.next++

	if transfer.next < transfer.total {
		return nil
	}

	// finalize
	if transfer.size >= 0 && transfer.written != transfer.size {
		r.cleanupTransferLocked(transfer)
		return fmt.Errorf("chunked file size mismatch for %q: expected=%d actual=%d", msg.FilePath, transfer.size, transfer.written)
	}
	if transfer.hash != "" {
		actualHash, err := hashFileSHA256(transfer.tempPath)
		if err != nil {
			r.cleanupTransferLocked(transfer)
			return fmt.Errorf("hash temp chunk file failed: %w", err)
		}
		if !strings.EqualFold(actualHash, transfer.hash) {
			r.cleanupTransferLocked(transfer)
			return fmt.Errorf("chunked file hash mismatch for %q: expected=%s actual=%s", msg.FilePath, transfer.hash, actualHash)
		}
	}

	if err := renameFileReplace(transfer.tempPath, transfer.fullPath); err != nil {
		r.cleanupTransferLocked(transfer)
		return fmt.Errorf("finalize chunked file failed: %w", err)
	}

	delete(r.transfers, transfer.key)
	return nil
}

func (r *localFileChunkReceiver) cleanupTransferLocked(transfer *localFileChunkTransfer) {
	if transfer == nil {
		return
	}
	delete(r.transfers, transfer.key)
	if transfer.tempPath != "" {
		_ = os.Remove(transfer.tempPath)
	}
}

func buildLocalFileChunkTransferKey(peerID string, msg NetworkMessage) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%d|%s",
		peerID,
		msg.Table,
		msg.Key,
		msg.FilePath,
		msg.FileHash,
		msg.FileSize,
		msg.RequestID,
	)
}

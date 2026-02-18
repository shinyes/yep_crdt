package sync

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/shinyes/yep_crdt/pkg/crdt"
)

const (
	// Keep small files inline for low-latency sync, large files go chunked.
	defaultLocalFileInlineLimitBytes = int64(8 * 1024 * 1024) // 8MB
	defaultLocalFileChunkSizeBytes   = 1 * 1024 * 1024        // 1MB
)

func (dsm *DataSyncManager) buildLocalFilePayloadsFromRaw(rawData []byte) ([]SyncedLocalFile, error) {
	metas, err := extractLocalFileMetadata(rawData)
	if err != nil {
		return nil, err
	}
	if len(metas) == 0 {
		return nil, nil
	}

	baseDir := strings.TrimSpace(dsm.db.FileStorageDir)
	if baseDir == "" {
		return nil, fmt.Errorf("FileStorageDir not configured, cannot sync LocalFileCRDT files")
	}

	files := make([]SyncedLocalFile, 0, len(metas))
	for _, meta := range metas {
		fullPath, err := resolveLocalFilePath(baseDir, meta.Path)
		if err != nil {
			return nil, fmt.Errorf("resolve local file path %q failed: %w", meta.Path, err)
		}

		info, err := os.Stat(fullPath)
		if err != nil {
			return nil, fmt.Errorf("stat local file %q failed: %w", fullPath, err)
		}
		if info.IsDir() {
			return nil, fmt.Errorf("local file %q is directory", fullPath)
		}
		if meta.Size >= 0 && info.Size() != meta.Size {
			return nil, fmt.Errorf("local file size mismatch for %q: metadata=%d actual=%d", meta.Path, meta.Size, info.Size())
		}

		payload := SyncedLocalFile{
			Path: meta.Path,
			Hash: meta.Hash,
			Size: meta.Size,
		}

		if shouldChunkLocalFile(meta.Size) {
			// Stream hash once for large files to avoid huge memory spikes.
			if meta.Hash != "" {
				actualHash, err := hashFileSHA256(fullPath)
				if err != nil {
					return nil, fmt.Errorf("hash local file %q failed: %w", meta.Path, err)
				}
				if !strings.EqualFold(actualHash, meta.Hash) {
					return nil, fmt.Errorf("local file hash mismatch for %q: metadata=%s actual=%s", meta.Path, meta.Hash, actualHash)
				}
			}
			payload.Chunked = true
			files = append(files, payload)
			continue
		}

		content, err := os.ReadFile(fullPath)
		if err != nil {
			return nil, fmt.Errorf("read local file %q failed: %w", fullPath, err)
		}
		if meta.Size >= 0 && int64(len(content)) != meta.Size {
			return nil, fmt.Errorf("local file size mismatch for %q: metadata=%d actual=%d", meta.Path, meta.Size, len(content))
		}
		if meta.Hash != "" {
			actualHash := hashSHA256(content)
			if !strings.EqualFold(actualHash, meta.Hash) {
				return nil, fmt.Errorf("local file hash mismatch for %q: metadata=%s actual=%s", meta.Path, meta.Hash, actualHash)
			}
		}

		payload.Data = content
		files = append(files, payload)
	}

	return files, nil
}

func (dsm *DataSyncManager) materializeSyncedLocalFiles(files []SyncedLocalFile) error {
	if len(files) == 0 {
		return nil
	}

	baseDir := strings.TrimSpace(dsm.db.FileStorageDir)
	if baseDir == "" {
		return fmt.Errorf("FileStorageDir not configured, cannot persist synced LocalFileCRDT files")
	}

	for _, f := range files {
		if strings.TrimSpace(f.Path) == "" {
			return fmt.Errorf("synced local file path is empty")
		}

		fullPath, err := resolveLocalFilePath(baseDir, f.Path)
		if err != nil {
			return fmt.Errorf("resolve synced file path %q failed: %w", f.Path, err)
		}

		// Chunked payloads carry metadata only. File bytes should already be streamed via
		// MsgTypeLocalFileChunk and available in storage by the time row merge is applied.
		if f.Chunked && len(f.Data) == 0 {
			if err := verifyExistingSyncedFile(fullPath, f.Size, f.Hash); err != nil {
				return fmt.Errorf("chunked file %q missing or invalid: %w", f.Path, err)
			}
			continue
		}

		data := f.Data
		if data == nil {
			data = []byte{}
		}

		if f.Size >= 0 && int64(len(data)) != f.Size {
			return fmt.Errorf("synced local file size mismatch for %q: metadata=%d actual=%d", f.Path, f.Size, len(data))
		}
		if f.Hash != "" {
			actualHash := hashSHA256(data)
			if !strings.EqualFold(actualHash, f.Hash) {
				return fmt.Errorf("synced local file hash mismatch for %q: metadata=%s actual=%s", f.Path, f.Hash, actualHash)
			}
		}

		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			return fmt.Errorf("create directory for %q failed: %w", f.Path, err)
		}

		tmpPath := fmt.Sprintf("%s.sync.%d.tmp", fullPath, time.Now().UnixNano())
		if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
			return fmt.Errorf("write temp synced file %q failed: %w", f.Path, err)
		}
		if err := renameFileReplace(tmpPath, fullPath); err != nil {
			return fmt.Errorf("rename synced file %q failed: %w", f.Path, err)
		}
	}

	return nil
}

func (dsm *DataSyncManager) sendChunkedLocalFilesToPeer(targetNodeID string, tableName string, key string, requestID string, files []SyncedLocalFile) error {
	if len(files) == 0 || !hasChunkedLocalFiles(files) {
		return nil
	}

	dsm.mu.RLock()
	network := dsm.network
	dsm.mu.RUnlock()
	if network == nil {
		return ErrNoNetwork
	}

	return dsm.emitChunkedLocalFiles(files, func(msg *NetworkMessage) error {
		msg.Table = tableName
		msg.Key = key
		msg.RequestID = requestID
		msg.GCFloor = dsm.db.GCFloor()
		msg.Timestamp = dsm.db.Clock().Now()
		return network.SendMessage(targetNodeID, msg)
	})
}

func (dsm *DataSyncManager) broadcastChunkedLocalFiles(tableName string, key string, files []SyncedLocalFile) error {
	if len(files) == 0 || !hasChunkedLocalFiles(files) {
		return nil
	}

	dsm.mu.RLock()
	network := dsm.network
	dsm.mu.RUnlock()
	if network == nil {
		return ErrNoNetwork
	}

	broadcaster, ok := network.(networkMessageBroadcaster)
	if !ok {
		return fmt.Errorf("network does not support LocalFileCRDT chunk broadcast")
	}

	return dsm.emitChunkedLocalFiles(files, func(msg *NetworkMessage) error {
		msg.Table = tableName
		msg.Key = key
		msg.GCFloor = dsm.db.GCFloor()
		msg.Timestamp = dsm.db.Clock().Now()
		_, err := broadcaster.Broadcast(msg)
		return err
	})
}

func (dsm *DataSyncManager) emitChunkedLocalFiles(files []SyncedLocalFile, emit func(msg *NetworkMessage) error) error {
	baseDir := strings.TrimSpace(dsm.db.FileStorageDir)
	if baseDir == "" {
		return fmt.Errorf("FileStorageDir not configured, cannot stream LocalFileCRDT chunks")
	}
	if emit == nil {
		return fmt.Errorf("chunk emitter is nil")
	}

	buf := make([]byte, defaultLocalFileChunkSizeBytes)
	for _, f := range files {
		if !f.Chunked {
			continue
		}

		fullPath, err := resolveLocalFilePath(baseDir, f.Path)
		if err != nil {
			return fmt.Errorf("resolve local file path %q failed: %w", f.Path, err)
		}

		info, err := os.Stat(fullPath)
		if err != nil {
			return fmt.Errorf("stat local file %q failed: %w", fullPath, err)
		}
		if info.IsDir() {
			return fmt.Errorf("local file %q is directory", fullPath)
		}
		if f.Size >= 0 && info.Size() != f.Size {
			return fmt.Errorf("local file size mismatch for %q: metadata=%d actual=%d", f.Path, f.Size, info.Size())
		}

		file, err := os.Open(fullPath)
		if err != nil {
			return fmt.Errorf("open local file %q failed: %w", fullPath, err)
		}

		totalChunks := calcChunkCount(info.Size(), defaultLocalFileChunkSizeBytes)
		chunkIndex := 0

		var hasher hash.Hash
		if f.Hash != "" {
			hasher = sha256.New()
		}

		for {
			n, readErr := file.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])

				if hasher != nil {
					if _, err := hasher.Write(chunk); err != nil {
						_ = file.Close()
						return fmt.Errorf("hash local file %q failed: %w", f.Path, err)
					}
				}

				if err := emit(&NetworkMessage{
					Type:       MsgTypeLocalFileChunk,
					FilePath:   f.Path,
					FileHash:   f.Hash,
					FileSize:   f.Size,
					ChunkIndex: chunkIndex,
					ChunkTotal: totalChunks,
					ChunkData:  chunk,
				}); err != nil {
					_ = file.Close()
					return fmt.Errorf("send local file chunk failed: path=%s, idx=%d/%d, err=%w", f.Path, chunkIndex+1, totalChunks, err)
				}

				chunkIndex++
			}

			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				_ = file.Close()
				return fmt.Errorf("read local file chunk failed: path=%s, idx=%d, err=%w", f.Path, chunkIndex, readErr)
			}
		}

		if err := file.Close(); err != nil {
			return fmt.Errorf("close local file failed: %w", err)
		}

		if chunkIndex != totalChunks {
			return fmt.Errorf("chunk count mismatch for %q: sent=%d expected=%d", f.Path, chunkIndex, totalChunks)
		}

		if hasher != nil {
			actualHash := fmt.Sprintf("%x", hasher.Sum(nil))
			if !strings.EqualFold(actualHash, f.Hash) {
				return fmt.Errorf("local file hash mismatch for %q: metadata=%s actual=%s", f.Path, f.Hash, actualHash)
			}
		}
	}

	return nil
}

func extractLocalFileMetadata(rawData []byte) ([]crdt.FileMetadata, error) {
	if len(rawData) == 0 {
		return nil, nil
	}

	rowMap, err := crdt.FromBytesMap(rawData)
	if err != nil {
		return nil, fmt.Errorf("decode row map failed while extracting local files: %w", err)
	}

	seen := make(map[string]crdt.FileMetadata)
	files := make([]crdt.FileMetadata, 0)
	for col, entry := range rowMap.Entries {
		if entry == nil || entry.Type != crdt.TypeLocalFile || len(entry.Data) == 0 {
			continue
		}

		lf, err := crdt.FromBytesLocalFile(entry.Data)
		if err != nil {
			return nil, fmt.Errorf("decode LocalFileCRDT for column %q failed: %w", col, err)
		}

		meta, ok := lf.Value().(crdt.FileMetadata)
		if !ok {
			return nil, fmt.Errorf("invalid LocalFileCRDT metadata type for column %q", col)
		}
		if strings.TrimSpace(meta.Path) == "" {
			return nil, fmt.Errorf("LocalFileCRDT metadata path is empty for column %q", col)
		}

		if existing, exists := seen[meta.Path]; exists {
			if existing.Hash != meta.Hash || existing.Size != meta.Size {
				return nil, fmt.Errorf("conflicting LocalFileCRDT metadata for path %q", meta.Path)
			}
			continue
		}

		seen[meta.Path] = meta
		files = append(files, meta)
	}

	return files, nil
}

func resolveLocalFilePath(baseDir string, relativePath string) (string, error) {
	if strings.TrimSpace(baseDir) == "" {
		return "", fmt.Errorf("base directory is empty")
	}

	clean := filepath.Clean(relativePath)
	if clean == "" || clean == "." {
		return "", fmt.Errorf("invalid relative path %q", relativePath)
	}
	if filepath.IsAbs(clean) {
		return "", fmt.Errorf("path must be relative: %q", relativePath)
	}
	parentPrefix := ".." + string(filepath.Separator)
	if clean == ".." || strings.HasPrefix(clean, parentPrefix) {
		return "", fmt.Errorf("path escapes storage directory: %q", relativePath)
	}

	return filepath.Join(baseDir, clean), nil
}

func hasChunkedLocalFiles(files []SyncedLocalFile) bool {
	for _, f := range files {
		if f.Chunked {
			return true
		}
	}
	return false
}

func shouldChunkLocalFile(size int64) bool {
	return size > defaultLocalFileInlineLimitBytes
}

func calcChunkCount(size int64, chunkSize int) int {
	if chunkSize <= 0 {
		chunkSize = defaultLocalFileChunkSizeBytes
	}
	if size <= 0 {
		return 1
	}
	return int((size + int64(chunkSize) - 1) / int64(chunkSize))
}

func verifyExistingSyncedFile(fullPath string, size int64, hashHex string) error {
	info, err := os.Stat(fullPath)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("target path is directory")
	}
	if size >= 0 && info.Size() != size {
		return fmt.Errorf("size mismatch: expected=%d actual=%d", size, info.Size())
	}
	if hashHex == "" {
		return nil
	}
	actualHash, err := hashFileSHA256(fullPath)
	if err != nil {
		return err
	}
	if !strings.EqualFold(actualHash, hashHex) {
		return fmt.Errorf("hash mismatch: expected=%s actual=%s", hashHex, actualHash)
	}
	return nil
}

func hashFileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func renameFileReplace(src string, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	_ = os.Remove(dst)
	if err := os.Rename(src, dst); err != nil {
		_ = os.Remove(src)
		return err
	}
	return nil
}

func hashSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:])
}

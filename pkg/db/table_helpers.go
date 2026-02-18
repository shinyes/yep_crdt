package db

import (
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// Internal Helpers

func (t *Table) getColCrdtType(col string) (string, error) {
	for _, c := range t.schema.Columns {
		if c.Name == col {
			if c.CrdtType == "" {
				return meta.CrdtLWW, nil // Default
			}
			return c.CrdtType, nil
		}
	}
	return "", fmt.Errorf("column not found: %s", col)
}

func (t *Table) getColumnSchema(col string) (meta.ColumnSchema, bool) {
	for _, c := range t.schema.Columns {
		if c.Name == col {
			return c, true
		}
	}
	return meta.ColumnSchema{}, false
}

// validateKey logic removed as we use uuid.UUID type. Check version if needed.

func (t *Table) loadRow(txn store.Tx, pk uuid.UUID) (*crdt.MapCRDT, map[string]any, error) {
	keyBytes := t.dataKey(pk)
	existingBytes, err := txn.Get(keyBytes)

	var currentMap *crdt.MapCRDT
	var oldBody map[string]any

	if err == store.ErrKeyNotFound {
		currentMap = crdt.NewMapCRDT()
	} else if err != nil {
		return nil, nil, err
	} else {
		currentMap, err = crdt.FromBytesMap(existingBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode existing data: %w", err)
		}
		oldBody = currentMap.Value().(map[string]any)
	}
	return currentMap, oldBody, nil
}

func (t *Table) saveRow(txn store.Tx, pk uuid.UUID, currentMap *crdt.MapCRDT, oldBody map[string]any) error {
	newBody := currentMap.Value().(map[string]any)

	oldIndexBody, err := t.decodeRowForIndex(oldBody)
	if err != nil {
		return err
	}
	newIndexBody, err := t.decodeRowForIndex(newBody)
	if err != nil {
		return err
	}

	if err := t.indexManager.UpdateIndexes(txn, t.schema.ID, t.schema.Indexes, pk[:], oldIndexBody, newIndexBody); err != nil {
		return err
	}
	finalBytes, err := currentMap.Bytes()
	if err != nil {
		return err
	}
	keyBytes := t.dataKey(pk)
	return txn.Set(keyBytes, finalBytes, 0)
}

func (t *Table) getRGA(m *crdt.MapCRDT, col string) (*crdt.RGA[[]byte], error) {
	// Map stores generic CRDTs. We need to cast it.
	rga, err := crdt.GetRGA[[]byte](m, col)
	if err != nil {
		return nil, err
	}
	if rga == nil {
		// Initialize
		newRGA := crdt.NewRGA[[]byte](t.db.clock)
		m.Apply(crdt.OpMapSet{Key: col, Value: newRGA})
		return newRGA, nil
	}
	return rga, nil
}

func toInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int64:
		return val, true
	case int32:
		return int64(val), true
	default:
		return 0, false
	}
}

func columnsFromMap(data map[string]any) []string {
	columns := make([]string, 0, len(data))
	for col := range data {
		if col == "" {
			continue
		}
		columns = append(columns, col)
	}
	return columns
}

func (t *Table) decodeRowForResult(raw map[string]any) (map[string]any, error) {
	return t.decodeRowBySchema(raw)
}

func (t *Table) decodeRowForIndex(raw map[string]any) (map[string]any, error) {
	return t.decodeRowBySchema(raw)
}

func (t *Table) decodeRowBySchema(raw map[string]any) (map[string]any, error) {
	if raw == nil {
		return nil, nil
	}
	decoded := make(map[string]any, len(raw))
	for col, val := range raw {
		schemaCol, ok := t.getColumnSchema(col)
		if !ok {
			decoded[col] = val
			continue
		}

		crdtType := schemaCol.CrdtType
		if crdtType == "" {
			crdtType = meta.CrdtLWW
		}
		if crdtType != meta.CrdtLWW {
			decoded[col] = val
			continue
		}

		typedValue, err := decodeLWWValueByColumnType(schemaCol.Type, val)
		if err != nil {
			return nil, fmt.Errorf("decode column %q failed: %w", col, err)
		}
		decoded[col] = typedValue
	}
	return decoded, nil
}

// Helper to encode value for LWW/RGA (bytes) or ORSet (string->bytes)
func encodeValue(v any) []byte {
	if b, ok := v.([]byte); ok {
		return b
	}
	if s, ok := v.(string); ok {
		return []byte(s)
	}
	return []byte(fmt.Sprintf("%v", v))
}

// extractInt64 从任意数值类型提取 int64 值
func extractInt64(v any) (int64, error) {
	switch val := v.(type) {
	case int:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int64:
		return val, nil
	case uint:
		if uint64(val) > math.MaxInt64 {
			return 0, fmt.Errorf("value %d overflows int64", val)
		}
		return int64(val), nil
	case uint8:
		return int64(val), nil
	case uint16:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case uint64:
		if val > math.MaxInt64 {
			return 0, fmt.Errorf("value %d overflows int64", val)
		}
		return int64(val), nil
	case float32:
		f := float64(val)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, fmt.Errorf("value %v is not finite", val)
		}
		if f < math.MinInt64 || f > math.MaxInt64 {
			return 0, fmt.Errorf("value %v overflows int64", val)
		}
		if math.Trunc(f) != f {
			return 0, fmt.Errorf("value %v is not an integer", val)
		}
		return int64(f), nil
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return 0, fmt.Errorf("value %v is not finite", val)
		}
		if val < math.MinInt64 || val > math.MaxInt64 {
			return 0, fmt.Errorf("value %v overflows int64", val)
		}
		if math.Trunc(val) != val {
			return 0, fmt.Errorf("value %v is not an integer", val)
		}
		return int64(val), nil
	default:
		return 0, fmt.Errorf("cannot extract int64 from %T", v)
	}
}

func copyFile(src, dst string) error {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()

	if _, err := io.Copy(destination, source); err != nil {
		return err
	}
	return nil
}

// createFileMetadata 根据本地文件路径创建 FileMetadata。
// 它会自动计算文件大小和 SHA256 哈希值。
func createFileMetadata(localPath string, relativePath string) (crdt.FileMetadata, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return crdt.FileMetadata{}, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return crdt.FileMetadata{}, err
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return crdt.FileMetadata{}, err
	}

	return crdt.FileMetadata{
		Path: relativePath,
		Size: info.Size(),
		Hash: fmt.Sprintf("%x", h.Sum(nil)),
	}, nil
}

type stagedFileImport struct {
	TempPath     string
	DestPath     string
	RelativePath string
}

type promotedFileImport struct {
	DestPath   string
	BackupPath string
}

func stageFileImport(srcPath string, destPath string, relativePath string) (stagedFileImport, error) {
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return stagedFileImport{}, err
	}

	tempPath := fmt.Sprintf("%s.import.%d.tmp", destPath, time.Now().UnixNano())
	if err := copyFile(srcPath, tempPath); err != nil {
		return stagedFileImport{}, err
	}

	return stagedFileImport{
		TempPath:     tempPath,
		DestPath:     destPath,
		RelativePath: relativePath,
	}, nil
}

func cleanupStagedFileImports(staged []stagedFileImport) {
	for _, s := range staged {
		if s.TempPath != "" {
			_ = os.Remove(s.TempPath)
		}
	}
}

func promoteStagedFileImports(staged []stagedFileImport) ([]promotedFileImport, error) {
	promoted := make([]promotedFileImport, 0, len(staged))

	for _, s := range staged {
		backupPath := ""
		if _, err := os.Stat(s.DestPath); err == nil {
			backupPath = fmt.Sprintf("%s.import.%d.bak", s.DestPath, time.Now().UnixNano())
			if err := os.Rename(s.DestPath, backupPath); err != nil {
				rollbackPromotedFileImports(promoted)
				return nil, err
			}
		} else if !os.IsNotExist(err) {
			rollbackPromotedFileImports(promoted)
			return nil, err
		}

		if err := os.Rename(s.TempPath, s.DestPath); err != nil {
			if backupPath != "" {
				_ = os.Rename(backupPath, s.DestPath)
			}
			rollbackPromotedFileImports(promoted)
			return nil, err
		}

		promoted = append(promoted, promotedFileImport{
			DestPath:   s.DestPath,
			BackupPath: backupPath,
		})
	}

	return promoted, nil
}

func rollbackPromotedFileImports(promoted []promotedFileImport) {
	for i := len(promoted) - 1; i >= 0; i-- {
		p := promoted[i]
		if p.DestPath != "" {
			_ = os.Remove(p.DestPath)
		}
		if p.BackupPath != "" {
			_ = os.Rename(p.BackupPath, p.DestPath)
		}
	}
}

func cleanupPromotedBackupFiles(promoted []promotedFileImport) {
	for _, p := range promoted {
		if p.BackupPath != "" {
			_ = os.Remove(p.BackupPath)
		}
	}
}

func (t *Table) onWriteCommitted(fn func()) {
	if fn == nil {
		return
	}
	if t.txCtx != nil {
		t.txCtx.onCommit(fn)
		return
	}
	fn()
}

func (t *Table) onWriteRollback(fn func()) {
	if fn == nil {
		return
	}
	if t.txCtx != nil {
		t.txCtx.onRollback(fn)
	}
}

func (t *Table) notifyChangeAfterWrite(key uuid.UUID, columns []string) {
	colsCopy := append([]string(nil), columns...)
	t.onWriteCommitted(func() {
		t.db.notifyChangeWithColumns(t.schema.Name, key, colsCopy)
	})
}

func validateUUIDv7(key uuid.UUID) error {
	if key.Version() != 7 {
		return fmt.Errorf("invalid key version: must be UUIDv7")
	}
	return nil
}

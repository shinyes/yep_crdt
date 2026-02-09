package crdt

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// FileMetadata 定义了文件的元数据。
type FileMetadata struct {
	Path string `json:"path"` // 相对于 BaseDir 的路径
	Hash string `json:"hash"` // 文件内容的哈希值 (如 SHA256)
	Size int64  `json:"size"` // 文件大小 (字节)
	// 可以根据需要添加其他字段，如 ContentType、ModTime 等
}

// LocalFileCRDT 是关联本地文件的 LWW Register CRDT。
// 它本身只存储文件的元数据 (FileMetadata)，并提供读取实际文件内容的方法。
type LocalFileCRDT struct {
	metadata  FileMetadata
	timestamp int64  // 最后写入的时间戳
	baseDir   string // 文件的存储根目录 (不序列化)
	mu        sync.RWMutex
}

// NewLocalFileCRDT 创建一个新的 LocalFileCRDT。
func NewLocalFileCRDT(metadata FileMetadata, timestamp int64) *LocalFileCRDT {
	return &LocalFileCRDT{
		metadata:  metadata,
		timestamp: timestamp,
	}
}

// SetBaseDir 设置用于查找文件的根目录。
// 必须在使用 ReadAll 或 ReadAt 之前调用。
func (lf *LocalFileCRDT) SetBaseDir(dir string) {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	lf.baseDir = dir
}

// Type 返回 CRDT 的类型。
func (lf *LocalFileCRDT) Type() Type {
	return TypeLocalFile
}

// Value 返回当前的 FileMetadata。
func (lf *LocalFileCRDT) Value() any {
	lf.mu.RLock()
	defer lf.mu.RUnlock()
	return lf.metadata
}

// OpLocalFileSet 是更新 LocalFileCRDT 的操作。
type OpLocalFileSet struct {
	Metadata  FileMetadata
	Timestamp int64
}

func (op OpLocalFileSet) Type() Type {
	return TypeLocalFile
}

// Apply 将操作应用于 LocalFileCRDT。
func (lf *LocalFileCRDT) Apply(op Op) error {
	setOp, ok := op.(OpLocalFileSet)
	if !ok {
		return ErrInvalidOp
	}

	lf.mu.Lock()
	defer lf.mu.Unlock()

	// LWW 逻辑：如果时间戳更大则更新
	if setOp.Timestamp > lf.timestamp {
		lf.metadata = setOp.Metadata
		lf.timestamp = setOp.Timestamp
	}
	return nil
}

// Merge 将另一个 CRDT 状态合并到此状态中。
func (lf *LocalFileCRDT) Merge(other CRDT) error {
	o, ok := other.(*LocalFileCRDT)
	if !ok {
		return fmt.Errorf("cannot merge %T into LocalFileCRDT", other)
	}

	lf.mu.Lock()
	defer lf.mu.Unlock()

	if o.timestamp > lf.timestamp {
		lf.metadata = o.metadata
		lf.timestamp = o.timestamp
	}
	return nil
}

// GC 执行垃圾回收 (对于 LWW Register 不需要做任何事)。
func (lf *LocalFileCRDT) GC(safeTimestamp int64) int {
	return 0
}

// Bytes 序列化 LocalFileCRDT。
// 格式：Timestamp (8 字节) + JSON(Metadata)
func (lf *LocalFileCRDT) Bytes() ([]byte, error) {
	lf.mu.RLock()
	metaBytes, err := json.Marshal(lf.metadata)
	lf.mu.RUnlock() // 尽早释放锁
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 8+len(metaBytes))
	binary.BigEndian.PutUint64(buf[0:8], uint64(lf.timestamp))
	copy(buf[8:], metaBytes)
	return buf, nil
}

// FromBytesLocalFile 反序列化 LocalFileCRDT。
func FromBytesLocalFile(data []byte) (*LocalFileCRDT, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("invalid data length for LocalFileCRDT")
	}
	ts := int64(binary.BigEndian.Uint64(data[0:8]))
	var meta FileMetadata
	if err := json.Unmarshal(data[8:], &meta); err != nil {
		return nil, err
	}

	return &LocalFileCRDT{
		metadata:  meta,
		timestamp: ts,
	}, nil
}

// ReadAll 读取整个关联文件的内容。
func (lf *LocalFileCRDT) ReadAll() ([]byte, error) {
	lf.mu.RLock()
	baseDir := lf.baseDir
	path := lf.metadata.Path
	lf.mu.RUnlock()

	if baseDir == "" {
		return nil, fmt.Errorf("baseDir not set for LocalFileCRDT")
	}
	fullPath := filepath.Join(baseDir, path)
	return os.ReadFile(fullPath)
}

// ReadAt 从指定位置读取指定长度的文件内容。
func (lf *LocalFileCRDT) ReadAt(offset int64, length int) ([]byte, error) {
	lf.mu.RLock()
	baseDir := lf.baseDir
	path := lf.metadata.Path
	lf.mu.RUnlock()

	if baseDir == "" {
		return nil, fmt.Errorf("baseDir not set for LocalFileCRDT")
	}
	fullPath := filepath.Join(baseDir, path)

	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, length)
	n, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	// 如果读取的数据少于请求的长度 (例如到达文件末尾)，
	// 我们返回实际读取的部分。
	return buf[:n], nil
}

// createFileMetadata 根据本地文件路径创建 FileMetadata (内部使用)。
// 它会自动计算文件大小和 SHA256 哈希值。
func createFileMetadata(localPath string, relativePath string) (FileMetadata, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return FileMetadata{}, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return FileMetadata{}, err
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return FileMetadata{}, err
	}

	return FileMetadata{
		Path: relativePath,
		Size: info.Size(),
		Hash: fmt.Sprintf("%x", h.Sum(nil)),
	}, nil
}

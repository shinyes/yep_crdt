package crdt

import "time"

// FileMetadata 保存本地文件的元数据。
type FileMetadata struct {
	Hash      string `json:"hash"`
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	MimeType  string `json:"mime_type"`
	CreatedAt int64  `json:"created_at"`
}

// LocalFile 本质上是一个持有 FileMetadata 的专用 LWW-Register。
type LocalFile struct {
	Register *LWWRegister
}

// NewLocalFile 创建一个新的 LocalFile 实例。
func NewLocalFile(meta FileMetadata) *LocalFile {
	return &LocalFile{
		Register: NewLWWRegister(meta, time.Now().UnixNano()),
	}
}

// Apply 应用操作到 LocalFile。
func (f *LocalFile) Apply(op Op) error {
	// LocalFile 的更新只是对元数据的 LWW 更新（例如，如果我们想 "重命名" 文件，
	// 我们技术上是在更新元数据。如果我们 "更改" 内容，我们会得到一个新的 Hash，因此是新的元数据）。
	return f.Register.Apply(op)
}

// Value 返回当前文件元数据。
func (f *LocalFile) Value() interface{} {
	return f.Register.Value()
}

// Type 返回 CRDT 类型。
func (f *LocalFile) Type() Type { return TypeLocalFile }

// NewLocalFileOp 是创建用于更新/设置文件的操作的助手函数。
func NewLocalFileOp(origin string, meta FileMetadata, ts int64) Op {
	return LWWOp{
		OriginID: origin,
		Value:    meta,
		Ts:       ts,
	}
}

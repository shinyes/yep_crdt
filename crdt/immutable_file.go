package crdt

import "time"

// FileMetadata 保存不可变文件的元数据。
type FileMetadata struct {
	Hash      string `json:"hash"`
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	MimeType  string `json:"mime_type"`
	CreatedAt int64  `json:"created_at"`
}

// ImmutableFile 本质上是一个持有 FileMetadata 的专用 LWW-Register。
type ImmutableFile struct {
	Register *LWWRegister
}

func NewImmutableFile(meta FileMetadata) *ImmutableFile {
	return &ImmutableFile{
		Register: NewLWWRegister(meta, time.Now().UnixNano()),
	}
}

func (f *ImmutableFile) Apply(op Op) error {
	// ImmutableFile 的更新只是对元数据的 LWW 更新（例如，如果我们想 "重命名" 文件，
	// 我们技术上是在更新元数据。如果我们 "更改" 内容，我们会得到一个新的 Hash，因此是新的元数据）。
	return f.Register.Apply(op)
}

func (f *ImmutableFile) Value() interface{} {
	return f.Register.Value()
}

func (f *ImmutableFile) Type() Type { return TypeImmutableFile }

// NewImmutableFileOp 是创建用于更新/设置文件的操作的助手函数。
func NewImmutableFileOp(origin string, meta FileMetadata, ts int64) Op {
	return LWWOp{
		OriginID: origin,
		Value:    meta,
		Ts:       ts,
	}
}

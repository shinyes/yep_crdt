package db

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/index"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// Table 代表逻辑表。
type Table struct {
	db           *DB
	schema       *meta.TableSchema
	indexManager *index.Manager
	tx           store.Tx
	txCtx        *Tx
}

// FileImport 用于在 Set 操作中指定要导入的外部文件。
// 数据库会自动计算哈希、大小，并将文件复制到 storageDir。
type FileImport struct {
	LocalPath    string // 本地源文件路径
	RelativePath string // 目标存储相对路径
}

// RawRow 表示一行的原始 CRDT 数据。
// 用于同步模块导出或导入表数据。
type RawRow struct {
	Key  uuid.UUID // 行的主键
	Data []byte    // MapCRDT 序列化后的原始字节
}

// TableGCResult 包含表级 GC 操作的结果统计。
type TableGCResult struct {
	RowsScanned       int     // 扫描的行数量
	TombstonesRemoved int     // 移除的墓碑数量
	Errors            []error // 遇到的错误
}

// RowDigest 行摘要，用于版本沟通。
type RowDigest struct {
	Key  uuid.UUID // 行主键
	Hash uint32    // 数据哈希（用于快速比较）
}

func (t *Table) inTx(update bool, fn func(store.Tx) error) error {
	if t.tx != nil {
		return fn(t.tx)
	}
	if update {
		return t.db.store.Update(fn)
	}
	return t.db.store.View(fn)
}

func (t *Table) tablePrefix() []byte {
	return []byte(fmt.Sprintf("/d/%s/", t.schema.Name))
}

func (t *Table) dataKey(u uuid.UUID) []byte {
	// 格式：/d/<table>/<16-byte-uuid>
	prefix := t.tablePrefix()
	return append(prefix, u[:]...)
}

package db

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// Set 插入或更新一行。
// data 是 column -> value 的映射 (原始 Go 类型，还不是 CRDT)。
// 为了简化 V1，我们假设数据映射到 LWWRegister 或者我们需要根据模式处理类型。
// 为了使其完全支持 CRDT 泛型，输入可能应该是 CRDT Ops 或就绪的 CRDT。
// 但检查 Fluent API 设计：Set(key, Data{...})。
// 所以我们需要将 Data 转换为 CRDT。
func (t *Table) Set(key uuid.UUID, data map[string]any) error {

	err := t.inTx(true, func(txn store.Tx) error {
		// 1. 加载现有 CRDT (用于索引比较)
		keyBytes := t.dataKey(key)
		existingBytes, err := txn.Get(keyBytes)

		var currentMap *crdt.MapCRDT
		var oldBody map[string]any

		if err == store.ErrKeyNotFound {
			currentMap = crdt.NewMapCRDT()
		} else if err != nil {
			return err
		} else {
			currentMap, err = crdt.FromBytesMap(existingBytes)
			if err != nil {
				return fmt.Errorf("failed to decode existing data: %w", err)
			}
			oldBody = currentMap.Value().(map[string]any)
		}

		// 2. 应用更改
		// 我们遍历输入数据并更新 MapCRDT。
		// 对于 MVP，除非在模式中指定，否则我们假设所有输入都是 LWWRegister。
		// 我们需要模式来知道类型。

		for col, val := range data {
			// 查找列模式，根据 schema 决定 CRDT/列类型
			colCrdtType := meta.CrdtLWW
			colDataType := meta.ColTypeString
			if schemaCol, ok := t.getColumnSchema(col); ok {
				if schemaCol.CrdtType != "" {
					colCrdtType = schemaCol.CrdtType
				}
				if schemaCol.Type != "" {
					colDataType = schemaCol.Type
				}
			}

			// 检查是否已经是 CRDT
			if c, ok := val.(crdt.CRDT); ok {
				op := crdt.OpMapSet{
					Key:   col,
					Value: c,
				}
				if err := currentMap.Apply(op); err != nil {
					return err
				}
				continue
			}

			// 检查是否是 FileImport (自动导入)
			if fImport, ok := val.(FileImport); ok {
				if t.db.FileStorageDir == "" {
					return fmt.Errorf("FileStorageDir not configured, cannot import file")
				}
				relativePath, err := normalizeImportRelativePath(fImport.RelativePath)
				if err != nil {
					return fmt.Errorf("invalid file import relative path: %w", err)
				}

				// 1. 确保源文件存在
				srcInfo, err := os.Stat(fImport.LocalPath)
				if err != nil {
					return fmt.Errorf("source file error: %w", err)
				}
				if srcInfo.IsDir() {
					return fmt.Errorf("source path is a directory")
				}

				// 2. 准备目标路径
				destPath := filepath.Join(t.db.FileStorageDir, relativePath)
				// 确保父目录存在
				if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
					return fmt.Errorf("failed to create destination dir: %w", err)
				}

				// 3. 复制文件
				if err := copyFile(fImport.LocalPath, destPath); err != nil {
					return fmt.Errorf("failed to copy file: %w", err)
				}

				// 4. 创建元数据 (从目标文件读取，确保一致性)
				meta, err := createFileMetadata(destPath, relativePath)
				if err != nil {
					return fmt.Errorf("failed to create file metadata: %w", err)
				}

				lf := crdt.NewLocalFileCRDT(meta, t.db.clock.Now())
				op := crdt.OpMapSet{
					Key:   col,
					Value: lf,
				}
				if err := currentMap.Apply(op); err != nil {
					return err
				}
				continue
			}

			// 根据 schema 定义的 CRDT 类型创建对应的 CRDT
			ts := t.db.clock.Now()
			var crdtVal crdt.CRDT

			switch colCrdtType {
			case meta.CrdtCounter:
				// PN-Counter
				crdtVal = crdt.NewPNCounter(t.db.NodeID)
				intVal, err := extractInt64(val)
				if err != nil {
					return fmt.Errorf("failed to extract int64 value for counter: %w", err)
				}
				if err := crdtVal.Apply(crdt.OpPNCounterInc{Val: intVal}); err != nil {
					return fmt.Errorf("failed to initialize counter: %w", err)
				}
			case meta.CrdtORSet:
				// OR-Set
				crdtVal = crdt.NewORSet[string]()
				// 添加初始元素
				if strVal, ok := val.(string); ok {
					crdtVal.Apply(crdt.OpORSetAdd[string]{Element: strVal})
				}
			case meta.CrdtRGA:
				// RGA
				crdtVal = crdt.NewRGA[[]byte](t.db.clock)
				// 添加初始元素
				if strVal, ok := val.(string); ok {
					crdtVal.Apply(crdt.OpRGAInsert[[]byte]{AnchorID: crdtVal.(*crdt.RGA[[]byte]).Head, Value: []byte(strVal)})
				}
			case meta.CrdtLocalFile:
				return fmt.Errorf("LocalFile requires FileImport type, got %T", val)
			default:
				// LWW (默认)
				encoded, err := encodeLWWValueByColumnType(colDataType, val)
				if err != nil {
					return fmt.Errorf("failed to encode column %q: %w", col, err)
				}
				crdtVal = crdt.NewLWWRegister(encoded, ts)
			}

			// 应用到 Map
			op := crdt.OpMapSet{
				Key:   col,
				Value: crdtVal,
			}
			if err := currentMap.Apply(op); err != nil {
				return err
			}
		}

		// 3. 保存并更新索引
		return t.saveRow(txn, key, currentMap, oldBody)
	})

	// 写入成功后触发变更回调（用于自动广播）
	if err == nil {
		t.db.notifyChangeWithColumns(t.schema.Name, key, columnsFromMap(data))
	}
	return err
}

func normalizeImportRelativePath(rawPath string) (string, error) {
	trimmed := strings.TrimSpace(rawPath)
	if trimmed == "" {
		return "", fmt.Errorf("path cannot be empty")
	}
	if filepath.IsAbs(trimmed) {
		return "", fmt.Errorf("path must be relative")
	}

	normalized := strings.ReplaceAll(trimmed, `\`, `/`)
	relativePath := path.Clean(normalized)
	if relativePath == "" || relativePath == "." {
		return "", fmt.Errorf("path cannot be empty")
	}
	if strings.HasPrefix(relativePath, "/") {
		return "", fmt.Errorf("path must be relative")
	}
	if strings.Contains(relativePath, ":") {
		return "", fmt.Errorf("path contains invalid ':'")
	}
	if relativePath == ".." || strings.HasPrefix(relativePath, "../") {
		return "", fmt.Errorf("path escapes storage directory")
	}
	return relativePath, nil
}

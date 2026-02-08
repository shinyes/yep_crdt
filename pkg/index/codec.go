package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// 索引键的前缀
const IndexPrefix byte = 'i'

// EncodeKey 生成索引条目的 BadgerDB 键。
// 格式：前缀(1) + TableID(4) + IndexID(4) + 值... + PK
func EncodeKey(tableID uint32, indexID uint32, values []any, pk []byte) ([]byte, error) {
	buf := new(bytes.Buffer)

	// 1. 前缀
	buf.WriteByte(IndexPrefix)

	// 2. TableID
	if err := binary.Write(buf, binary.BigEndian, tableID); err != nil {
		return nil, err
	}

	// 3. IndexID
	if err := binary.Write(buf, binary.BigEndian, indexID); err != nil {
		return nil, err
	}

	// 4. 值 (保持顺序)
	for _, v := range values {
		if err := encodeValue(buf, v); err != nil {
			return nil, err
		}
	}

	// 5. PK (在末尾以保证唯一性和查找)
	buf.Write(pk)

	return buf.Bytes(), nil
}

func encodeValue(buf *bytes.Buffer, v any) error {
	switch val := v.(type) {
	case int:
		// 转换为 int64 以保持一致性
		return binary.Write(buf, binary.BigEndian, int64(val))
	case int64:
		// 在排序时如何处理负数？
		// 为了简单起见，假设为正数或使用特定编码 (如反转符号位)
		// 标准 BigEndian 错误地排序负数 (补码)。
		// 正确的方法：反转符号位。
		// uint64(val) ^ 0x8000000000000000
		encoded := uint64(val) ^ 0x8000000000000000
		return binary.Write(buf, binary.BigEndian, encoded)
	case string:
		buf.WriteString(val)
		buf.WriteByte(0x00) // 空终止符
	case []byte:
		buf.Write(val)
		buf.WriteByte(0x00)
	default:
		return fmt.Errorf("unsupported index type: %T", v)
	}
	return nil
}

// EncodePrefix 生成用于迭代的前缀 (例如 "WHERE age > 20")
// 格式：前缀(1) + TableID(4) + IndexID(4) + [部分值]
func EncodePrefix(tableID uint32, indexID uint32, values []any) ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(IndexPrefix)
	binary.Write(buf, binary.BigEndian, tableID)
	binary.Write(buf, binary.BigEndian, indexID)
	for _, v := range values {
		if err := encodeValue(buf, v); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

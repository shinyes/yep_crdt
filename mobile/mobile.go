package mobile

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/shinyes/yep_crdt/crdt"
	"github.com/shinyes/yep_crdt/manager"
)

// MobileManager 包装 Manager，对外提供 gomobile 兼容的 API。
// gomobile 不支持 interface{} 返回值和复杂的 map，因此需要此封装。
type MobileManager struct {
	m *manager.Manager
}

// NewMobileManager 创建一个新的管理器实例。
func NewMobileManager(dbPath string, blobPath string) (*MobileManager, error) {
	m, err := manager.NewManager(dbPath, blobPath)
	if err != nil {
		return nil, err
	}
	return &MobileManager{m: m}, nil
}

// Close 关闭数据库连接。
func (mm *MobileManager) Close() error {
	return mm.m.Close()
}

// CreateMapRoot 创建一个新的 Map 类型的根节点。
func (mm *MobileManager) CreateMapRoot(id string) error {
	_, err := mm.m.CreateRoot(id, crdt.TypeMap)
	return err
}

// MobileQuery 是 Query 的封装，支持 gomobile。
type MobileQuery struct {
	mm     *MobileManager
	rootID string
	paths  []string
}

// From 开始构建查询。
func (mm *MobileManager) From(rootID string) *MobileQuery {
	return &MobileQuery{mm: mm, rootID: rootID}
}

// Select 添加要查询的字段。支持多个字段，用逗号分隔（gomobile 不支持可变参数）。
func (mq *MobileQuery) Select(commaSeparatedPaths string) *MobileQuery {
	if commaSeparatedPaths == "" {
		return mq
	}
	parts := strings.Split(commaSeparatedPaths, ",")
	for _, p := range parts {
		mq.paths = append(mq.paths, strings.TrimSpace(p))
	}
	return mq
}

// GetAsJSON 执行查询并将结果作为 JSON 字符串返回。
// 这是在移动端处理复杂结构数据的推荐方式。
func (mq *MobileQuery) GetAsJSON() (string, error) {
	q := mq.mm.m.From(mq.rootID)
	if len(mq.paths) > 0 {
		q.Select(mq.paths...)
	}

	val, err := q.Get()
	if err != nil {
		return "", err
	}

	// 如果结果为 nil，返回 null
	if val == nil {
		return "null", nil
	}

	bytes, err := json.Marshal(val)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// SetString 设置字符串值。
func (mq *MobileQuery) SetString(path string, val string) error {
	return mq.mm.m.From(mq.rootID).Update().Set(path, val).Commit()
}

// SetInt 设置整数值 (int64)。
func (mq *MobileQuery) SetInt(path string, val int64) error {
	return mq.mm.m.From(mq.rootID).Update().Set(path, val).Commit()
}

// SetBool 设置布尔值。
func (mq *MobileQuery) SetBool(path string, val bool) error {
	return mq.mm.m.From(mq.rootID).Update().Set(path, val).Commit()
}

// SetFloat 设置浮点值。
func (mq *MobileQuery) SetFloat(path string, val float64) error {
	return mq.mm.m.From(mq.rootID).Update().Set(path, val).Commit()
}

// Inc 增加计数器。
func (mq *MobileQuery) Inc(path string, amount int64) error {
	return mq.mm.m.From(mq.rootID).Update().Inc(path, amount).Commit()
}

// Delete 删除指定的键。
func (mq *MobileQuery) Delete(path string) error {
	return mq.mm.m.From(mq.rootID).Update().Delete(path).Commit()
}

// Dec 减少计数器（仅支持 PNCounter）。
func (mq *MobileQuery) Dec(path string, amount int64) error {
	return mq.mm.m.From(mq.rootID).Update().Inc(path, -amount).Commit()
}

// GetInt 获取指定路径的整数值。如果路径不存在或类型不匹配，返回错误。
func (mq *MobileQuery) GetInt(path string) (int64, error) {
	q := mq.mm.m.From(mq.rootID)
	if path != "" {
		q.Select(path)
	}

	val, err := q.Get()
	if err != nil {
		return 0, err
	}

	switch v := val.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("值类型 %T 无法转换为 int64", val)
	}
}

// GetString 获取指定路径的字符串值。如果路径不存在或类型不匹配，返回错误。
func (mq *MobileQuery) GetString(path string) (string, error) {
	q := mq.mm.m.From(mq.rootID)
	if path != "" {
		q.Select(path)
	}

	val, err := q.Get()
	if err != nil {
		return "", err
	}

	switch v := val.(type) {
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("值类型 %T 无法转换为 string", val)
	}
}

// CreateCounterRoot 创建一个新的 Counter 类型的根节点（PNCounter，支持增减）。
func (mm *MobileManager) CreateCounterRoot(id string) error {
	_, err := mm.m.CreateRoot(id, crdt.TypePNCounter)
	return err
}

// CreateGCounterRoot 创建一个新的 GCounter 类型的根节点（仅支持增加）。
func (mm *MobileManager) CreateGCounterRoot(id string) error {
	_, err := mm.m.CreateRoot(id, crdt.TypeCounter)
	return err
}

// AddToSet 向指定路径的集合添加元素。
func (mq *MobileQuery) AddToSet(path string, value string) error {
	return mq.mm.m.From(mq.rootID).Update().AddToSet(path, value).Commit()
}

// RemoveFromSet 从指定路径的集合移除元素。
func (mq *MobileQuery) RemoveFromSet(path string, value string) error {
	return mq.mm.m.From(mq.rootID).Update().RemoveFromSet(path, value).Commit()
}

// ========== 序列操作 ==========

// InsertAt 在序列的指定位置插入元素。
// prevID: 前一个元素的 ID，使用 "start" 表示在开头插入。
func (mq *MobileQuery) InsertAt(path, prevID, value string) error {
	return mq.mm.m.From(mq.rootID).Update().InsertAt(path, prevID, value).Commit()
}

// Append 在序列末尾追加元素。
func (mq *MobileQuery) Append(path, value string) error {
	return mq.mm.m.From(mq.rootID).Update().Append(path, value).Commit()
}

// RemoveAt 从序列中移除指定 ID 的元素。
func (mq *MobileQuery) RemoveAt(path, elemID string) error {
	return mq.mm.m.From(mq.rootID).Update().RemoveAt(path, elemID).Commit()
}

// GetSequenceAsJSON 获取序列内容（带元素 ID）作为 JSON 字符串。
func (mq *MobileQuery) GetSequenceAsJSON(path string) (string, error) {
	elems, err := mq.mm.m.From(mq.rootID).GetSequence(path)
	if err != nil {
		return "", err
	}
	if elems == nil {
		return "[]", nil
	}
	bytes, err := json.Marshal(elems)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// ========== 根节点管理 ==========

// NewMobileManagerWithNodeID 创建带有自定义节点 ID 的 Manager。
func NewMobileManagerWithNodeID(dbPath, blobPath, nodeID string) (*MobileManager, error) {
	m, err := manager.NewManagerWithNodeID(dbPath, blobPath, nodeID)
	if err != nil {
		return nil, err
	}
	return &MobileManager{m: m}, nil
}

// NodeID 返回当前节点 ID。
func (mm *MobileManager) NodeID() string {
	return mm.m.NodeID()
}

// ListRootsAsJSON 列出所有根节点的元数据作为 JSON 字符串。
func (mm *MobileManager) ListRootsAsJSON() (string, error) {
	roots, err := mm.m.ListRoots()
	if err != nil {
		return "", err
	}
	bytes, err := json.Marshal(roots)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// DeleteRoot 删除指定的根节点。
func (mm *MobileManager) DeleteRoot(id string) error {
	return mm.m.DeleteRoot(id)
}

// Exists 检查根节点是否存在。
func (mm *MobileManager) Exists(id string) bool {
	return mm.m.Exists(id)
}

// CreateSequenceRoot 创建一个新的 Sequence (RGA) 类型的根节点。
func (mm *MobileManager) CreateSequenceRoot(id string) error {
	_, err := mm.m.CreateRoot(id, crdt.TypeSequence)
	return err
}

// CreateSetRoot 创建一个新的 Set (ORSet) 类型的根节点。
func (mm *MobileManager) CreateSetRoot(id string) error {
	_, err := mm.m.CreateRoot(id, crdt.TypeSet)
	return err
}

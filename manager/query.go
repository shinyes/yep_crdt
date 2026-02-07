package manager

import (
	"fmt"
	"strings"

	"github.com/shinyes/yep_crdt/crdt"
)

// Query 是操作的流畅构建器 (Fluent Builder)。
type Query struct {
	m         *Manager
	rootID    string
	err       error
	selection []string
}

func (m *Manager) From(rootID string) *Query {
	return &Query{m: m, rootID: rootID}
}

// Select 指定要检索的字段。支持点号分隔的嵌套键。
func (q *Query) Select(paths ...string) *Query {
	q.selection = append(q.selection, paths...)
	return q
}

// Get 执行查询并返回结果。
func (q *Query) Get() (interface{}, error) {
	if q.err != nil {
		return nil, q.err
	}

	root, err := q.m.GetRoot(q.rootID)
	if err != nil {
		return nil, err
	}

	// 如果没有选择字段，返回整个根节点的值
	if len(q.selection) == 0 {
		return root.Value(), nil
	}

	// 如果只选择了一个字段
	if len(q.selection) == 1 {
		val, err := q.getValueAtPath(root, q.selection[0])
		if err != nil {
			return nil, err
		}
		return val, nil
	}

	// 如果选择了多个字段，返回 map
	result := make(map[string]interface{})
	for _, path := range q.selection {
		val, err := q.getValueAtPath(root, path)
		if err != nil {
			return nil, err
		}
		result[path] = val
	}
	return result, nil
}

func (q *Query) getValueAtPath(root crdt.CRDT, path string) (interface{}, error) {
	keys := strings.Split(path, ".")
	current := root

	for _, key := range keys {
		mapC, ok := current.(*crdt.MapCRDT)
		if !ok {
			return nil, fmt.Errorf("路径 %s 中的节点不是 Map", path)
		}
		child := mapC.GetChild(key)
		if child == nil {
			return nil, nil // 或者返回错误，视需求而定。这里假设不存在返回 nil
		}
		current = child
	}
	return current.Value(), nil
}

// Update 准备一个更新。
type UpdateQuery struct {
	*Query
}

func (q *Query) Update() *UpdateQuery {
	return &UpdateQuery{q}
}

// Set 更新或插入指定路径的值。
func (uq *UpdateQuery) Set(path string, val interface{}) *UpdateQuery {
	if uq.err != nil {
		return uq
	}
	root, err := uq.m.GetRoot(uq.rootID)
	if err != nil {
		uq.err = err
		return uq
	}

	parent, key, err := uq.ensurePath(root, path)
	if err != nil {
		uq.err = err
		return uq
	}

	// 确定值类型
	var initType crdt.Type
	switch val.(type) {
	case int, int64, float64:
		initType = crdt.TypeRegister // 默认使用 Register 存储数字
	case string, bool:
		initType = crdt.TypeRegister
	default:
		// 暂时默认为 Register
		initType = crdt.TypeRegister
	}

	// 检查是否存在
	child := parent.GetChild(key)
	if child == nil {
		// 初始化
		initOp := crdt.MapOp{
			OriginID: uq.m.NodeID(),
			Key:      key,
			IsInit:   true,
			InitType: initType,
			Ts:       uq.m.NextTimestamp(),
		}
		if err := parent.Apply(initOp); err != nil {
			uq.err = err
			return uq
		}

		// 持久化 init op
		keys := strings.Split(path, ".")
		if len(keys) > 0 {
			parentKeys := keys[:len(keys)-1]
			if err := uq.saveWrappedOp(parentKeys, initOp); err != nil {
				uq.err = err
				return uq
			}
		}
	}

	// 应用值
	// 目前只处理 Register 的 Set
	// 如果是其他类型（如 Counter），Set 语义可能不同，或者不支持
	// 这里简化处理，假设都是 LWWRegister

	op := crdt.LWWOp{
		OriginID: uq.m.NodeID(),
		Value:    val,
		Ts:       uq.m.NextTimestamp(),
	}

	mapOp := crdt.MapOp{
		OriginID: uq.m.NodeID(),
		Key:      key,
		ChildOp:  op,
		Ts:       uq.m.NextTimestamp(),
	}

	if err := parent.Apply(mapOp); err != nil {
		uq.err = err
	}

	// 持久化 update op
	keys := strings.Split(path, ".")
	if len(keys) > 0 {
		parentKeys := keys[:len(keys)-1]
		if err := uq.saveWrappedOp(parentKeys, mapOp); err != nil {
			uq.err = err
		}
	}

	return uq
}

// Inc 增加指定路径下的 Counter。
func (uq *UpdateQuery) Inc(path string, amount int64) *UpdateQuery {
	if uq.err != nil {
		return uq
	}
	root, err := uq.m.GetRoot(uq.rootID)
	if err != nil {
		uq.err = err
		return uq
	}

	parent, key, err := uq.ensurePath(root, path)
	if err != nil {
		uq.err = err
		return uq
	}

	child := parent.GetChild(key)
	if child == nil {
		// 初始化 Counter（现在所有计数器都是 PNCounter，支持增减）
		initOp := crdt.MapOp{
			OriginID: uq.m.NodeID(),
			Key:      key,
			IsInit:   true,
			InitType: crdt.TypeCounter,
			Ts:       uq.m.NextTimestamp(),
		}
		if err := parent.Apply(initOp); err != nil {
			uq.err = err
			return uq
		}

		// 持久化 init op
		keys := strings.Split(path, ".")
		if len(keys) > 0 {
			parentKeys := keys[:len(keys)-1]
			if err := uq.saveWrappedOp(parentKeys, initOp); err != nil {
				uq.err = err
				return uq
			}
		}

		// 重新获取
		child = parent.GetChild(key)
	}

	var op crdt.Op
	switch child.Type() {
	case crdt.TypeCounter:
		op = crdt.PNCounterOp{
			OriginID: uq.m.NodeID(),
			Amount:   amount,
			Ts:       uq.m.NextTimestamp(),
		}
	default:
		uq.err = fmt.Errorf("路径 %s 处的节点不是 Counter", path)
		return uq
	}

	mapOp := crdt.MapOp{
		OriginID: uq.m.NodeID(),
		Key:      key,
		ChildOp:  op,
		Ts:       uq.m.NextTimestamp(),
	}

	if err := parent.Apply(mapOp); err != nil {
		uq.err = err
	}

	// 持久化 update op
	keys := strings.Split(path, ".")
	if len(keys) > 0 {
		parentKeys := keys[:len(keys)-1]
		if err := uq.saveWrappedOp(parentKeys, mapOp); err != nil {
			uq.err = err
		}
	}

	return uq
}

// Delete 删除指定路径下的键。
func (uq *UpdateQuery) Delete(path string) *UpdateQuery {
	if uq.err != nil {
		return uq
	}
	root, err := uq.m.GetRoot(uq.rootID)
	if err != nil {
		uq.err = err
		return uq
	}

	// 对于 Delete，我们只需要找到父节点
	keys := strings.Split(path, ".")
	if len(keys) == 0 {
		return uq
	}
	parentKey := keys[len(keys)-1]
	parentPath := strings.Join(keys[:len(keys)-1], ".")

	var parent *crdt.MapCRDT
	if parentPath == "" {
		// 根节点的直接子节点
		var ok bool
		parent, ok = root.(*crdt.MapCRDT)
		if !ok {
			uq.err = fmt.Errorf("根节点不是 Map")
			return uq
		}
	} else {
		// 查找父节点，但不创建
		p, err := uq.findMap(root, parentPath)
		if err != nil {
			uq.err = err
			return uq
		}
		if p == nil {
			// 父路径不存在，无需删除
			return uq
		}
		parent = p
	}

	op := crdt.MapOp{
		OriginID: uq.m.NodeID(),
		Key:      parentKey,
		IsRemove: true,
		Ts:       uq.m.NextTimestamp(),
	}

	if err := parent.Apply(op); err != nil {
		uq.err = err
	}

	// 持久化 delete op
	var parentKeys []string
	if parentPath != "" {
		parentKeys = strings.Split(parentPath, ".")
	}
	if err := uq.saveWrappedOp(parentKeys, op); err != nil {
		uq.err = err
	}

	return uq
}

// AddToSet 向指定路径的 Set 添加元素。
func (uq *UpdateQuery) AddToSet(path string, val interface{}) *UpdateQuery {
	if uq.err != nil {
		return uq
	}
	root, err := uq.m.GetRoot(uq.rootID)
	if err != nil {
		uq.err = err
		return uq
	}

	parent, key, err := uq.ensurePath(root, path)
	if err != nil {
		uq.err = err
		return uq
	}

	child := parent.GetChild(key)
	if child == nil {
		// 初始化 ORSet
		initOp := crdt.MapOp{
			OriginID: uq.m.NodeID(),
			Key:      key,
			IsInit:   true,
			InitType: crdt.TypeSet,
			Ts:       uq.m.NextTimestamp(),
		}
		if err := parent.Apply(initOp); err != nil {
			uq.err = err
			return uq
		}

		// 持久化 init op
		keys := strings.Split(path, ".")
		if len(keys) > 0 {
			parentKeys := keys[:len(keys)-1]
			if err := uq.saveWrappedOp(parentKeys, initOp); err != nil {
				uq.err = err
				return uq
			}
		}
	}

	// 生成唯一标签和元素 ID
	ts := uq.m.NextTimestamp()
	elemID := fmt.Sprintf("%d_%s", ts, uq.m.NodeID())
	tag := fmt.Sprintf("%d_%s", ts, uq.m.NodeID())

	op := crdt.ORSetOp{
		OriginID: uq.m.NodeID(),
		TypeCode: 0, // 添加
		ElemID:   elemID,
		InitType: crdt.TypeRegister,
		InitVal:  val,
		Tag:      tag,
		Ts:       ts,
	}

	mapOp := crdt.MapOp{
		OriginID: uq.m.NodeID(),
		Key:      key,
		ChildOp:  op,
		Ts:       uq.m.NextTimestamp(),
	}

	if err := parent.Apply(mapOp); err != nil {
		uq.err = err
	}

	// 持久化 update op
	keys := strings.Split(path, ".")
	if len(keys) > 0 {
		parentKeys := keys[:len(keys)-1]
		if err := uq.saveWrappedOp(parentKeys, mapOp); err != nil {
			uq.err = err
		}
	}

	return uq
}

// RemoveFromSet 从指定路径的 Set 移除元素。
func (uq *UpdateQuery) RemoveFromSet(path string, val interface{}) *UpdateQuery {
	if uq.err != nil {
		return uq
	}
	root, err := uq.m.GetRoot(uq.rootID)
	if err != nil {
		uq.err = err
		return uq
	}

	parent, key, err := uq.ensurePath(root, path)
	if err != nil {
		uq.err = err
		return uq
	}

	child := parent.GetChild(key)
	if child == nil {
		// Set 不存在，无需操作
		return uq
	}

	// 获取当前元素的所有标签
	orset, ok := child.(*crdt.ORSet)
	if !ok {
		uq.err = fmt.Errorf("路径 %s 处的节点不是 Set", path)
		return uq
	}

	// 根据值查找元素 ID（遍历所有元素查找匹配值）
	var elemID string
	var tags []string
	for _, elem := range orset.Elements() {
		if elem.Child != nil && elem.Child.Value() == val {
			elemID = elem.ID
			tags = orset.GetTagsByID(elem.ID)
			break
		}
	}
	if elemID == "" || len(tags) == 0 {
		// 元素不存在
		return uq
	}

	op := crdt.ORSetOp{
		OriginID: uq.m.NodeID(),
		TypeCode: 1, // 移除
		RemoveID: elemID,
		RemTags:  tags,
		Ts:       uq.m.NextTimestamp(),
	}

	mapOp := crdt.MapOp{
		OriginID: uq.m.NodeID(),
		Key:      key,
		ChildOp:  op,
		Ts:       uq.m.NextTimestamp(),
	}

	if err := parent.Apply(mapOp); err != nil {
		uq.err = err
	}

	// 持久化 update op
	keys := strings.Split(path, ".")
	if len(keys) > 0 {
		parentKeys := keys[:len(keys)-1]
		if err := uq.saveWrappedOp(parentKeys, mapOp); err != nil {
			uq.err = err
		}
	}

	return uq
}

// SequenceElement 表示序列中的元素，包含 ID 和值。
type SequenceElement struct {
	ID    string      `json:"id"`
	Value interface{} `json:"value"`
}

// InsertAt 在指定路径的序列中插入元素。
// prevID: 前一个元素的 ID（使用 "start" 表示在开头插入）。
func (uq *UpdateQuery) InsertAt(path string, prevID string, value interface{}) *UpdateQuery {
	if uq.err != nil {
		return uq
	}
	root, err := uq.m.GetRoot(uq.rootID)
	if err != nil {
		uq.err = err
		return uq
	}

	parent, key, err := uq.ensurePath(root, path)
	if err != nil {
		uq.err = err
		return uq
	}

	child := parent.GetChild(key)
	if child == nil {
		// 初始化 RGA
		initOp := crdt.MapOp{
			OriginID: uq.m.NodeID(),
			Key:      key,
			IsInit:   true,
			InitType: crdt.TypeSequence,
			Ts:       uq.m.NextTimestamp(),
		}
		if err := parent.Apply(initOp); err != nil {
			uq.err = err
			return uq
		}

		// 持久化 init op
		keys := strings.Split(path, ".")
		if len(keys) > 0 {
			parentKeys := keys[:len(keys)-1]
			if err := uq.saveWrappedOp(parentKeys, initOp); err != nil {
				uq.err = err
				return uq
			}
		}
	}

	// 生成唯一元素 ID
	elemID := fmt.Sprintf("%d_%s", uq.m.NextTimestamp(), uq.m.NodeID())

	op := crdt.RGAOp{
		OriginID: uq.m.NodeID(),
		TypeCode: 0, // 插入
		PrevID:   prevID,
		ElemID:   elemID,
		InitType: crdt.TypeRegister,
		InitVal:  value,
		Ts:       uq.m.NextTimestamp(),
	}

	mapOp := crdt.MapOp{
		OriginID: uq.m.NodeID(),
		Key:      key,
		ChildOp:  op,
		Ts:       uq.m.NextTimestamp(),
	}

	if err := parent.Apply(mapOp); err != nil {
		uq.err = err
	}

	// 持久化 update op
	keys := strings.Split(path, ".")
	if len(keys) > 0 {
		parentKeys := keys[:len(keys)-1]
		if err := uq.saveWrappedOp(parentKeys, mapOp); err != nil {
			uq.err = err
		}
	}

	return uq
}

// Append 在指定路径的序列末尾追加元素。
func (uq *UpdateQuery) Append(path string, value interface{}) *UpdateQuery {
	if uq.err != nil {
		return uq
	}
	root, err := uq.m.GetRoot(uq.rootID)
	if err != nil {
		uq.err = err
		return uq
	}

	parent, key, err := uq.ensurePath(root, path)
	if err != nil {
		uq.err = err
		return uq
	}

	child := parent.GetChild(key)
	if child == nil {
		// 初始化 RGA
		initOp := crdt.MapOp{
			OriginID: uq.m.NodeID(),
			Key:      key,
			IsInit:   true,
			InitType: crdt.TypeSequence,
			Ts:       uq.m.NextTimestamp(),
		}
		if err := parent.Apply(initOp); err != nil {
			uq.err = err
			return uq
		}

		// 持久化 init op
		keys := strings.Split(path, ".")
		if len(keys) > 0 {
			parentKeys := keys[:len(keys)-1]
			if err := uq.saveWrappedOp(parentKeys, initOp); err != nil {
				uq.err = err
				return uq
			}
		}

		child = parent.GetChild(key)
	}

	// 找到序列末尾的元素 ID
	rga, ok := child.(*crdt.RGA)
	if !ok {
		uq.err = fmt.Errorf("路径 %s 处的节点不是 Sequence", path)
		return uq
	}

	lastID := rga.LastID()

	// 生成唯一元素 ID
	elemID := fmt.Sprintf("%d_%s", uq.m.NextTimestamp(), uq.m.NodeID())

	op := crdt.RGAOp{
		OriginID: uq.m.NodeID(),
		TypeCode: 0, // 插入
		PrevID:   lastID,
		ElemID:   elemID,
		InitType: crdt.TypeRegister,
		InitVal:  value,
		Ts:       uq.m.NextTimestamp(),
	}

	mapOp := crdt.MapOp{
		OriginID: uq.m.NodeID(),
		Key:      key,
		ChildOp:  op,
		Ts:       uq.m.NextTimestamp(),
	}

	if err := parent.Apply(mapOp); err != nil {
		uq.err = err
	}

	// 持久化 update op
	keys := strings.Split(path, ".")
	if len(keys) > 0 {
		parentKeys := keys[:len(keys)-1]
		if err := uq.saveWrappedOp(parentKeys, mapOp); err != nil {
			uq.err = err
		}
	}

	return uq
}

// RemoveAt 从序列中移除指定 ID 的元素。
func (uq *UpdateQuery) RemoveAt(path string, elemID string) *UpdateQuery {
	if uq.err != nil {
		return uq
	}
	root, err := uq.m.GetRoot(uq.rootID)
	if err != nil {
		uq.err = err
		return uq
	}

	parent, key, err := uq.ensurePath(root, path)
	if err != nil {
		uq.err = err
		return uq
	}

	child := parent.GetChild(key)
	if child == nil {
		// 序列不存在，无需操作
		return uq
	}

	op := crdt.RGAOp{
		OriginID: uq.m.NodeID(),
		TypeCode: 1, // 移除
		RemoveID: elemID,
		Ts:       uq.m.NextTimestamp(),
	}

	mapOp := crdt.MapOp{
		OriginID: uq.m.NodeID(),
		Key:      key,
		ChildOp:  op,
		Ts:       uq.m.NextTimestamp(),
	}

	if err := parent.Apply(mapOp); err != nil {
		uq.err = err
	}

	// 持久化 update op
	keys := strings.Split(path, ".")
	if len(keys) > 0 {
		parentKeys := keys[:len(keys)-1]
		if err := uq.saveWrappedOp(parentKeys, mapOp); err != nil {
			uq.err = err
		}
	}

	return uq
}

// GetSequence 获取序列内容（带元素 ID）。
func (q *Query) GetSequence(path string) ([]SequenceElement, error) {
	root, err := q.m.GetRoot(q.rootID)
	if err != nil {
		return nil, err
	}

	val, err := q.getValueAtPath(root, path)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// 需要获取底层 RGA 以获取元素 ID
	keys := strings.Split(path, ".")
	current := root
	for _, key := range keys {
		mapC, ok := current.(*crdt.MapCRDT)
		if !ok {
			return nil, fmt.Errorf("路径 %s 中的节点不是 Map", path)
		}
		current = mapC.GetChild(key)
		if current == nil {
			return nil, nil
		}
	}

	rga, ok := current.(*crdt.RGA)
	if !ok {
		return nil, fmt.Errorf("路径 %s 处的节点不是 Sequence", path)
	}

	// 将 RGAElement 转换为 SequenceElement
	rgaElems := rga.Elements()
	result := make([]SequenceElement, len(rgaElems))
	for i, elem := range rgaElems {
		result[i] = SequenceElement{
			ID:    elem.ID,
			Value: elem.Value,
		}
	}
	return result, nil
}

func (uq *UpdateQuery) Commit() error {
	return uq.err
}

// 辅助函数

// ensurePath 遍历路径，如果中间 Map 不存在则创建。返回父 Map 和最后一个键。
func (uq *UpdateQuery) ensurePath(root crdt.CRDT, path string) (*crdt.MapCRDT, string, error) {
	keys := strings.Split(path, ".")
	if len(keys) == 0 {
		return nil, "", fmt.Errorf("路径为空")
	}

	current := root
	for i := 0; i < len(keys)-1; i++ {
		key := keys[i]
		mapC, ok := current.(*crdt.MapCRDT)
		if !ok {
			return nil, "", fmt.Errorf("路径中间节点不是 Map: %s", key)
		}

		child := mapC.GetChild(key)
		if child == nil {
			// 自动创建中间 Map
			initOp := crdt.MapOp{
				OriginID: uq.m.NodeID(),
				Key:      key,
				IsInit:   true,
				InitType: crdt.TypeMap,
				Ts:       uq.m.NextTimestamp(),
			}
			if err := mapC.Apply(initOp); err != nil {
				return nil, "", err
			}

			// 持久化 init op
			if err := uq.saveWrappedOp(keys[:i], initOp); err != nil {
				return nil, "", err
			}

			child = mapC.GetChild(key)
		}
		current = child
	}

	parentMap, ok := current.(*crdt.MapCRDT)
	if !ok {
		return nil, "", fmt.Errorf("父节点不是 Map")
	}

	return parentMap, keys[len(keys)-1], nil
}

// findMap 查找指定路径的 Map，如果不存在返回 nil
func (uq *UpdateQuery) findMap(root crdt.CRDT, path string) (*crdt.MapCRDT, error) {
	keys := strings.Split(path, ".")
	current := root
	for _, key := range keys {
		mapC, ok := current.(*crdt.MapCRDT)
		if !ok {
			return nil, fmt.Errorf("路径中间节点不是 Map: %s", key)
		}
		child := mapC.GetChild(key)
		if child == nil {
			return nil, nil
		}
		current = child
	}
	mapC, ok := current.(*crdt.MapCRDT)
	if !ok {
		return nil, fmt.Errorf("目标不是 Map")
	}
	return mapC, nil
}

// GetContent 检索 LocalFile 根节点的文件内容。
func (q *Query) GetContent() ([]byte, error) {
	root, err := q.m.GetRoot(q.rootID)
	if err != nil {
		return nil, err
	}

	fRoot, ok := root.(*crdt.LocalFile)
	if !ok {
		return nil, fmt.Errorf("根节点不是本地文件")
	}

	val := fRoot.Value()
	meta, ok := val.(crdt.FileMetadata)
	if !ok {
		return nil, fmt.Errorf("文件元数据无效")
	}

	return q.m.GetBlob(meta.Hash)
}

func (uq *UpdateQuery) saveWrappedOp(path []string, op crdt.Op) error {
	wrapped := op
	for i := len(path) - 1; i >= 0; i-- {
		wrapped = crdt.MapOp{
			OriginID: uq.m.NodeID(),
			Key:      path[i],
			ChildOp:  wrapped,
			Ts:       op.Timestamp(),
		}
	}
	return uq.m.SaveOp(uq.rootID, wrapped)
}

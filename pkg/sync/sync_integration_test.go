package sync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// TestIncrementalSync_BasicMerge 测试基础增量同步和 CRDT Merge
func TestIncrementalSync_BasicMerge(t *testing.T) {
	// 创建两个节点
	_, db1 := createTestNode(t, "node-1")
	node2, db2 := createTestNode(t, "node-2")
	defer db1.Close()
	defer db2.Close()

	// 定义相同的表结构
	schema := &meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "email", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}
	if err := db1.DefineTable(schema); err != nil {
		t.Fatalf("节点1定义表失败: %v", err)
	}
	if err := db2.DefineTable(schema); err != nil {
		t.Fatalf("节点2定义表失败: %v", err)
	}

	// 节点1写入数据
	userID, _ := uuid.NewV7()
	err := db1.Update(func(tx *db.Tx) error {
		table := tx.Table("users")
		return table.Set(userID, map[string]any{
			"name":  "Alice",
			"email": "alice@example.com",
		})
	})
	if err != nil {
		t.Fatalf("节点1写入失败: %v", err)
	}

	t.Logf("节点1写入数据: %s", userID.String()[:8])

	// 获取原始 CRDT 字节
	var rawData []byte
	db1.View(func(tx *db.Tx) error {
		table := tx.Table("users")
		rawData, _ = table.GetRawRow(userID)
		return nil
	})

	if rawData == nil {
		t.Fatal("获取原始数据失败")
	}

	t.Logf("原始 CRDT 字节大小: %d bytes", len(rawData))

	// 模拟增量同步：节点2接收并 Merge
	clock1 := db1.Clock().Now()
	err = node2.dataSync.OnReceiveMerge("users", userID.String(), rawData, clock1)
	if err != nil {
		t.Fatalf("节点2 Merge 失败: %v", err)
	}

	t.Logf("节点2成功 Merge 数据")

	// 验证节点2的数据
	var user2 map[string]any
	db2.View(func(tx *db.Tx) error {
		table := tx.Table("users")
		user2, _ = table.Get(userID)
		return nil
	})

	if user2 == nil {
		t.Fatal("节点2未找到数据")
	}

	if toString(user2["name"]) != "Alice" || toString(user2["email"]) != "alice@example.com" {
		t.Errorf("节点2数据不匹配: %+v", user2)
	}

	t.Logf("✓ 增量同步成功，数据一致")
}

// TestIncrementalSync_LWWConflict 测试 LWW 冲突解决
func TestIncrementalSync_LWWConflict(t *testing.T) {
	node1, db1 := createTestNode(t, "node-1")
	node2, db2 := createTestNode(t, "node-2")
	defer db1.Close()
	defer db2.Close()

	// 定义表
	schema := &meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "email", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}
	db1.DefineTable(schema)
	db2.DefineTable(schema)

	userID, _ := uuid.NewV7()

	// 节点1写入初始数据
	db1.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(userID, map[string]any{
			"email": "alice@v1.com",
		})
	})

	// 同步到节点2
	var rawData1 []byte
	db1.View(func(tx *db.Tx) error {
		rawData1, _ = tx.Table("users").GetRawRow(userID)
		return nil
	})
	node2.dataSync.OnReceiveMerge("users", userID.String(), rawData1, db1.Clock().Now())

	t.Logf("初始同步完成")

	// 模拟并发更新：节点1和节点2同时更新
	time.Sleep(10 * time.Millisecond) // 确保时钟推进

	// 节点1更新
	db1.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(userID, map[string]any{
			"email": "alice@node1.com",
		})
	})
	clock1 := db1.Clock().Now()

	time.Sleep(20 * time.Millisecond) // 节点2稍后更新，HLC 会更大

	// 节点2更新
	db2.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(userID, map[string]any{
			"email": "alice@node2.com",
		})
	})
	clock2 := db2.Clock().Now()

	t.Logf("节点1时钟: %d, 节点2时钟: %d", clock1, clock2)

	// 双向同步
	var rawDataNode1, rawDataNode2 []byte
	db1.View(func(tx *db.Tx) error {
		rawDataNode1, _ = tx.Table("users").GetRawRow(userID)
		return nil
	})
	db2.View(func(tx *db.Tx) error {
		rawDataNode2, _ = tx.Table("users").GetRawRow(userID)
		return nil
	})

	// 节点1接收节点2的更新
	node1.dataSync.OnReceiveMerge("users", userID.String(), rawDataNode2, clock2)

	// 节点2接收节点1的更新
	node2.dataSync.OnReceiveMerge("users", userID.String(), rawDataNode1, clock1)

	// 验证：HLC 时钟较大的应该胜出（节点2）
	var email1, email2 string
	db1.View(func(tx *db.Tx) error {
		user, _ := tx.Table("users").Get(userID)
		email1 = toString(user["email"])
		return nil
	})
	db2.View(func(tx *db.Tx) error {
		user, _ := tx.Table("users").Get(userID)
		email2 = toString(user["email"])
		return nil
	})

	t.Logf("节点1 email: %s, 节点2 email: %s", email1, email2)

	// 两个节点应该收敛到相同的值（HLC 较大的）
	if email1 != email2 {
		t.Errorf("LWW 冲突解决失败: 节点1=%s, 节点2=%s", email1, email2)
	}

	if email1 != "alice@node2.com" {
		t.Errorf("预期 alice@node2.com (HLC 较大), 实际: %s", email1)
	}

	t.Logf("✓ LWW 冲突解决成功，收敛到: %s", email1)
}

// TestIncrementalSync_ORSetMerge 测试 ORSet 并发添加
func TestIncrementalSync_ORSetMerge(t *testing.T) {
	node1, db1 := createTestNode(t, "node-1")
	node2, db2 := createTestNode(t, "node-2")
	defer db1.Close()
	defer db2.Close()

	// 定义表
	schema := &meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "tags", Type: meta.ColTypeString, CrdtType: meta.CrdtORSet},
		},
	}
	db1.DefineTable(schema)
	db2.DefineTable(schema)

	userID, _ := uuid.NewV7()

	// 节点1添加标签 "developer"
	err := db1.Update(func(tx *db.Tx) error {
		return tx.Table("users").Add(userID, "tags", "developer")
	})
	if err != nil {
		t.Fatalf("节点1添加失败: %v", err)
	}

	// 节点2添加标签 "admin"
	err = db2.Update(func(tx *db.Tx) error {
		return tx.Table("users").Add(userID, "tags", "admin")
	})
	if err != nil {
		t.Fatalf("节点2添加失败: %v", err)
	}

	t.Logf("节点1添加: developer, 节点2添加: admin")

	// 双向同步
	var rawData1, rawData2 []byte
	db1.View(func(tx *db.Tx) error {
		rawData1, _ = tx.Table("users").GetRawRow(userID)
		return nil
	})
	db2.View(func(tx *db.Tx) error {
		rawData2, _ = tx.Table("users").GetRawRow(userID)
		return nil
	})

	node1.dataSync.OnReceiveMerge("users", userID.String(), rawData2, db2.Clock().Now())
	node2.dataSync.OnReceiveMerge("users", userID.String(), rawData1, db1.Clock().Now())

	// 验证：两个标签都应该存在
	var user1Data, user2Data map[string]any
	db1.View(func(tx *db.Tx) error {
		user1Data, _ = tx.Table("users").Get(userID)
		return nil
	})
	db2.View(func(tx *db.Tx) error {
		user2Data, _ = tx.Table("users").Get(userID)
		return nil
	})

	t.Logf("节点1数据: %+v", user1Data)
	t.Logf("节点2数据: %+v", user2Data)

	tags1 := toStringSlice(user1Data["tags"])
	tags2 := toStringSlice(user2Data["tags"])

	t.Logf("节点1 tags: %v, 节点2 tags: %v", tags1, tags2)

	// 检查两个节点的标签集合是否一致
	if len(tags1) != 2 || len(tags2) != 2 {
		t.Errorf("ORSet Merge 失败: 节点1=%v, 节点2=%v", tags1, tags2)
	} else {
		// 检查是否包含两个标签
		hasAdmin1, hasDev1 := false, false
		for _, tag := range tags1 {
			if tag == "admin" {
				hasAdmin1 = true
			}
			if tag == "developer" {
				hasDev1 = true
			}
		}

		if !hasAdmin1 || !hasDev1 {
			t.Errorf("节点1缺少标签: %v", tags1)
		}
	}

	t.Logf("✓ ORSet 并发添加成功")
}

// TestFullSync_CompleteTable 测试完整表的全量同步
func TestFullSync_CompleteTable(t *testing.T) {
	node1, db1 := createTestNode(t, "node-1")
	node2, db2 := createTestNode(t, "node-2")
	defer db1.Close()
	defer db2.Close()

	// 定义表
	schema := &meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "email", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}
	db1.DefineTable(schema)
	db2.DefineTable(schema)

	// 节点1写入多条数据
	userIDs := make([]uuid.UUID, 10)
	for i := 0; i < 10; i++ {
		userIDs[i], _ = uuid.NewV7()
		db1.Update(func(tx *db.Tx) error {
			return tx.Table("users").Set(userIDs[i], map[string]any{
				"name":  "User" + string(rune('A'+i)),
				"email": "user" + string(rune('a'+i)) + "@example.com",
			})
		})
	}

	t.Logf("节点1写入 %d 条数据", len(userIDs))

	// 创建模拟网络
	mockNet := &MockNetwork{
		node1: node1,
		node2: node2,
		db1:   db1,
		db2:   db2,
	}
	node2.RegisterNetwork(mockNet)

	// 节点2从节点1全量同步
	ctx := context.Background()
	result, err := node2.FullSync(ctx, "node-1")
	if err != nil {
		t.Fatalf("全量同步失败: %v", err)
	}

	t.Logf("全量同步结果: tables=%d, rows=%d, rejected=%d",
		result.TablesSynced, result.RowsSynced, result.RejectedCount)

	if result.TablesSynced != 1 {
		t.Errorf("预期同步1个表，实际: %d", result.TablesSynced)
	}

	if result.RowsSynced != 10 {
		t.Errorf("预期同步10行，实际: %d", result.RowsSynced)
	}

	// 验证节点2的数据
	var users2 []map[string]any
	db2.View(func(tx *db.Tx) error {
		users2, _ = tx.Table("users").Where("name", "!=", "").Limit(100).Find()
		return nil
	})

	if len(users2) != 10 {
		t.Errorf("节点2数据数量不匹配: 预期10, 实际%d", len(users2))
	}

	t.Logf("✓ 全量同步成功，节点2获得 %d 条数据", len(users2))
}

// TestFullSync_WithExistingData 测试有现有数据时的全量同步（Merge）
func TestFullSync_WithExistingData(t *testing.T) {
	node1, db1 := createTestNode(t, "node-1")
	node2, db2 := createTestNode(t, "node-2")
	defer db1.Close()
	defer db2.Close()

	// 定义表
	schema := &meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}
	db1.DefineTable(schema)
	db2.DefineTable(schema)

	// 节点1和节点2都有一些数据
	sharedID, _ := uuid.NewV7()
	node1OnlyID, _ := uuid.NewV7()
	node2OnlyID, _ := uuid.NewV7()

	// 共享数据（两个节点都有，但值不同）
	db1.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(sharedID, map[string]any{"name": "SharedUser_Node1"})
	})
	time.Sleep(10 * time.Millisecond)
	db2.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(sharedID, map[string]any{"name": "SharedUser_Node2"})
	})

	// 节点1独有数据
	db1.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(node1OnlyID, map[string]any{"name": "Node1Only"})
	})

	// 节点2独有数据
	db2.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(node2OnlyID, map[string]any{"name": "Node2Only"})
	})

	t.Logf("初始状态: 节点1有2条, 节点2有2条, 共享1条")

	// 创建模拟网络
	mockNet := &MockNetwork{node1: node1, node2: node2, db1: db1, db2: db2}
	node2.RegisterNetwork(mockNet)

	// 节点2从节点1全量同步
	ctx := context.Background()
	result, err := node2.FullSync(ctx, "node-1")
	if err != nil {
		t.Fatalf("全量同步失败: %v", err)
	}

	t.Logf("全量同步结果: rows=%d", result.RowsSynced)

	// 验证节点2现在应该有3条数据（合并后）
	var users2 []map[string]any
	db2.View(func(tx *db.Tx) error {
		users2, _ = tx.Table("users").Where("name", "!=", "").Limit(100).Find()
		return nil
	})

	if len(users2) != 3 {
		t.Errorf("节点2数据数量不匹配: 预期3, 实际%d", len(users2))
	}

	// 验证共享数据的冲突解决（HLC 较大的应该胜出）
	var sharedUser map[string]any
	db2.View(func(tx *db.Tx) error {
		sharedUser, _ = tx.Table("users").Get(sharedID)
		return nil
	})

	t.Logf("共享数据最终值: %s", toString(sharedUser["name"]))
	// 节点2的更新时间更晚，应该保留
	if toString(sharedUser["name"]) != "SharedUser_Node2" {
		t.Errorf("共享数据冲突解决错误: %s", toString(sharedUser["name"]))
	}

	t.Logf("✓ 全量同步 Merge 成功")
}

// TestIncrementalSync_RejectStaleData 测试拒绝过期数据
func TestIncrementalSync_RejectStaleData(t *testing.T) {
	node1, db1 := createTestNode(t, "node-1")
	defer db1.Close()

	schema := &meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}
	db1.DefineTable(schema)

	// 通过写入操作推进本地时钟
	tempID, _ := uuid.NewV7()
	db1.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(tempID, map[string]any{"name": "temp"})
	})

	currentClock := db1.Clock().Now()
	t.Logf("当前时钟: %d", currentClock)

	// 尝试接收过期数据
	userID, _ := uuid.NewV7()
	staleTimestamp := currentClock - 1000
	testRawData := []byte(`{"Entries":{}}`)

	err := node1.dataSync.OnReceiveMerge("users", userID.String(), testRawData, staleTimestamp)

	if err == nil {
		t.Error("应该拒绝过期数据")
	}

	t.Logf("✓ 正确拒绝过期数据: %v", err)
}

// MockNetwork 模拟网络接口用于测试
type MockNetwork struct {
	node1 *NodeManager
	node2 *NodeManager
	db1   *db.DB
	db2   *db.DB
}

func (m *MockNetwork) SendHeartbeat(targetNodeID string, clock int64) error {
	return nil
}

func (m *MockNetwork) BroadcastHeartbeat(clock int64) error {
	return nil
}

func (m *MockNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	return nil
}

func (m *MockNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	return nil
}

func (m *MockNetwork) SendMessage(targetNodeID string, msg *NetworkMessage) error {
	return nil
}

func (m *MockNetwork) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	// 从节点1导出数据
	if sourceNodeID == "node-1" {
		return m.node1.dataSync.ExportTableRawData(tableName)
	}
	return nil, nil
}

// createTestNode 创建测试节点
func createTestNode(t *testing.T, nodeID string) (*NodeManager, *db.DB) {
	s, err := store.NewBadgerStore(t.TempDir() + "/" + nodeID)
	if err != nil {
		t.Fatalf("创建存储失败: %v", err)
	}

	database := db.Open(s, nodeID)
	nm := NewNodeManager(database, nodeID)

	return nm, database
}

// toString 安全地将值转为字符串（处理 string/[]byte/fmt.Stringer）
func toString(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// toStringSlice 安全地将值转为 []string
func toStringSlice(v any) []string {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []any:
		result := make([]string, len(val))
		for i, item := range val {
			result[i] = toString(item)
		}
		return result
	case []string:
		return val
	default:
		return nil
	}
}

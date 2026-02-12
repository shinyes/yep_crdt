package db

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// TestDB_GC 测试数据库层面的 GC 功能
func TestDB_GC(t *testing.T) {
	// 创建临时数据库
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("NewBadgerStore failed: %v", err)
	}
	defer s.Close()

	db := Open(s, "test-db")

	// 定义表
	schema := &meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString},
			{Name: "email", Type: meta.ColTypeString},
		},
	}
	if err := db.DefineTable(schema); err != nil {
		t.Fatalf("DefineTable failed: %v", err)
	}

	table := db.Table("users")
	if table == nil {
		t.Fatal("Table not found")
	}

	// 插入多行数据
	for i := 0; i < 10; i++ {
		key := uuid.New()
		data := map[string]any{
			"name":  "user-" + string(rune('A'+i)),
			"email": "user-" + string(rune('A'+i)) + "@example.com",
		}
		if err := table.Set(key, data); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	t.Log("插入 10 行数据")

	// 获取当前时间
	currentTime := db.Now()
	t.Logf("当前时间: %d", currentTime)

	// 使用较早的时间进行 GC（应该不会清理任何东西）
	result := db.GC(currentTime - 1000)
	t.Logf("GC 结果（safeTimestamp=%d）: 扫描表=%d, 扫描行=%d, 清理 tombstone=%d",
		currentTime-1000, result.TablesScanned, result.RowsScanned, result.TombstonesRemoved)

	if result.TablesScanned == 0 {
		t.Error("应该扫描至少一个表")
	}
	if result.RowsScanned == 0 {
		t.Error("应该扫描至少一行")
	}

	// 等待一段时间
	time.Sleep(100 * time.Millisecond)
	safeTimestamp := db.Now()

	// 再次 GC
	result = db.GC(safeTimestamp)
	t.Logf("GC 结果（safeTimestamp=%d）: 扫描表=%d, 扫描行=%d, 清理 tombstone=%d",
		safeTimestamp, result.TablesScanned, result.RowsScanned, result.TombstonesRemoved)

	if len(result.Errors) > 0 {
		t.Errorf("GC 遇到错误: %v", result.Errors)
	}
}

// TestDB_GCByTimeOffset 测试基于时间偏移量的 GC
func TestDB_GCByTimeOffset(t *testing.T) {
	// 创建临时数据库
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("NewBadgerStore failed: %v", err)
	}
	defer s.Close()

	db := Open(s, "test-db")

	// 定义表
	schema := &meta.TableSchema{
		Name: "documents",
		Columns: []meta.ColumnSchema{
			{Name: "title", Type: meta.ColTypeString},
		},
	}
	if err := db.DefineTable(schema); err != nil {
		t.Fatalf("DefineTable failed: %v", err)
	}

	table := db.Table("documents")
	if table == nil {
		t.Fatal("Table not found")
	}

	// 插入数据
	key := uuid.New()
	data := map[string]any{
		"title": "Test Document",
	}
	if err := table.Set(key, data); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// 等待一会儿
	time.Sleep(50 * time.Millisecond)

	// 使用 GCByTimeOffset，清理 30 毫秒前的数据
	result := db.GCByTimeOffset(30 * time.Millisecond)
	t.Logf("GCByTimeOffset 结果: 扫描表=%d, 扫描行=%d, 清理 tombstone=%d",
		result.TablesScanned, result.RowsScanned, result.TombstonesRemoved)

	if result.TablesScanned == 0 {
		t.Error("应该扫描至少一个表")
	}
	if result.RowsScanned == 0 {
		t.Error("应该扫描至少一行")
	}
}

// TestDB_GCWithORSet 测试 ORSet 在数据库层面的 GC
func TestDB_GCWithORSet(t *testing.T) {
	// 创建临时数据库
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("NewBadgerStore failed: %v", err)
	}
	defer s.Close()

	db := Open(s, "test-db")

	// 定义表
	schema := &meta.TableSchema{
		Name: "collections",
		Columns: []meta.ColumnSchema{
			{Name: "tags", Type: meta.ColTypeString},
		},
	}
	if err := db.DefineTable(schema); err != nil {
		t.Fatalf("DefineTable failed: %v", err)
	}

	table := db.Table("collections")
	if table == nil {
		t.Fatal("Table not found")
	}

	// 插入包含 ORSet 的数据
	key := uuid.New()
	
	// 创建 ORSet 并添加元素
	tagSet := crdt.NewORSet[string]()
	tagSet.Apply(crdt.OpORSetAdd[string]{Element: "tag1"})
	tagSet.Apply(crdt.OpORSetAdd[string]{Element: "tag2"})
	tagSet.Apply(crdt.OpORSetAdd[string]{Element: "tag3"})

	data := map[string]any{
		"tags": tagSet,
	}
	if err := table.Set(key, data); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	t.Log("初始状态:")
	t.Logf("  Tags: %v", tagSet.Elements())
	t.Logf("  Tombstones: %d", len(tagSet.Tombstones))

	// 删除一个标签
	tagSet.Apply(crdt.OpORSetRemove[string]{Element: "tag1"})
	
	data = map[string]any{
		"tags": tagSet,
	}
	if err := table.Set(key, data); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	t.Log("删除 tag1 后:")
	t.Logf("  Tags: %v", tagSet.Elements())
	t.Logf("  Tombstones: %d", len(tagSet.Tombstones))

	// 等待一段时间
	time.Sleep(50 * time.Millisecond)
	safeTimestamp := db.Now()

	// 执行 GC
	result := db.GC(safeTimestamp)
	t.Logf("GC 结果: 扫描表=%d, 扫描行=%d, 清理 tombstone=%d",
		result.TablesScanned, result.RowsScanned, result.TombstonesRemoved)

	if len(result.Errors) > 0 {
		t.Errorf("GC 遇到错误: %v", result.Errors)
	}

	// 验证数据仍然正确
	retrieved, err := table.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	t.Logf("GC 后:")
	t.Logf("  Retrieved data: %v", retrieved)
	t.Logf("  Tags type: %T", retrieved["tags"])

	// 检查 tags 是否是 CRDT 类型
	if tagsCRDT, ok := retrieved["tags"].(crdt.CRDT); ok {
		// 尝试转换为 ORSet
		// 注意：由于泛型，我们无法直接转换为 *ORSet[string]
		// 所以我们只检查它是否是 CRDT 接口
		t.Logf("  Tags is CRDT: %v", tagsCRDT.Type())
	}
	
	// 由于 Table.Get 返回的是 Value()，我们无法直接访问 Tombstones
	// 这是预期的行为
}

// TestDB_TableGC 测试表级别的 GC
func TestDB_TableGC(t *testing.T) {
	// 创建临时数据库
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("NewBadgerStore failed: %v", err)
	}
	defer s.Close()

	db := Open(s, "test-db")

	// 定义表
	schema := &meta.TableSchema{
		Name: "items",
		Columns: []meta.ColumnSchema{
			{Name: "value", Type: meta.ColTypeString},
		},
	}
	if err := db.DefineTable(schema); err != nil {
		t.Fatalf("DefineTable failed: %v", err)
	}

	table := db.Table("items")
	if table == nil {
		t.Fatal("Table not found")
	}

	// 插入多行
	for i := 0; i < 5; i++ {
		key := uuid.New()
		data := map[string]any{
			"value": string(rune('A' + i)),
		}
		if err := table.Set(key, data); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// 执行表级 GC
	safeTimestamp := db.Now()
	result := table.GC(safeTimestamp)

	t.Logf("表 GC 结果: 扫描行=%d, 清理 tombstone=%d",
		result.RowsScanned, result.TombstonesRemoved)

	if result.RowsScanned == 0 {
		t.Error("应该扫描至少一行")
	}

	if len(result.Errors) > 0 {
		t.Errorf("表 GC 遇到错误: %v", result.Errors)
	}
}

# Yep CRDT Database

一个简单的、支持 CRDT 的本地优先 (Local-First) 数据库，构建在 BadgerDB 之上，提供类似 SQL 的查询能力、自动索引以及针对特定 CRDT 类型的高级操作支持。

## 特性

*   **本地优先 (Local-First)**: 数据存储在本地，支持离线操作。
*   **CRDT 支持**: 内置多种 CRDT 类型，支持细粒度的无冲突更新。
    *   `LWW-Register`: 最后写入胜出 (Last-Write-Wins)，适用于普通字段。
    *   `OR-Set`: 观察-移除集合 (Observed-Remove Set)，支持泛型 `ORSet[T]`，适用于标签、类别等。
    *   `PN-Counter`: 正负计数器 (Positive-Negative Counter)，适用于点赞数、浏览量等。
    *   `RGA`: 复制可增长数组 (Replicated Growable Array)，支持泛型 `RGA[T]`，适用于有序列表、TODO 列表等。
*   **SQL-Like 查询**: 提供流式 API 进行数据查询。
    *   支持 `Where`, `And`, `Limit`, `Offset`, `OrderBy` 等操作。
    *   支持 `=`, `!=`, `>`, `>=`, `<`, `<=`, `IN` 等条件。
*   **智能查询规划器**:
    *   实现了 **最长前缀匹配 (Longest Prefix Match)** 算法。
    *   自动选择最佳的复合索引或单列索引。
*   **分布式基础**:
    *   **NodeID 持久化**: 节点身份在重启后保持不变。
    *   **混合逻辑时钟 (HLC)**: 提供因果一致的时间戳，为分布式同步奠定基础。
*   **自动索引**: 定义 Schema 后自动维护二级索引。
*   **多租户支持**: 基于文件系统的多租户隔离。
*   **泛型支持 (Generics)**: 核心 CRDT 类型 (`ORSet`, `RGA`) 全面支持 Go 泛型，提供更好的类型安全和开发体验。
*   **垃圾回收 (Garbage Collection)**:
    *   **稳定时间戳**: 基于 Hybrid Logical Clock (HLC) 的 Safe Time 机制。
    *   **自动清理**: 自动清理过期的 Tombstones (ORSet) 和物理移除已删除的节点 (RGA)，彻底解决 CRDT 元数据膨胀问题。
*   **强制 UUIDv7**: 主键必须是有效的 UUIDv7 格式，以确保时间有序性和全局唯一性。
*   **事务支持**: 所有更新操作都在 BadgerDB 的事务中原子执行。

## 快速开始

### 安装

```bash
go get github.com/shinyes/yep_crdt
```

### 完整使用指南

#### 1. 初始化数据库

```go
package main

import (
	"log"
	"os"
	"fmt"

	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
	"github.com/google/uuid"
)

func main() {
	// 初始化存储路径
	dbPath := "./tmp/my_db"
	os.MkdirAll(dbPath, 0755)

	// 创建 BadgerDB 存储后端
	s, err := store.NewBadgerStore(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// 打开数据库实例
	myDB := db.Open(s)
    // ...
}
```

#### 2. 定义 Schema (表结构)

支持为每一列指定 CRDT 类型。如果未指定，默认为 `LWW`。

```go
	err := myDB.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			// 普通字段 (LWW)
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "age", Type: meta.ColTypeInt, CrdtType: meta.CrdtLWW},
            
			// 计数器 (Counter)
			{Name: "views", Type: meta.ColTypeInt, CrdtType: meta.CrdtCounter},
            
			// 集合 (ORSet)
			{Name: "tags", Type: meta.ColTypeString, CrdtType: meta.CrdtORSet},
            
			// 有序列表 (RGA)
			{Name: "todos", Type: meta.ColTypeString, CrdtType: meta.CrdtRGA},
		},
		Indexes: []meta.IndexSchema{
			// 复合索引
			{Name: "idx_name_age", Columns: []string{"name", "age"}, Unique: false},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	table := myDB.Table("users")
```

#### 3. 基础操作 (LWW Register)

**注意**: 所有键必须是有效的 UUIDv7。

```go
	// 生成 UUIDv7 主键
	u1ID, _ := uuid.NewV7()

	// 插入或更新整行 (或部分 LWW 列)
	table.Set(u1ID, map[string]any{
		"name": "Alice", 
		"age": 30,
	})
```

#### 4. 高级 CRDT 操作

对于 `Counter`, `ORSet`, `RGA` 类型的列，请使用 `Add`, `Remove` 等专用方法，以保留并发合并特性。

##### 计数器 (Counter)

```go
	// 增加
	table.Add(u1ID, "views", 10) // views = 10
	table.Add(u1ID, "views", 5)  // views = 15

	// 减少 (两种方式)
	table.Add(u1ID, "views", -3) // views = 12
	table.Remove(u1ID, "views", 2) // views = 10
```

##### 集合 (ORSet)

```go
	// 添加元素
	table.Add(u1ID, "tags", "developer")
	table.Add(u1ID, "tags", "golang")

	// 移除元素
	table.Remove(u1ID, "tags", "developer")
    // tags 现在只包含 "golang"
```

##### 有序列表 (RGA)

```go
	// 追加到末尾
	table.Add(u1ID, "todos", "Task 1")
	table.Add(u1ID, "todos", "Task 3")
    
	// 在指定元素后插入
	table.InsertAfter(u1ID, "todos", "Task 1", "Task 2")
    // 列表顺序: [Task 1, Task 2, Task 3]

	// 按索引插入 (0-based)
	table.InsertAt(u1ID, "todos", 0, "Urgent Task")
    // 列表顺序: [Urgent Task, Task 1, Task 2, Task 3]

	// 按索引移除
	table.RemoveAt(u1ID, "todos", 3) // 移除 "Task 3"
```

#### 5. 查询数据

查询规划器会自动选择最佳索引。支持丰富的查询操作符和分页/排序。

```go
	// 简单主键查询
	u1, _ := table.Get(u1ID)
	fmt.Println(u1)

	// 复杂条件查询
	// 自动使用 idx_name_age 索引
	results, err := table.Where("name", db.OpEq, "Alice").
                          And("age", db.OpGt, 20).
                          OrderBy("age", true). // 按 age 倒序
                          Offset(0).
                          Limit(10).
                          Find()
    
    // IN 查询
    results, err = table.Where("views", db.OpIn, []any{100, 200}).Find()

	for _, row := range results {
		fmt.Printf("Row: %v\n", row)
	}
```

#### 6. 事务支持

使用 `Update` 或 `View` 方法执行原子性操作。

```go
	err := myDB.Update(func(tx *db.Tx) error {
		// 获取绑定到事务的表句柄
		t := tx.Table("users")
		
		// 所有的操作都在同一个事务中原子执行
		err := t.Add(u1ID, "views", 100)
		if err != nil {
			return err // 返回错误将导致事务回滚
		}

		return t.Add(u2ID, "views", 100)
	})
```

## 独立使用 CRDT 包 (Standalone CRDT Package)

`pkg/crdt` 可以作为独立的 Go 库使用，支持泛型和垃圾回收。

```go
package main

import (
    "fmt"
    "github.com/shinyes/yep_crdt/pkg/crdt"
    "github.com/shinyes/yep_crdt/pkg/hlc"
)

func main() {
    // 1. 泛型 ORSet (支持任意 comparable 类型)
    intSet := crdt.NewORSet[int]()
    intSet.Apply(crdt.OpORSetAdd[int]{Element: 100})
    intSet.Apply(crdt.OpORSetAdd[int]{Element: 200})
    fmt.Println(intSet.Value()) // Output: [100 200]

    // 2. 泛型 RGA (支持任意类型)
    clock := hlc.New()
    rga := crdt.NewRGA[string](clock)
    
    // 插入操作
    rga.Apply(crdt.OpRGAInsert[string]{AnchorID: rga.Head, Value: "Hello"})
    

    // ...
    // 3. 垃圾回收 (GC)
    // 假设 safeTimestamp 是集群中最小的已知 HLC 时间
    safeTime := clock.Now() 
    
    // 执行 GC，物理移除已删除且过期的节点
    removed := rga.GC(safeTime)
    fmt.Printf("垃圾回收移除节点数: %d\n", removed)
}
```

# 获取当前节点 HLC
```go
package main

import (
    "fmt"
    "github.com/shinyes/yep_crdt/pkg/db"
    "github.com/shinyes/yep_crdt/pkg/store"
)

func main() {
    // ... 初始化 DB ...
    
    // 获取当前逻辑时间戳
    hlcTime := myDB.Now()
    fmt.Printf("Current HLC Time: %d\n", hlcTime)
    
    // 获取 Clock 实例 (用于同步)
    clock := myDB.Clock()
    // clock.Update(remoteTimestamp)
}
```

### 垃圾回收机制详解 (Garbage Collection)

Yep CRDT 采用 **基于稳定时间戳 (Safe Timestamp)** 的垃圾回收机制，以防止 CRDT 元数据（墓碑）无限膨胀。

#### 核心概念：Safe Time
Safe Time 是一个时间点，系统保证在该时间点之前的所有操作都已同步到所有节点。通常，`SafeTime = min(All nodes' current HLC time) - max_network_delay`。

#### 工作原理
1.  **ORSet**: 遍历 `Tombstones`，物理删除 `DeletionTime < SafeTime` 的记录。
2.  **RGA**: 遍历链表，物理删除满足以下条件的节点：
    *   **已标记删除** (`Deleted == true`)
    *   **过期** (`DeletedAt < SafeTime`)
    *   **叶子节点** (无子节点的节点)，防止破坏树结构。

#### 使用建议
建议在上层应用中定期（如每分钟或每小时）计算集群的 `SafeTime` 并调用 `GC` 接口。

#### 离线节点处理策略 (Handling Offline Nodes)
如果在计算 SafeTime 时有一个节点长期离线，它会拖慢 `SafeTime` 的推进，导致无法有效 GC。建议采取以下策略：
1.  **超时剔除**: 设定阈值（如 24 小时）。若节点超时未发送心跳，计算 `SafeTime` 时将其排除。
2.  **强制重置**: 当离线节点重新上线时，如果它落后于当前的 `SafeTime`，系统应拒绝其增量同步请求，强制其清空本地状态并进行 **全量同步 (Full Sync)**，以防止“僵尸数据”复活。

## 性能最佳实践 (Performance Best Practices)

### 1. 遍历大型 RGA (Streaming Large Lists)
当 RGA 数据量较大（如超过 10,000 个元素）时，请避免使用 `.Value()` 方法，因为它会一次性分配巨大的内存切片。
推荐使用 `Query.FindCRDTs()` 获取原始对象，并结合 `.Iterator()` 进行零分配遍历。
**注意：`Query.FindCRDTs()` 返回的是只读接口 (比如：`ReadOnlyMap`、`ReadOnlyRGA`)，强制禁止修改，以防止误用和非持久化的变更。**

#### 只读接口概览

`Query.FindCRDTs()` 返回 `[]crdt.ReadOnlyMap`。该接口提供了类型安全的只读访问方法：

**ReadOnlyMap**:
- `Get(key string) (any, bool)`: 获取任意类型的值。
- `GetString(key string) (string, bool)`: 获取字符串值。
- `GetInt(key string) (int, bool)`: 获取整数值。
- `GetRGAString(key string) (ReadOnlyRGA[string], error)`: 获取只读的 RGA[string]。
- `GetRGABytes(key string) (ReadOnlyRGA[[]byte], error)`: 获取只读的 RGA[[]byte]。
- `GetSetString(key string) (ReadOnlySet[string], error)`: 获取只读的 ORSet[string]。
- `GetSetInt(key string) (ReadOnlySet[int], error)`: 获取只读的 ORSet[int]。

**ReadOnlyRGA[T]**:
- `Value() any`: 获取全量切片（慎用）。
- `Iterator() func() (T, bool)`: 获取迭代器，用于流式遍历。

**ReadOnlySet[T]**:
- `Value() any`: 获取全量切片。
- `Contains(element T) bool`: 检查元素是否存在。
- `Elements() []T`: 获取所有元素。

```go
// 1. 获取包含原始 CRDT 的结果集（不自动反序列化 Value）
// 返回 []crdt.ReadOnlyMap
crdts, _ := table.Where("id", db.OpEq, "doc1").FindCRDTs()

for _, doc := range crdts {
    // 2. 按需获取 RGA 实例 (只读)
    // 使用类型安全的方法获取 (例如 GetRGABytes 或 GetRGAString)
    rga, _ := doc.GetRGABytes("content")
    
    // 3. 使用 Iterator 流式遍历
    if rga != nil {
        iter := rga.Iterator()
        for {
            val, ok := iter()
            if !ok {
                break
            }
            // 处理 val (无需全量加载到内存)
        }
    }
}
```

### 2. MapCRDT 缓存
MapCRDT 内部实现了写回缓存 (Write-Back Cache)。
*   **读取**: 在 `Apply` 和 `Value` 操作中，CRDT 对象会保留在内存中，大幅提升连续操作的性能。
*   **写入**: 只有在调用 `.Bytes()` 或持久化时，才会触发序列化。

## 架构概览

*   **pkg/store**: 底层 KV 存储抽象 (BadgerDB 实现)。
*   **pkg/crdt**: 核心 CRDT 数据结构实现 (LWW, ORSet, PNCounter, RGA, Map)。
*   **pkg/meta**: 元数据和 Schema 管理 (Catalog)。
*   **pkg/index**: 索引编码和管理。
*   **pkg/db**: 顶层数据库 API，集成 Schema、Index 和 Storage，包含查询规划器。

## 待办事项

*   [ ] 实现 CRDT 状态的 P2P 同步逻辑 (Merkle Tree / Vector Clocks)。
*   [ ] 增加 HTTP/RPC 接口。
*   [ ] 增加 Mobile (Android/iOS) 绑定支持。

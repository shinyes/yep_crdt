# Yep CRDT 数据库使用手册

## 1. 简介

**Yep CRDT** (Conflict-free Replicated Data Type Database) 是一个高性能、本地优先 (Local-First) 的嵌入式数据库，专为构建离线可用、实时协作的应用程序而设计。它构建在 BadgerDB 之上，提供了强大的 CRDT 支持，确保在分布式和断网环境下的数据最终一致性。

### 核心特性

*   **本地优先 (Local-First)**: 数据存储在本地设备，应用程序随时可用，无需持续联网。
*   **CRDT 内核**: 内置多种 CRDT 类型，自动处理并发冲突，无需复杂的锁机制。
*   **SQL-Like 查询**: 提供流畅的链式 API 用于数据查询，支持索引、复杂条件筛选和排序。
*   **自动索引**: 基于 Schema 定义自动维护二级索引，查询速度快。
*   **类型安全**: 核心 API 支持泛型，减少运行时错误。
*   **高性能**: 底层使用 BadgerDB KV 存储，结合 LSM Tree 提供高吞吐量的读写。
*   **垃圾回收 (GC)**: 基于 HLC (Hybrid Logical Clock) 的无等待垃圾回收机制，有效防止元数据膨胀。

---

## 2. 安装

使用 Go Modules 安装：

```bash
go get github.com/shinyes/yep_crdt
```

确保你的 Go 版本 >= 1.20。

---

## 3. 快速开始

### 3.1 初始化数据库

Yep CRDT 需要一个本地目录来存储数据（基于 BadgerDB）。

```go
package main

import (
	"log"
	"os"

	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func main() {
	// 1. 确保存储路径存在
	dbPath := "./data/mydb"
	os.MkdirAll(dbPath, 0755)

	// 2. 初始化 BadgerDB 存储后端
	s, err := store.NewBadgerStore(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// 3. 打开数据库实例
	// Open 会启动后台服务（如 HLC 时钟）
	myDB := db.Open(s)
    
    // ... 后续操作
}
```

### 3.2 定义 Schema (表结构)

在使用表之前，必须定义其 Schema。Schema 决定了每一列的数据类型和 CRDT 类型，以及索引策略。

```go
import "github.com/shinyes/yep_crdt/pkg/meta"

// ...

err := myDB.DefineTable(&meta.TableSchema{
    Name: "products",
    Columns: []meta.ColumnSchema{
        // 1. LWW (Last-Write-Wins): 普通字段
        // 适用于用户名、标题、价格等不需要合并逻辑的字段。
        // 最后写入的值会覆盖旧值。
        {Name: "title", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
        {Name: "price", Type: meta.ColTypeInt, CrdtType: meta.CrdtLWW},
        
        // 2. Counter (PN-Counter): 计数器
        // 适用于点赞数、库存、浏览量。支持并发加减。
        {Name: "views", Type: meta.ColTypeInt, CrdtType: meta.CrdtCounter},
        
        // 3. ORSet (Observed-Remove Set): 集合
        // 适用于标签、分类、关注列表。支持并发添加/移除元素。
        {Name: "tags", Type: meta.ColTypeString, CrdtType: meta.CrdtORSet},
        
        // 4. RGA (Replicated Growable Array): 有序列表
        // 适用于文档内容、评论列表、待办事项。支持并发插入/排序。
        {Name: "comments", Type: meta.ColTypeString, CrdtType: meta.CrdtRGA},
    },
    Indexes: []meta.IndexSchema{
        // 简单索引
        {Name: "idx_price", Columns: []string{"price"}, Unique: false},
        // 复合索引 (支持多列组合查询)
        {Name: "idx_title_price", Columns: []string{"title", "price"}, Unique: false},
    },
})

if err != nil {
    log.Fatal("Failed to define table:", err)
}

// 获取表句柄用于后续操作
table := myDB.Table("products")
```

---

## 4. 数据操作

所有数据操作都需要通过 `Table` 对象进行。**注意：主键必须是 UUIDv7 格式**，以保证时间有序性和全局唯一性，这对分布式系统至关重要。

### 4.1 基础增删改 (LWW Register)

适用于 `LWW Register` 类型的字段（如上面的 `title`, `price`）。

```go
import "github.com/google/uuid"

// 生成 UUIDv7 主键
id, _ := uuid.NewV7()

// 插入或更新整行 (或部分 LWW 列)
// Set 接受 map[string]any，会覆盖指定的字段
err := table.Set(id, map[string]any{
    "title": "Go Programming Guide",
    "price": 59,
})
if err != nil {
    log.Fatal(err)
}
```

### 4.2 计数器操作 (PN Counter)

适用于 `PN Counter` 类型的字段。`PN Counter` 类型保证在并发环境下计数的准确性。

```go
// 增加浏览量 (+1)
table.Add(id, "views", 1)

// 增加多个 (+10)
table.Add(id, "views", 10)

// 减少浏览量 (两种方式)
table.Add(id, "views", -1)
// 或者
table.Remove(id, "views", 1)
```

### 4.3 集合操作 (ORSet)

适用于 `ORSet` 类型的字段。`ORSet` 允许并发添加和删除元素，且删除操作优先于添加操作（Observed-Remove）。

```go
// 添加标签
table.Add(id, "tags", "book")
table.Add(id, "tags", "coding")

// 移除标签
table.Remove(id, "tags", "book")

// 查询时，tags 集合将只包含 ["coding"]
```

### 4.4 有序列表操作 (RGA)

适用于 `RGA` 类型的字段。`RGA` 是专为协同编辑设计的有序序列，支持在任意位置插入和删除。

```go
// 1. 追加到末尾
table.Add(id, "comments", "First comment")
table.Add(id, "comments", "Second comment")
// 列表: ["First comment", "Second comment"]

// 2. 在指定元素后插入 (InsertAfter)
// 假设我们要插在 "First comment" 后面
table.InsertAfter(id, "comments", "First comment", "Reply to first")
// 列表: ["First comment", "Reply to first", "Second comment"]

// 3. 按索引位置插入 (InsertAt) - 0-based
// 插在最前面 (索引 0)
table.InsertAt(id, "comments", 0, "Top comment")
// 列表: ["Top comment", "First comment", "Reply to first", "Second comment"]

// 4. 按索引位置移除 (RemoveAt)
table.RemoveAt(id, "comments", 1) // 移除索引为 1 的元素 ("First comment")
```

> **注意**: `InsertAfter` 需要提供锚点元素的值。如果有多个相同值的元素，它会查找第一个未被删除的匹配项。

---

## 5. 数据查询 (Query Builder)

Yep CRDT 提供了类似 SQL 的 Fluent API 查询构建器。它会自动根据 `Where` 条件选择最佳索引（最长前缀匹配算法）。

### 5.1 简单查询 (Get)

```go
// 根据 ID 获取单行数据
row, err := table.Get(id)
// row 是 map[string]any 类型
if row != nil {
    fmt.Println("Title:", row["title"])
}
```

### 5.2 条件查询 (Where/And)

支持的运算符：
*   `db.OpEq` (`=`): 等于
*   `db.OpNe` (`!=`): 不等于
*   `db.OpGt` (`>`): 大于
*   `db.OpGte` (`>=`): 大于等于
*   `db.OpLt` (`<`): 小于
*   `db.OpLte` (`<=`): 小于等于
*   `db.OpIn` (`IN`): 包含于数组

```go
// 查询价格 > 50 的书
results, err := table.Where("price", db.OpGt, 50).Find()

// 组合条件 (AND)
// 查询价格 > 50 且 浏览量 < 1000 的书
// 这将自动利用复合索引 `idx_title_price` (如果条件匹配索引前缀) 或者回退到表扫描
results, err := table.Where("price", db.OpGt, 50).
                      And("views", db.OpLt, 1000).
                      Find()

// IN 查询
results, err := table.Where("title", db.OpIn, []any{"Go Guide", "Rust Guide"}).Find()
```

### 5.3 排序与分页 (OrderBy/Limit/Offset)

```go
results, err := table.Where("price", db.OpGt, 0).
                      OrderBy("price", true). // true = 降序 (DESC), false = 升序 (ASC)
                      Offset(10).             // 跳过前 10 条 (Pagination)
                      Limit(5).               // 最多取 5 条
                      Find()
```

### 5.4 高级查询：获取 CRDT 对象 (FindCRDTs)

默认的 `Find()` 方法返回 `[]map[string]any`，即数据的 JSON 友好快照。如果你需要遍历大型 RGA 列表，或者需要高性能访问，可以使用 `FindCRDTs()`。

`FindCRDTs()` 返回 `[]crdt.ReadOnlyMap` 接口，允许你在不反序列化整个对象的情况下访问部分数据，但是是只读的。

```go
// 返回 []crdt.ReadOnlyMap
docs, err := table.Where("id", db.OpEq, id).FindCRDTs()

for _, doc := range docs {
    // 1. 高效获取 LWW 字段
    if title, ok := doc.GetString("title"); ok {
        fmt.Println("Title:", title)
    }

    // 2. 高效获取 RGA 迭代器，避免一次性加载整个切片
    // 这对于含有成千上万条记录的列表非常有用
    rga, _ := doc.GetRGAString("comments")
    if rga != nil {
        iter := rga.Iterator()
        for {
            val, ok := iter() // 每次调用返回下一个元素
            if !ok { break }
            fmt.Println("Comment:", val)
        }
    }
}
```

---

## 6. 事务 (Transactions)

Yep CRDT 支持 ACID 事务。你可以将多个操作打包在一个事务中原子执行。这在需要同时更新多个表或多行数据时非常重要。

```go
err := myDB.Update(func(tx *db.Tx) error {
    t := tx.Table("products")

    // 步骤 1: 修改产品 A 的浏览量
    if err := t.Add(idA, "views", 1); err != nil {
        return err // 返回错误将导致整个事务回滚
    }

    // 步骤 2: 修改产品 B 的价格
    if err := t.Set(idB, map[string]any{"price": 100}); err != nil {
        return err
    }

    // 如果返回 nil，事务自动提交
    return nil
})
```

只读事务可以使用 `myDB.View(...)`，通常比 `Update` 稍微快一些且不占用写锁。

---

## 7. 分布式与同步 (Preview)

Yep CRDT 设计为去中心化架构的基础。

*   **Node Identity**: 每个数据库实例都有一个唯一的 `myDB.NodeID`。
*   **HLC (Hybrid Logical Clock)**: 数据库内部维护一个混合逻辑时钟 `myDB.Clock()`，用于生成单调递增且具有因果关系的时间戳。
*   **Sync**: 虽然目前的 API 集中在单机操作，但底层数据结构 (`MapCRDT`, `RGA` 等) 均支持 `Merge` 操作。可以通过导出/导入数据块或操作日志来实现多节点间的同步。

---

## 8. 附录：数据类型参考表

| Schema 类型 | 对应的 Go 类型 | 描述 |
| :--- | :--- | :--- |
| `ColTypeString` | `string` | 文本字符串 |
| `ColTypeInt` | `int`, `int64` | 整数 |
| `ColTypeBool` | `bool` | 布尔值 |

| CRDT 类型 | 操作方法 | 适用场景 |
| :--- | :--- | :--- |
| **CrdtLWW** | `Set` | 用户名、状态、配置项 (Last-Write-Wins) |
| **CrdtCounter** | `Add`, `Remove` | 计数器、统计数据 (PN-Counter) |
| **CrdtORSet** | `Add`, `Remove` | 标签、分类、ID 集合 (Observed-Remove Set) |
| **CrdtRGA** | `Add`, `InsertAt`, `InsertAfter`, `Remove`, `RemoveAt` | 文本编辑、即时通讯消息流、任务列表 (RGA) |

---

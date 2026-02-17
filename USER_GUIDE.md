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
*   **垃圾回收 (GC)**: 支持数据库级与表级 GC，并提供多节点手动协商 GC 能力（prepare/commit/execute）与 `gc_floor` 安全栅栏。

---

## 2. 安装

使用 Go Modules 安装：

```bash
go get github.com/shinyes/yep_crdt
```

建议使用与仓库 `go.mod` 一致的 Go 版本（当前为 `1.25.5`）。

---

## 3. 快速开始

### 3.1 初始化数据库

Yep CRDT 需要一个本地目录来存储数据（基于 BadgerDB）。

```go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/store"
	"github.com/shinyes/yep_crdt/pkg/sync"
)

func main() {
	// 1. 确保存储路径存在
	dbPath := "./data/mydb"
	os.MkdirAll(dbPath, 0755)

	// 2. 初始化 BadgerDB 存储后端
	s, err := store.NewBadgerStore(
		dbPath,
		store.WithBadgerValueLogFileSize(256*1024*1024), // 256MB vlog 文件
	)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// 3. 打开数据库实例
	// Open 会启动后台服务（如 HLC 时钟），并校验 DatabaseID
	myDB, err := db.Open(s, "my-database-id")
	if err != nil {
		log.Fatal(err)
	}
	defer myDB.Close()

	// (可选) 设置文件存储根目录，用于 LocalFileCRDT
	myDB.SetFileStorageDir("./data/files")

	// 4. [NEW] 开启自动同步
	// 仅需一行代码，即可加入 P2P 网络
	engine, err := sync.EnableMultiTenantSync([]*db.DB{myDB}, db.SyncConfig{
		ListenPort: 8001,       // 本地监听端口 (0 表示随机)
		ConnectTo:  "192.168.1.100:8001", // (可选) 初始连接节点
		Password:   "my-secret-password", // 集群密码
	})
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("Node ID: %s running at %s\n", engine.LocalID(), engine.LocalAddr())
    
    // ... 后续操作
}
```

### 3.2 定义 Schema (表结构)

在使用表之前，必须定义其 Schema。

`DefineTable` 的主要作用是 **注册表结构 (Schema)**，它的行为类似于 **"Create If Not Exists" (如果不存在则创建)**。

1. **如果表不存在**：它会在 Catalog 中创建新表，分配 Table ID 并持久化。
2. **如果表已存在且 schema 等价**：直接返回成功，确保操作幂等。
3. **如果表已存在但 schema 冲突**：返回错误（例如列类型、CRDT 类型或索引定义不一致）。

Schema 决定了每一列的数据类型和 CRDT 类型，以及索引策略。

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

        // 5. LocalFile (本地文件): 引用外部文件
        // 适用于图片、附件、大文件。只存储元数据，支持按需读取内容。
        {Name: "avatar", Type: meta.ColTypeString, CrdtType: meta.CrdtLocalFile},
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

### 4.5 本地文件操作 (LocalFile)

LocalFileCRDT 仅存储文件的元数据（Path, Size, Hash），实际文件存储在本地文件系统。


```go
// 插入本地文件 (自动导入)
// 数据库会自动将 /tmp/img.png 复制到 <FileStorageDir>/images/avatar.png
// 并自动计算哈希和文件大小
err = table.Set(id, map[string]any{
    "avatar": db.FileImport{
        LocalPath:    "/tmp/img.png",
        RelativePath: "images/avatar.png",
    },
})

// 读取文件 (必须使用 FindCRDTs 获取强类型对象)
rows, _ := table.Where("id", db.OpEq, id).FindCRDTs()
for _, row := range rows {
    if file, err := row.GetLocalFile("avatar"); err == nil {
        // 读取全部内容 -> []byte
        content, _ := file.ReadAll()
        
        // 随机读取 (offset, length) -> []byte
        header, _ := file.ReadAt(0, 100)
        
        // 获取元数据
        meta := file.Value().(crdt.FileMetadata)
        fmt.Println("File Size:", meta.Size)
    }
}
```

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

### 5.1.1 获取单行原始 CRDT (GetCRDT)

如果你需要访问单行的原始 CRDT 结构（例如为了获取 RGA 的迭代器），即可以使用 `GetCRDT`。它返回 `crdt.ReadOnlyMap` 接口。

```go
// 返回 crdt.ReadOnlyMap
crdtRow, err := table.GetCRDT(id)
if err == nil && crdtRow != nil {
    // 使用只读接口安全访问嵌套 CRDT
    if rSet, _ := crdtRow.GetSetString("tags"); rSet != nil {
        fmt.Println("Tags:", rSet.Elements())
    }
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

## 6. 垃圾回收 (Garbage Collection)

Yep CRDT 通过 GC 清理 CRDT 操作产生的墓碑（Tombstone），防止元数据持续膨胀。

当前版本的同步层不再内置自动 GC 调度器；推荐在业务侧显式触发 GC。多节点场景下，推荐使用同步模块提供的“手动协商 GC”。

### 6.1 核心概念

#### Safe Timestamp
**Safe Timestamp** 表示“在该时间点之前的数据可以安全清理墓碑”。

在同步模块的手动协商流程中：

- 每个参与节点先计算自己的 `safeTimestamp`
- 协调者取所有节点返回值的最小值，作为本轮最终 `safeTimestamp`
- `commit` 阶段只做可执行确认，不执行 GC
- `execute` 阶段才真正执行 GC

#### 为什么需要 GC？
在 CRDT 系统中，删除操作不会立即删除数据，而是标记为已删除（Tombstone）。这确保了：
- **分布式一致性**：即使网络延迟或分区，所有节点都能正确处理删除
- **无冲突合并**：延迟删除的元素不会被错误地恢复

但这也导致：
- **内存增长**：Tombstones 持续积累，占用内存
- **存储膨胀**：序列化数据包含所有墓碑，增加存储成本

因此需要定期执行 GC 来清理过期的墓碑。

### 6.2 数据库级别的 GC API

`db.DB` 提供数据库级统一 GC 接口，可一次性清理所有表中的墓碑。

```go
import "time"

// 获取当前时间戳
currentTime := myDB.Now()

// 计算 safeTimestamp（例如：5 秒前的数据可以安全清理）
safeTimestamp := currentTime - 5000 // 5000 毫秒 = 5 秒

// 执行 GC
result := myDB.GC(safeTimestamp)

// 检查结果
fmt.Printf("扫描表数量: %d\n", result.TablesScanned)
fmt.Printf("扫描行数量: %d\n", result.RowsScanned)
fmt.Printf("清理的墓碑数量: %d\n", result.TombstonesRemoved)

if len(result.Errors) > 0 {
    for _, err := range result.Errors {
        log.Printf("GC 错误: %v\n", err)
    }
}
```

#### GCByTimeOffset（推荐）

更方便的方法是 `GCByTimeOffset`，由数据库根据偏移量自动计算 `safeTimestamp`。

```go
// 清理 1 分钟前的数据
result := myDB.GCByTimeOffset(1 * time.Minute)

fmt.Printf("清理了 %d 个墓碑\n", result.TombstonesRemoved)
```

### 6.3 表级别的 GC API

如果只需要清理特定表的墓碑，可以使用表级别的 GC。

```go
table := myDB.Table("users")

// 执行表级 GC
result := table.GC(safeTimestamp)

fmt.Printf("扫描行: %d\n", result.RowsScanned)
fmt.Printf("清理墓碑: %d\n", result.TombstonesRemoved)
```

### 6.4 多节点手动协商 GC（推荐）

同步层提供 `prepare -> commit -> execute` 协商流程（失败时会发送 `abort` 清理 pending 状态），建议通过 `LocalNode` 或 `MultiEngine` 触发。

```go
// LocalNode 入口（推荐）
gcResult, err := node.ManualGCTenant("tenant-1", 15*time.Second)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("safeTs=%d prepared=%d committed=%d executed=%d localRemoved=%d\n",
    gcResult.SafeTimestamp,
    len(gcResult.PreparedPeers),
    len(gcResult.CommittedPeers),
    len(gcResult.ExecutedPeers),
    gcResult.LocalResult.TombstonesRemoved,
)
```

```go
// MultiEngine 入口
gcResult, err := engine.ManualGC("tenant-1", 15*time.Second)
if err != nil {
    log.Fatal(err)
}
_ = gcResult
```

协商语义：

- `prepare`：收集参与节点 `safeTimestamp`
- 协调者取最小值
- `commit`：向参与节点下发最终 `safeTimestamp`，仅确认可执行
- `execute`：在所有参与节点确认后执行 GC
- `abort`：任一阶段失败时清理远端 pending 状态（尽力而为）

实现备注：

- `timeout <= 0` 时使用默认超时（当前实现为 10 秒）
- 若协商过程中节点拒绝或超时，调用会返回错误
- 建议在业务侧将触发动作做成运维命令或定时任务（显式触发，而非隐式后台）

### 6.5 GC 结果统计

GC 操作返回详细的结果统计，便于监控和调试。

```go
type GCResult struct {
    TablesScanned     int      // 扫描的表数量
    RowsScanned       int      // 扫描的行数量
    TombstonesRemoved int      // 移除的墓碑数量
    Errors           []error  // 遇到的错误列表
}

type TableGCResult struct {
    RowsScanned       int      // 扫描的行数量
    TombstonesRemoved int      // 移除的墓碑数量
    Errors           []error  // 遇到的错误列表
}
```

### 6.6 注意事项

#### Safe Timestamp 的计算
- **保守为上**：宁可保留更多墓碑，也不要过早清理
- **网络分区**：考虑节点长期离线的情况，`safeTimestamp` 可能被拖慢
- **显式触发**：生产环境建议由业务侧统一调度 GC，不要隐式自动执行

#### 性能影响
- **扫描开销**：GC 会扫描所有行，可能影响性能
- **低峰期执行**：建议在系统负载低时执行 GC
- **批量处理**：GC 使用事务批量更新，减少 I/O 开销

#### 错误处理
- **不中断执行**：单个行的错误不会中断整个 GC 操作
- **收集错误**：所有错误都被收集在结果中，便于后续分析

---

## 7. 事务 (Transactions)

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

## 7. 分布式与同步 (Distributed & Sync)

Yep CRDT 内置了强大的分布式同步引擎 (`pkg/sync`)，旨在让构建“本地优先 (Local-First)”应用变得极其简单。

### 7.1 核心架构

Yep CRDT 的同步层基于 TCP 长连接和 Gossip 协议（部分理念），实现了全自动的 P2P 同步。

*   **Node Identity (节点身份)**: 每个数据库实例在初始化时会生成一个基于 UUIDv7 的唯一 `NodeID`，并持久化存储。
*   **Version Vector (版本向量)**: 系统使用混合逻辑时钟 (HLC) 和版本向量来跟踪数据的因果关系，精确识别哪些数据需要同步。
*   **Automatic Discovery (自动发现)**: 节点间通过 TCP 互联后，会自动交换节点列表，形成网状拓扑。

### 7.2 启用同步

使用 `sync.EnableMultiTenantSync` 一键启用同步。不需要额外部署中心化服务器（如 Redis 或 Kafka）。

```go
import "github.com/shinyes/yep_crdt/pkg/sync"

engine, err := sync.EnableMultiTenantSync([]*db.DB{myDB}, db.SyncConfig{
    // 网络配置
    ListenPort: 8080,        // 本地监听端口
    ConnectTo:  "seed-node:8080", // 种子节点地址
    
    // 安全配置
    Password: "cluster-secret", // 预共享密钥 (PSK) 防止未授权访问
    
    // 调试
    Debug: true, // 打印详细的同步日志
})
```

如需增量优先模式（关闭重连时自动全量触发），可传入 `nodeOpts`：

```go
engine, err := sync.EnableMultiTenantSync(
    []*db.DB{myDB},
    db.SyncConfig{
        ListenPort: 8080,
        Password:   "cluster-secret",
    },
    sync.WithTimeoutThreshold(0),
    sync.WithClockThreshold(0),
)
```

使用 `sync.StartLocalNode(...)` 时，也可通过 `LocalNodeOptions{IncrementalOnly: true}` 达到相同效果。

### 7.3 同步机制原理

Yep CRDT 实现了三种层级的同步机制，自动智能切换：

#### 1. 实时广播 (Real-time Broadcast)
当本地发生数据写入 (`table.Set`, `table.Add` 等) 时，变更操作会被立即封装并通过长连接广播给所有活跃的对等节点。
*   **延迟**: 毫秒级
*   **触发**: 写入即触发

#### 2. 增量同步 (Incremental Sync)
当两个节点建立连接（或重连）时，它们会交换各自的 **Version Vector (版本向量)**。
*   **流程**: 节点 A 告诉节点 B：“我拥有 Node X 的数据直到时间 T1”。节点 B 计算差异，只发送 A 缺失的数据补丁。
*   **优势**: 极大节省带宽，仅传输断连期间产生的差异数据。

#### 3. 差异检测 (Anti-Entropy / Full Sync)
为了处理极端情况（如完全陌生节点加入），系统支持基于 Merkle Tree 思想的表级指纹 (Table Digest) 比对。
*   **流程**: 定期（或在握手时）比对表的 Hash 摘要。如果发现不一致，则拉取差异数据。

### 7.4 冲突解决

基于 CRDT (Conflict-free Replicated Data Types) 数学理论，Yep CRDT 保证所有副本的数据 **最终强一致 (Strong Eventual Consistency)**。

*   **LWW (Last-Write-Wins)**: 基于 HLC 时间戳，时间戳大的覆盖小的。HLC 保证了即使在物理时钟略有偏差的情况下，因果关系也能被正确保留。
*   **ORSet / RGA / Counter**: 通过保留操作历史或元数据，自动合并并发修改，不会丢失数据。

### 7.5 网络拓扑与防火墙

*   **P2P Mesh**: 节点间形成网状结构，不需要所有节点两两互联。消息可以通过中间节点转发（Gossip 传播）。
*   **穿透性**: 目前主要支持直连。如果在 NAT 后，需要确保端口映射或使用 VPN/Overlay 网络（如 Tailscale）。

---

## 8. 附录：数据类型参考表

| Schema 类型 | 对应的 Go 类型 | 描述 |
| :--- | :--- | :--- |
| `ColTypeString` | `string` | 文本字符串 |
| `ColTypeInt` | `int`, `int64` | 整数（值本体约 `1~20` 字节，含负号） |
| `ColTypeBool` | `bool` | 布尔值 |
| `ColTypeFloat` | `float32`, `float64` | 浮点数（值本体通常约 `1~24` 字节） |
| `ColTypeBytes` | `[]byte` | 二进制数据（BLOB） |
| `ColTypeTimestamp` | `time.Time`, RFC3339 字符串, Unix 时间戳 | 时间戳 |

说明：`Table.Get()` 与 `Query.Find()` 会按 schema 返回类型化值，不再要求业务侧手动从字符串/字节做二次解析。

| CRDT 类型 | 操作方法 | 适用场景 |
| :--- | :--- | :--- |
| **CrdtLWW** | `Set` | 用户名、状态、配置项 (Last-Write-Wins) |
| **CrdtCounter** | `Add`, `Remove` | 计数器、统计数据 (PN-Counter) |
| **CrdtORSet** | `Add`, `Remove` | 标签、分类、ID 集合 (Observed-Remove Set) |
| **CrdtRGA** | `Add`, `InsertAt`, `InsertAfter`, `Remove`, `RemoveAt` | 文本编辑、即时通讯消息流、任务列表 (RGA) |
| **LocalFile** | `Set` (via `FileMetadata`), `ReadAll`, `ReadAt` (read-only) | 图片、附件、大文件引用 |

---

## 9. 同步模块更新（2026-02）

### 9.1 列级 Delta 同步

- 写入成功后，优先广播 `raw_delta`（包含列名 `columns`）。
- 当列级载荷构建失败时，自动回退为整行 `raw_data`，确保同步不中断。

### 9.2 全量同步流控配置

`sync.TenetConfig` 新增：

- `FetchResponseBuffer`：fetch 响应缓冲容量（默认 `256`）
- `FetchResponseIdleTimeout`：无 done marker 时的空闲等待（默认 `1s`）

```go
network, err := sync.NewTenantNetwork("tenant-1", &sync.TenetConfig{
    Password:                 "cluster-secret",
    ListenPort:               9001,
    FetchResponseBuffer:      1024,
    FetchResponseIdleTimeout: 3 * time.Second,
})
if err != nil {
    log.Fatal(err)
}
_ = network
```

### 9.3 运行时统计（可观测性）

`MultiEngine` 提供 `TenantStats(tenantID)` 快照：

- 引擎侧：`ChangeEnqueued` / `ChangeProcessed` / `ChangeBackpressure` / `ChangeQueueDepth`
- 网络侧：`FetchRequests` / `FetchSuccess` / `FetchTimeouts` / `FetchPartialTimeouts` / `FetchOverflows` / `DroppedResponses`

```go
stats, ok := engine.TenantStats("tenant-1")
if !ok {
    log.Fatal("tenant not started")
}

fmt.Printf("sync queue depth=%d, enqueued=%d, processed=%d, backpressure=%d\n",
    stats.ChangeQueueDepth,
    stats.ChangeEnqueued,
    stats.ChangeProcessed,
    stats.ChangeBackpressure,
)

fmt.Printf("fetch req=%d ok=%d timeout=%d partial_timeout=%d overflow=%d dropped=%d\n",
    stats.Network.FetchRequests,
    stats.Network.FetchSuccess,
    stats.Network.FetchTimeouts,
    stats.Network.FetchPartialTimeouts,
    stats.Network.FetchOverflows,
    stats.Network.DroppedResponses,
)
```

### 9.4 错误分类与恢复策略

`pkg/sync/sync_errors.go` 提供可用于 `errors.Is` 的错误类型：

- `sync.ErrTimeoutWaitingResponse`
- `sync.ErrTimeoutWaitingResponseCompletion`
- `sync.ErrResponseOverflow`
- `sync.ErrPeerDisconnectedBeforeResponse`
- `sync.ErrResponseChannelClosed`

```go
rows, err := nodeMgr.FetchRawTableData(peerID, "users")
if err != nil {
    switch {
    case errors.Is(err, sync.ErrTimeoutWaitingResponse):
        // 直接超时，建议重试
    case errors.Is(err, sync.ErrTimeoutWaitingResponseCompletion):
        // 已收到部分数据，建议重试并打告警
    case errors.Is(err, sync.ErrResponseOverflow):
        // 适当增大 FetchResponseBuffer 或降低并发
    case errors.Is(err, sync.ErrPeerDisconnectedBeforeResponse):
        // 等待重连后重试
    }
}
_ = rows
```

### 9.5 手动协商 GC（替代旧自动 GC）

同步层已移除旧的自动 GC 组件（`GCManager`、`GCInterval`、`GCTimeOffset` 配置项）。

当前推荐通过以下 API 触发分布式 GC：

- `node.ManualGCTenant(tenantID, timeout)`
- `engine.ManualGC(tenantID, timeout)`

执行流程为 `prepare -> commit -> execute` 三阶段（失败时带 `abort`）：

1. 协调者向在线节点发送 `gc_prepare`，收集各节点 `safeTimestamp`
2. 协调者取最小值作为最终 `safeTimestamp`
3. 协调者发送 `gc_commit`，参与节点仅确认可执行
4. 协调者发送 `gc_execute`，参与节点执行 `db.GC(safeTimestamp)`
5. 任一阶段失败时发送 `gc_abort` 清理远端 pending 状态（尽力而为）

```go
gcResult, err := node.ManualGCTenant("tenant-1", 15*time.Second)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("safeTs=%d prepared=%d committed=%d executed=%d removed=%d\n",
    gcResult.SafeTimestamp,
    len(gcResult.PreparedPeers),
    len(gcResult.CommittedPeers),
    len(gcResult.ExecutedPeers),
    gcResult.LocalResult.TombstonesRemoved,
)
```

### 9.6 `gc_floor` 安全栅栏（新增）

每个节点持久化本地 `gc_floor`（已成功执行 GC 的最大 `safeTimestamp`），并在心跳/控制消息里传播。

- 若 `peer.gc_floor != local.gc_floor`，该 peer 的增量同步会被阻断（发送与接收都按 peer 门控）
- 若 `peer.gc_floor > local.gc_floor`，本地会自动从该 peer 触发 full sync 追平
- full sync 成功后本地提升 `gc_floor`，恢复与该 peer 的增量同步
- 该机制用于防止“落后节点误参与增量同步导致缺历史上下文”的风险

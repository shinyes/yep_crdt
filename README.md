# Yep CRDT

Yep CRDT 是一个基于 Go 语言实现的 CRDT (Conflict-free Replicated Data Type) 库，旨在为分布式系统提供高可用性和强最终一致性的数据同步解决方案。

## 功能特性

- **多种 CRDT 类型支持**：
  - `GCounter`: 增长计数器（只增）
  - `PNCounter`: 正负计数器（支持增减）
  - `ORSet`: 观察-移除集合 (Observed-Remove Set)
  - `RGA`: 复制可增长数组 (Replicated Growable Array)，用于序列/文本编辑
  - `LWWRegister`: 最后写入胜出寄存器
  - `Map`: 嵌套映射支持
  - `ImmutableFile`: 不可变文件同步
- **持久化存储**：集成 BadgerDB 进行数据持久化。
- **Blob 存储**：支持大文件（Blob）的存储与检索。
- **同步机制**：支持基于 Delta 的状态同步。
- **流式 Query API**：类似 ORM 的流畅查询接口。
- **Mobile 支持**：通过 gomobile 支持 Android/iOS 平台。
- **多节点支持**：可配置节点 ID，支持多节点同步。

## 快速开始

### 安装

```bash
go get github.com/shinyes/yep_crdt
```

### 基本示例：GCounter

```go
package main

import (
    "fmt"
    "github.com/shinyes/yep_crdt/crdt"
)

func main() {
    // 创建两个副本的计数器
    c1 := crdt.NewGCounter("node1")
    c2 := crdt.NewGCounter("node2")

    // Node 1 增加 10
    c1.Apply(crdt.GCounterOp{OriginID: "node1", Amount: 10})

    // Node 2 增加 5
    c2.Apply(crdt.GCounterOp{OriginID: "node2", Amount: 5})

    // 合并状态 (也就是同步)
    c1.Merge(c2.State())
    c2.Merge(c1.State())

    fmt.Printf("Node 1 Value: %v\n", c1.Value()) // Output: 15
    fmt.Printf("Node 2 Value: %v\n", c2.Value()) // Output: 15
}
```

### PNCounter 示例（支持增减）

```go
pn1 := crdt.NewPNCounter("node1")
pn2 := crdt.NewPNCounter("node2")

// Node 1: +10, -3
pn1.Apply(crdt.PNCounterOp{OriginID: "node1", Amount: 10})
pn1.Apply(crdt.PNCounterOp{OriginID: "node1", Amount: -3})

// Node 2: +5, -2
pn2.Apply(crdt.PNCounterOp{OriginID: "node2", Amount: 5})
pn2.Apply(crdt.PNCounterOp{OriginID: "node2", Amount: -2})

// 双向合并后两者一致
pn1.Merge(pn2.State())
pn2.Merge(pn1.State())

fmt.Printf("Value: %v\n", pn1.Value()) // 10
```

### Query API 示例

```go
// 创建带节点 ID 的 Manager
m, _ := manager.NewManagerWithNodeID("./db", "./blobs", "node-A")
defer m.Close()

// 创建 Map 根节点
m.CreateRoot("user_profile", crdt.TypeMap)

// 使用流式 API 设置值
m.From("user_profile").Update().
    Set("name", "Alice").
    Set("age", 30).
    Set("settings.theme", "dark").
    Commit()

// 查询单个值
name, _ := m.From("user_profile").Select("name").Get()

// 增加计数器
m.From("user_profile").Update().Inc("login_count", 1).Commit()

// 集合操作
m.From("user_profile").Update().
    AddToSet("tags", "premium").
    AddToSet("tags", "verified").
    Commit()

// 序列操作
m.From("user_profile").Update().
    Append("history", "action1").
    Append("history", "action2").
    Commit()

// 获取序列（带元素 ID）
elems, _ := m.From("user_profile").GetSequence("history")
for _, e := range elems {
    fmt.Printf("ID: %s, Value: %v\n", e.ID, e.Value)
}
```

### 根节点管理

```go
// 列出所有根节点
roots, _ := m.ListRoots()
for _, r := range roots {
    fmt.Printf("ID: %s, Type: %v\n", r.ID, r.Type)
}

// 检查是否存在
if m.Exists("user_profile") {
    // ...
}

// 删除根节点
m.DeleteRoot("old_data")

// 手动保存快照
m.TriggerSnapshot("user_profile")
```

### Mobile 示例 (Android/iOS)

```go
// 创建带节点 ID 的 MobileManager
mm, _ := mobile.NewMobileManagerWithNodeID("./db", "./blobs", "device-123")
defer mm.Close()

// 创建根节点
mm.CreateMapRoot("user")
mm.CreateCounterRoot("stats")
mm.CreateSequenceRoot("messages")

// 设置/获取值
mm.From("user").SetString("name", "Alice")
name, _ := mm.From("user").GetString("name")

// 序列操作
mm.From("messages").Append("messages", "Hello")
mm.From("messages").Append("messages", "World")
json, _ := mm.From("messages").GetSequenceAsJSON("messages")

// 根节点管理
rootsJSON, _ := mm.ListRootsAsJSON()
mm.DeleteRoot("old_data")
exists := mm.Exists("user")
```

## 目录结构

- `crdt/`: CRDT 核心实现
- `manager/`: 数据管理与持久化层
- `store/`: 存储接口与实现
- `sync/`: 同步协议相关
- `mobile/`: 移动端 (Android/iOS) 绑定支持

## API 参考

### CRDT 类型

| 类型 | 描述 | 创建方法 |
|------|------|----------|
| GCounter | 只增计数器 | `crdt.NewGCounter(id)` |
| PNCounter | 正负计数器 | `crdt.NewPNCounter(id)` |
| ORSet | 观察-移除集合 | `crdt.NewORSet()` |
| RGA | 复制可增长数组 | `crdt.NewRGA()` |
| LWWRegister | 最后写入胜出寄存器 | `crdt.NewLWWRegister(val, ts)` |
| MapCRDT | 嵌套映射 | `crdt.NewMapCRDT()` |

### Manager API

| 方法 | 描述 |
|------|------|
| `NewManager(dbPath, blobPath)` | 创建 Manager |
| `NewManagerWithNodeID(dbPath, blobPath, nodeID)` | 创建带节点 ID 的 Manager |
| `CreateRoot(id, type)` | 创建根节点 |
| `GetRoot(id)` | 获取根节点 |
| `ListRoots()` | 列出所有根节点 |
| `DeleteRoot(id)` | 删除根节点 |
| `Exists(id)` | 检查根节点是否存在 |
| `TriggerSnapshot(id)` | 手动保存快照 |

### Query API 方法

| 方法 | 描述 |
|------|------|
| `From(rootID).Select(paths...).Get()` | 查询指定路径的值 |
| `From(rootID).Update().Set(path, val).Commit()` | 设置值 |
| `From(rootID).Update().Inc(path, amount).Commit()` | 增加计数器 |
| `From(rootID).Update().Delete(path).Commit()` | 删除键 |
| `From(rootID).Update().AddToSet(path, val).Commit()` | 向集合添加元素 |
| `From(rootID).Update().RemoveFromSet(path, val).Commit()` | 从集合移除元素 |
| `From(rootID).Update().Append(path, val).Commit()` | 在序列末尾追加 |
| `From(rootID).Update().InsertAt(path, prevID, val).Commit()` | 在序列指定位置插入 |
| `From(rootID).Update().RemoveAt(path, elemID).Commit()` | 从序列移除元素 |
| `From(rootID).GetSequence(path)` | 获取序列（带元素 ID）|
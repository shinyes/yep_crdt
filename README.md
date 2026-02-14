# Yep CRDT Database

Yep CRDT 是一个本地优先（Local-First）的嵌入式 CRDT 数据库，构建在 BadgerDB 之上，提供：

- 多 CRDT 类型（LWW / ORSet / Counter / RGA / LocalFile）
- SQL-Like 链式查询（Where / And / Limit / Offset / OrderBy / IN）
- 自动索引与查询规划（Longest Prefix Match）
- 分布式自动同步（增量、全量、重连）
- 面向多节点场景的 GC 与可观测性

完整文档请看 `USER_GUIDE.md`。

## 目录

- [安装](#安装)
- [快速开始](#快速开始)
- [CRDT 列类型](#crdt-列类型)
- [查询示例](#查询示例)
- [分布式同步](#分布式同步)
- [垃圾回收](#垃圾回收)
- [性能建议](#性能建议)
- [项目结构](#项目结构)
- [待办事项](#待办事项)

## 安装

```bash
go get github.com/shinyes/yep_crdt
```

## 快速开始

### 1) 初始化数据库

```go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
	"github.com/shinyes/yep_crdt/pkg/sync"
)

func main() {
	dbPath := "./tmp/my_db"
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		log.Fatal(err)
	}

	s, err := store.NewBadgerStore(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	myDB := db.Open(s, "tenant-1")
	defer myDB.Close()
	myDB.SetFileStorageDir("./data/files")

	if err := myDB.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "views", Type: meta.ColTypeInt, CrdtType: meta.CrdtCounter},
			{Name: "tags", Type: meta.ColTypeString, CrdtType: meta.CrdtORSet},
		},
	}); err != nil {
		log.Fatal(err)
	}

	engine, err := sync.EnableSync(myDB, db.SyncConfig{
		ListenPort: 8001,
		ConnectTo:  "127.0.0.1:8002", // 可选
		Password:   "secret",          // 必填
		Debug:      false,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("node=%s addr=%s\n", engine.LocalID(), engine.LocalAddr())

	table := myDB.Table("users")
	id, _ := uuid.NewV7()

	// LWW 写入
	if err := table.Set(id, map[string]any{"name": "Alice"}); err != nil {
		log.Fatal(err)
	}

	// Counter / ORSet 操作
	_ = table.Add(id, "views", 1)
	_ = table.Add(id, "tags", "developer")
}
```

### 2) 事务写入

```go
err := myDB.Update(func(tx *db.Tx) error {
	t := tx.Table("users")
	if err := t.Add(id1, "views", 100); err != nil {
		return err
	}
	return t.Add(id2, "views", 100)
})
```

## CRDT 列类型

| CRDT 类型 | 典型方法 | 场景 |
| :--- | :--- | :--- |
| `CrdtLWW` | `Set` | 普通字段（昵称、状态、标题） |
| `CrdtCounter` | `Add` / `Remove` | 点赞数、浏览量、库存 |
| `CrdtORSet` | `Add` / `Remove` | 标签、分类、成员集合 |
| `CrdtRGA` | `Add` / `InsertAt` / `InsertAfter` / `RemoveAt` | 有序列表、协作文档片段 |
| `CrdtLocalFile` | `Set`(via `db.FileImport`) / `ReadAll` / `ReadAt` | 图片、附件、大文件引用 |

说明：主键要求 UUIDv7，以保证时间有序和全局唯一。

## 查询示例

```go
u, _ := table.Get(userID)
fmt.Println(u)

rows, err := table.Where("name", db.OpEq, "Alice").
	And("views", db.OpGt, 10).
	OrderBy("views", true).
	Offset(0).
	Limit(20).
	Find()

rows2, err := table.Where("views", db.OpIn, []any{100, 200, 300}).Find()
_ = rows
_ = rows2
_ = err
```

如果要高性能读取复杂 CRDT（尤其大 RGA），优先用 `FindCRDTs()` + `Iterator()` 流式遍历，避免一次性 `.Value()` 带来的大内存分配。

## 分布式同步

### 基础能力

- 自动广播本地变更
- 自动重连
- 增量同步 + 全量同步补齐
- HLC 驱动的因果一致时序

### 2026-02 更新

#### 1) 列级 Delta 同步

- 本地写入优先广播 `raw_delta`（携带 `columns`）。
- 构造列级载荷失败时自动回退 `raw_data`（整行）。

#### 2) 全量同步流控参数（`sync.TenetConfig`）

以下参数作用于 `sync.NewTenantNetwork` / `sync.NewMultiTenantManager`：

- `FetchResponseBuffer`：fetch 响应缓冲容量（默认 `256`）
- `FetchResponseIdleTimeout`：无 done marker 时的空闲等待（默认 `1s`）

```go
network, err := sync.NewTenantNetwork("tenant-1", &sync.TenetConfig{
	Password:                 "cluster-secret",
	ListenPort:               8001,
	FetchResponseBuffer:      1024,
	FetchResponseIdleTimeout: 3 * time.Second,
})
_ = network
_ = err
```

#### 3) 可观测性（`engine.Stats()`）

`engine.Stats()` 返回：

- 引擎侧：`ChangeEnqueued` / `ChangeProcessed` / `ChangeBackpressure` / `ChangeQueueDepth`
- 网络侧：`Network.FetchRequests` / `FetchSuccess` / `FetchTimeouts` / `FetchPartialTimeouts` / `FetchOverflows` / `DroppedResponses` / `InFlightRequests` 等

```go
stats := engine.Stats()
fmt.Printf("queue depth=%d enqueued=%d processed=%d backpressure=%d\n",
	stats.ChangeQueueDepth,
	stats.ChangeEnqueued,
	stats.ChangeProcessed,
	stats.ChangeBackpressure,
)
fmt.Printf("fetch req=%d ok=%d timeout=%d partial=%d overflow=%d dropped=%d inflight=%d\n",
	stats.Network.FetchRequests,
	stats.Network.FetchSuccess,
	stats.Network.FetchTimeouts,
	stats.Network.FetchPartialTimeouts,
	stats.Network.FetchOverflows,
	stats.Network.DroppedResponses,
	stats.Network.InFlightRequests,
)
```

#### 4) 错误分类（`errors.Is`）

`pkg/sync/sync_errors.go` 提供了可识别错误：

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
		// 收到部分数据但未完整结束，建议告警+重试
	case errors.Is(err, sync.ErrResponseOverflow):
		// 增大 FetchResponseBuffer 或降低并发
	case errors.Is(err, sync.ErrPeerDisconnectedBeforeResponse):
		// 等待重连后重试
	}
}
_ = rows
```

## 垃圾回收

```go
// 方式 1：指定 safeTimestamp
safeTs := myDB.Now() - 5000 // 5s
result := myDB.GC(safeTs)

// 方式 2：按时间偏移（推荐）
result2 := myDB.GCByTimeOffset(1 * time.Minute)

fmt.Printf("tables=%d rows=%d removed=%d errors=%d\n",
	result.TablesScanned,
	result.RowsScanned,
	result.TombstonesRemoved,
	len(result.Errors),
)
_ = result2
```

建议在低峰期周期执行 GC；分布式场景应保守计算 safe time，避免过早清理导致离线节点回补异常。

## 性能建议

- 大列表（RGA）读取：优先 `FindCRDTs()` + `Iterator()`
- 高频写入场景：关注 `engine.Stats().ChangeBackpressure`
- 全量同步场景：根据网络条件调整 `FetchResponseBuffer` 与 `FetchResponseIdleTimeout`

## 项目结构

- `pkg/store`：KV 存储抽象（BadgerDB 实现）
- `pkg/crdt`：CRDT 实现（LWW / ORSet / Counter / RGA / Map）
- `pkg/meta`：Catalog 与 Schema
- `pkg/index`：索引编码与管理
- `pkg/db`：数据库 API 与查询规划
- `pkg/sync`：分布式同步引擎

## 待办事项

- [ ] 增加 HTTP/RPC 接口
- [ ] 增加 Mobile (Android/iOS) 绑定支持

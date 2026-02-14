# Yep CRDT Database

Yep CRDT 是一个本地优先（Local-First）的 Go 嵌入式 CRDT 数据库，构建在 BadgerDB 之上。

- 多 CRDT 类型：`LWW` / `Counter` / `ORSet` / `RGA` / `LocalFile`
- SQL-Like 查询：`Where` / `And` / `OrderBy` / `Limit` / `Offset` / `IN`
- 自动索引与查询规划（Longest Prefix Match）
- 分布式自动同步（增量、全量补齐、重连）
- 面向多节点场景的 GC 与可观测性

完整说明见 `USER_GUIDE.md`。

## 快速导航

- [安装](#安装)
- [5 分钟上手](#5-分钟上手)
- [CRDT 列类型速查](#crdt-列类型速查)
- [查询与事务速查](#查询与事务速查)
- [分布式同步](#分布式同步)
- [垃圾回收](#垃圾回收)
- [性能建议](#性能建议)
- [项目结构](#项目结构)
- [待办事项](#待办事项)

## 安装

```bash
go get github.com/shinyes/yep_crdt
```

建议使用与 `go.mod` 一致的 Go 版本（当前为 `1.25.5`）。

## 5 分钟上手

以下示例覆盖：初始化、建表、写入、查询、开启同步。

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
	ysync "github.com/shinyes/yep_crdt/pkg/sync"
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

	engine, err := ysync.EnableSync(myDB, db.SyncConfig{
		ListenPort:   8001,
		ConnectTo:    "127.0.0.1:8002", // 可选
		Password:     "secret",          // 必填
		Debug:        false,
		IdentityPath: "./tmp/_tenet_identity/tenant-1-8001.json", // 推荐：持久化节点网络身份
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("node=%s addr=%s\n", engine.LocalID(), engine.LocalAddr())

	users := myDB.Table("users")
	id, err := uuid.NewV7()
	if err != nil {
		log.Fatal(err)
	}

	if err := users.Set(id, map[string]any{"name": "Alice"}); err != nil {
		log.Fatal(err)
	}
	if err := users.Add(id, "views", 1); err != nil {
		log.Fatal(err)
	}
	if err := users.Add(id, "tags", "developer"); err != nil {
		log.Fatal(err)
	}

	rows, err := users.Where("views", db.OpGt, 0).
		OrderBy("views", true).
		Limit(10).
		Find()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(rows)
}
```

## CRDT 列类型速查

主键要求使用 UUIDv7（时间有序、全局唯一）。

| CRDT 类型 | 常用方法 | 典型场景 |
| :--- | :--- | :--- |
| `CrdtLWW` | `Set` | 昵称、状态、标题等普通字段 |
| `CrdtCounter` | `Add` / `Remove` | 点赞、浏览量、库存 |
| `CrdtORSet` | `Add` / `Remove` | 标签、分类、成员集合 |
| `CrdtRGA` | `Add` / `InsertAt` / `InsertAfter` / `RemoveAt` | 有序列表、协作文档片段 |
| `CrdtLocalFile` | `Set`（通过 `db.FileImport`）/ `ReadAll` / `ReadAt` | 图片、附件、大文件引用 |

## 查询与事务速查

```go
// 单行读取
u, _ := users.Get(userID)
fmt.Println(u)

// 条件查询
rows, err := users.Where("name", db.OpEq, "Alice").
	And("views", db.OpGt, 10).
	OrderBy("views", true).
	Offset(0).
	Limit(20).
	Find()

// IN 查询
rows2, err := users.Where("views", db.OpIn, []any{100, 200, 300}).Find()

// 事务写入
err = myDB.Update(func(tx *db.Tx) error {
	t := tx.Table("users")
	if err := t.Add(id1, "views", 100); err != nil {
		return err
	}
	return t.Add(id2, "views", 100)
})
_ = rows
_ = rows2
_ = err
```

读取大 RGA 或复杂 CRDT 时，优先 `FindCRDTs()` + `Iterator()` 流式遍历，避免一次性 `.Value()` 带来的大内存分配。

## 分布式同步

### 基础能力

- 自动广播本地变更
- 自动重连
- 增量同步 + 全量同步补齐
- HLC 驱动的因果一致时序
- 网络层连接状态作为在线/离线判定来源（应用层不再单独做离线超时判定）

### 关键配置（2026-02）

`sync.TenetConfig` 新增流控参数：

- `FetchResponseBuffer`：fetch 响应缓冲容量（默认 `256`）
- `FetchResponseIdleTimeout`：无 done marker 时的空闲等待（默认 `1s`）

`db.SyncConfig` 关键建议：

- `IdentityPath`：tenet 节点身份文件路径。建议单独放在如 `./tmp/_tenet_identity/...`，不要与 Badger 数据目录混放；这样重启后 `peerID` 保持稳定。

```go
network, err := ysync.NewTenantNetwork("tenant-1", &ysync.TenetConfig{
	Password:                 "cluster-secret",
	ListenPort:               8001,
	FetchResponseBuffer:      1024,
	FetchResponseIdleTimeout: 3 * time.Second,
})
_ = network
_ = err
```

### 可观测性（`engine.Stats()`）

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

### 错误分类（`errors.Is`）

`pkg/sync/sync_errors.go` 提供可识别错误：

- `sync.ErrTimeoutWaitingResponse`
- `sync.ErrTimeoutWaitingResponseCompletion`
- `sync.ErrResponseOverflow`
- `sync.ErrPeerDisconnectedBeforeResponse`
- `sync.ErrResponseChannelClosed`

```go
rows, err := nodeMgr.FetchRawTableData(peerID, "users")
if err != nil {
	switch {
	case errors.Is(err, ysync.ErrTimeoutWaitingResponse):
		// 直接超时，建议重试
	case errors.Is(err, ysync.ErrTimeoutWaitingResponseCompletion):
		// 收到部分数据但未完整结束，建议告警 + 重试
	case errors.Is(err, ysync.ErrResponseOverflow):
		// 增大 FetchResponseBuffer 或降低并发
	case errors.Is(err, ysync.ErrPeerDisconnectedBeforeResponse):
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

建议在低峰期周期执行 GC。分布式场景下应保守估计 safe time，避免过早清理导致离线节点回补异常。

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
- [ ] 增加 Mobile（Android/iOS）绑定支持

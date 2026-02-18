# Yep CRDT Database

Yep CRDT 是一个本地优先（Local-First）的 Go 嵌入式 CRDT 数据库，构建在 BadgerDB 之上。

- 多 CRDT 类型：`LWW` / `Counter` / `ORSet` / `RGA` / `LocalFile`
- SQL-Like 查询：`Where` / `And` / `OrderBy` / `Limit` / `Offset` / `IN`
- 自动索引与查询规划（Longest Prefix Match）
- 分布式自动同步（增量、全量补齐、重连）
- 本地备份/恢复（单租户与全租户打包）
- `gc_floor` 安全栅栏（落后节点自动全量追平）
- 面向多节点场景的手动 GC 协商与可观测性

完整说明见 `USER_GUIDE.md`。

## 快速导航

- [安装](#安装)
- [快速开始（推荐）](#快速开始推荐)
- [初始化方式对比](#初始化方式对比)
- [CRDT 列类型速查](#crdt-列类型速查)
- [查询与事务速查](#查询与事务速查)
- [分布式同步](#分布式同步)
- [备份与恢复](#备份与恢复)
- [垃圾回收](#垃圾回收)
- [性能建议](#性能建议)
- [项目结构](#项目结构)
- [待办事项](#待办事项)

## 安装

```bash
go get github.com/shinyes/yep_crdt
```

建议使用与 `go.mod` 一致的 Go 版本（当前为 `1.25.5`）。

## 快速开始（推荐）

以下示例覆盖：一站式初始化、建表、写入、查询。

```go
package main

import (
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
)

func main() {
	database, err := db.OpenBadgerWithConfig(db.BadgerOpenConfig{
		Path:                   "./tmp/my_db",
		DatabaseID:             "tenant-1",
		BadgerValueLogFileSize: 256 * 1024 * 1024,
		Schemas: []*meta.TableSchema{
			{
				Name: "users",
				Columns: []meta.ColumnSchema{
					{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
					{Name: "views", Type: meta.ColTypeInt, CrdtType: meta.CrdtCounter},
					{Name: "tags", Type: meta.ColTypeString, CrdtType: meta.CrdtORSet},
				},
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	users := database.Table("users")
	if users == nil {
		log.Fatal("table users not found")
	}

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

可选：如果需要统一迁移逻辑，可以在 `BadgerOpenConfig` 里提供 `EnsureSchema`。

`DefineTable` 对同名表采用“等价幂等、冲突报错”语义：schema 等价时直接成功，若列类型/CRDT 或索引定义不一致会返回错误。

## 初始化方式对比

| 方式 | 推荐场景 | 特点 |
| :--- | :--- | :--- |
| `db.OpenBadgerWithConfig` | 大多数业务场景 | 一步完成目录创建、Badger 打开、`db.Open`、建表与初始化钩子，失败自动清理 |
| `store.NewBadgerStore` + `db.Open` | 需要完全控制底层初始化细节 | 更灵活，但调用方要自己处理目录、参数拼装、错误回滚 |

细粒度控制示例：

```go
package main

import (
	"log"
	"os"

	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func main() {
	dbPath := "./tmp/my_db"
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		log.Fatal(err)
	}

	s, err := store.NewBadgerStore(
		dbPath,
		store.WithBadgerValueLogFileSize(256*1024*1024),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	database, err := db.Open(s, "tenant-1")
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()
}
```

## CRDT 列类型速查

主键要求使用 UUIDv7（时间有序、全局唯一）。

Schema 列类型支持：`string` / `int` / `bool` / `float` / `bytes` / `timestamp`。
`Table.Get()` 与 `Query.Find()` 会按 schema 进行类型化解码（例如 `timestamp -> time.Time`）。
值本体长度说明：`ColTypeInt` 按十进制文本存储（约 `1~20` 字节，含负号）；`ColTypeFloat` 按 `float64` 文本存储（通常约 `1~24` 字节）。

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
err = database.Update(func(tx *db.Tx) error {
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
- `gc_floor` 落地持久化与传播（心跳/控制消息携带）
- peer `gc_floor` 与本地不一致时，按 peer 级别阻断增量
- peer `gc_floor` 领先本地时自动触发 full sync 追平
- HLC 驱动的因果一致时序
- 网络层连接状态作为在线/离线判定来源（应用层不再单独做离线超时判定）

### 本地多租户一键启动（简化版）

`sync.StartNodeFromDataRoot` 会自动探测 `DataRoot` 下租户目录，并启动所有租户频道：

- `tenant_port` 形式（例如 `tenant-a_9001`、`tenant-b_9001`）：会优先按当前 `ListenPort` 发现
- 纯租户目录形式（例如 `tenant-a/`、`tenant-b/`，目录内已有 Badger `MANIFEST`）

StartNodeFromDataRoot 会自动从 DataRoot 目录中寻找租户，并启动 tenet 网络及租户对应的频道进行同步。
```go
node, err := ysync.StartNodeFromDataRoot(ysync.NodeFromDataRootOptions{
	DataRoot:   "./tmp/demo_manual",
	ListenPort: 9001,
	ConnectTo:  "127.0.0.1:9002",
	Password:   "cluster-secret",
	Reset:      false,
	BadgerValueLogFileSize: 256 * 1024 * 1024, // 可选：vlog 大小
	EnsureSchema: func(d *db.DB) error {
		// 可选：统一建表/迁移
		return nil
	},
})
if err != nil {
	log.Fatal(err)
}
defer node.Close()
engine := node.Engine()

// 启动后按 tenantID 获取任意租户（唯一入口）
tenantDB, ok := engine.TenantDatabase("tenant-b")
if ok {
	_ = tenantDB
}
```

### 同步模式开关

`NodeFromDataRootOptions` 的 `IncrementalOnly` 可用于“增量优先”模式：

- `IncrementalOnly: true` 时，会关闭重连时的自动全量触发（长离线阈值与时钟差阈值触发都关闭）
- 适合希望把全量同步切换为显式运维动作的场景

```go
node, err := ysync.StartNodeFromDataRoot(ysync.NodeFromDataRootOptions{
	DataRoot:        "./tmp/demo_manual",
	ListenPort:      9001,
	Password:        "cluster-secret",
	IncrementalOnly: true,
})
_ = node
_ = err
```

### 关键配置

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

### 可观测性（`engine.TenantStats()`）

```go
stats, ok := engine.TenantStats("tenant-1")
if !ok {
	log.Fatal("tenant not started")
}
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

## 备份与恢复

当前备份/恢复能力基于 Badger backup stream：

- 仅包含 Badger KV 数据
- 不包含 `FileStorageDir` 实体文件（例如 LocalFileCRDT 对应的本地附件）
- 恢复时会自动创建目标 DB 目录的父目录

### 单租户（`db` API）

```go
// 全量备份（since=0）
since, err := database.BackupToLocal("./backup/tenant-a.badgerbak")
if err != nil {
	log.Fatal(err)
}
_ = since

// 恢复到指定 DB 目录（例如 ./data/tenant-a）
restoredDB, err := db.RestoreBadgerFromLocalBackup(db.BadgerRestoreConfig{
	BackupPath:      "./backup/tenant-a.badgerbak",
	Path:            "./data/tenant-a",
	DatabaseID:      "tenant-a",
	ReplaceExisting: true,
})
if err != nil {
	log.Fatal(err)
}
defer restoredDB.Close()
```

### 单租户（`LocalNode` 封装）

```go
// tenant-a 备份
_, err := node.BackupTenant("tenant-a", "./backup/tenant-a.badgerbak")
if err != nil {
	log.Fatal(err)
}

// 恢复到 <DataRoot>/tenant-a
// 注意：若 tenant-a 在当前 LocalNode 正在运行，会返回错误
err = node.RestoreTenant(ysync.TenantRestoreOptions{
	TenantID:        "tenant-a",
	BackupPath:      "./backup/tenant-a.badgerbak",
	ReplaceExisting: true,
})
if err != nil {
	log.Fatal(err)
}
```

### 全租户打包备份

```go
sinceByTenant, err := node.BackupAllTenants("./backup/all-tenants.zip")
if err != nil {
	log.Fatal(err)
}
fmt.Println("backup since:", sinceByTenant)
```

归档内容：

- `manifest.json`：租户列表、归档条目路径、since
- `tenants/<tenant_id_hex>.badgerbak`：每个租户的 Badger 备份流

当前未提供 `RestoreAllTenants` 一键恢复 API；可按 manifest 逐租户解包后调用 `db.RestoreBadgerFromLocalBackup(...)` 恢复到 `<DataRoot>/<tenantID>`。

## 垃圾回收

同步层的旧自动 GC 管理器已移除；当前推荐使用“手动触发 + 多节点协商”的方式执行 GC。

### 单节点/本地维护

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

### 多节点协商 GC（推荐）

```go
// 通过 LocalNode 触发（内部会执行 prepare -> commit -> execute，必要时 abort）
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

也可调用 engine 对象的 `ManualGC` 方法：

```go
gcResult, err := engine.ManualGC("tenant-1", 15*time.Second)
_ = gcResult
_ = err
```

说明：

- 协调器先向在线节点收集各自 `safeTimestamp`，取最小值作为本轮 `safeTimestamp`
- `commit` 阶段仅做可执行确认，不执行 GC
- 所有参与节点确认后才进入 `execute` 阶段执行 GC
- 任一阶段失败时会触发 `abort` 清理远端 pending 状态（尽力而为）
- 节点在本地 `execute` 成功后会更新并持久化 `gc_floor`
- `gc_floor` 不一致的 peer 不参与增量同步；peer floor 领先本地时会先 full sync 追平
- `timeout <= 0` 时会使用默认超时（当前实现为 10 秒）

## 性能建议

- 大列表（RGA）读取：优先 `FindCRDTs()` + `Iterator()`
- 高频写入场景：关注 `engine.TenantStats(tenantID).ChangeBackpressure`
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

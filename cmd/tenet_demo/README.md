# Tenet Demo 使用指南

## 简介

`tenet_demo` 是一个交互式命令行工具，用于演示基于 CRDT 的分布式数据同步。它展示了：

- **LWW (Last-Write-Wins)** 冲突解决
- **ORSet** 集合操作
- **增量同步**：自动广播数据变更
- **全量同步**：从其他节点拉取完整数据
- **HLC 时钟**：混合逻辑时钟保证因果一致性

## 启动节点

### 节点 1（监听端口 8001）
```bash
./tenet_demo.exe -l 8001 -t tenant-1 -p mypassword
```

### 节点 2（连接到节点 1）
```bash
./tenet_demo.exe -l 8002 -t tenant-1 -p mypassword -c localhost:8001
```

### 参数说明
- `-l`: 监听端口（0 表示随机）
- `-t`: 租户 ID（相同租户的节点才能互相通信）
- `-p`: 网络密码（必须相同）
- `-c`: 连接地址（可选，格式：`host:port`）
- `-d`: 启用调试日志

## 命令列表

### 数据操作

#### `add <name> <email>`
添加新用户，自动广播到其他节点。

```
> add Alice alice@example.com
✅ 添加成功: 1a2b3c4d (Alice <alice@example.com>)
📡 已广播原始 CRDT 数据到 1 个节点
```

#### `update <id> <name> <email>`
更新用户信息（演示 LWW 冲突解决）。

```
> update 1a2b3c4d Alice alice@newdomain.com
✅ 更新成功: 1a2b3c4d (Alice <alice@newdomain.com>)
```

**冲突解决演示**：
1. 在节点 1 和节点 2 同时断网
2. 分别更新同一用户的 email
3. 重新连接后，HLC 时钟较大的更新会胜出

#### `tag <id> <tag>`
为用户添加标签（演示 ORSet，支持并发添加）。

```
> tag 1a2b3c4d developer
✅ 标签已添加: 1a2b3c4d -> developer
```

**并发添加演示**：
1. 节点 1 添加标签 `developer`
2. 节点 2 添加标签 `admin`
3. 同步后，两个标签都会保留（ORSet 特性）

### 查询操作

#### `list`
列出所有用户。

```
> list
📋 用户列表:
  1. map[_id:1a2b3c4d name:Alice email:alice@example.com tags:[developer admin]]
共 1 条记录
```

#### `get <id>`
查看用户详情。

```
> get 1a2b3c4d
👤 用户详情 (1a2b3c4d):
  _id: 1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d
  name: Alice
  email: alice@example.com
  tags: [developer admin]
```

### 同步操作

#### `sync`
手动增量同步（广播所有本地数据）。

```
> sync
📢 已同步 5 条原始 CRDT 数据到 1 个节点 (HLC: 1234567890)
```

#### `fullsync <peer-id>`
从指定节点全量同步。

```
> fullsync a1b2c3d4e5f6g7h8
🔄 开始从节点 a1b2c3d4 全量同步...
✅ 全量同步完成:
   表数量: 1
   行数量: 10
   拒绝数: 0
```

**使用场景**：
- 新节点加入集群
- 节点长时间离线后重新上线
- 数据不一致时强制同步

### 监控操作

#### `clock`
查看当前 HLC 时钟。

```
> clock
🕐 当前 HLC 时钟: 1234567890
```

#### `peers`
查看在线节点。

```
> peers
🌐 在线节点 (2):
  1. a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6
  2. b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7
```

## CRDT 冲突解决演示

### LWW 冲突（Last-Write-Wins）

**场景**：两个节点同时更新同一字段

1. **节点 1**:
   ```
   > add Bob bob@example.com
   > update <bob-id> Bob bob@company1.com
   ```

2. **节点 2**（稍后）:
   ```
   > update <bob-id> Bob bob@company2.com
   ```

3. **结果**：HLC 时钟较大的更新胜出（通常是节点 2）

### ORSet 并发添加

**场景**：两个节点同时添加不同标签

1. **节点 1**:
   ```
   > tag <alice-id> developer
   ```

2. **节点 2**:
   ```
   > tag <alice-id> admin
   ```

3. **结果**：两个标签都保留 `[developer, admin]`

## 技术细节

### 数据同步流程

1. **本地写入** → `Table.Set()` → `MapCRDT.Apply()`
2. **获取原始字节** → `Table.GetRawRow()` → CRDT 序列化
3. **网络广播** → `BroadcastRawData()` → 发送原始字节
4. **远端接收** → `OnReceiveMerge()` → `Table.MergeRawRow()`
5. **CRDT 合并** → `MapCRDT.Merge()` → 无冲突合并

### HLC 时钟机制

- **本地写入**：`HLC.Tick()` 递增时钟
- **接收消息**：`HLC.Update(remoteClock)` 同步时钟
- **时间戳检查**：拒绝过期数据（`timestamp < localClock`）

### 全量同步机制

1. 发送 `MsgTypeFetchRawRequest` 请求
2. 远端调用 `Table.ScanRawRows()` 扫描所有行
3. 逐行发送 `MsgTypeFetchRawResponse` 响应
4. 本地调用 `Table.MergeRawRow()` 合并每一行

## 故障排查

### 节点无法连接
- 检查网络密码是否一致（`-p` 参数）
- 检查租户 ID 是否一致（`-t` 参数）
- 检查防火墙设置

### 数据未同步
- 使用 `peers` 命令检查节点是否在线
- 使用 `clock` 命令检查时钟是否正常
- 手动执行 `sync` 或 `fullsync` 强制同步

### 冲突解决异常
- 检查 HLC 时钟是否正常递增
- 使用 `get <id>` 查看详细数据
- 必要时使用 `fullsync` 重新同步

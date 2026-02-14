# Tenet Demo 浣跨敤鎸囧崡

## 绠€浠?

`tenet_demo` 鏄竴涓氦浜掑紡鍛戒护琛屽伐鍏凤紝鐢ㄤ簬婕旂ず鍩轰簬 CRDT 鐨勫垎甯冨紡鏁版嵁鍚屾銆傚畠灞曠ず浜嗭細

- **LWW (Last-Write-Wins)** 鍐茬獊瑙ｅ喅
- **ORSet** 闆嗗悎鎿嶄綔
- **列级 Delta**：默认优先广播变更列（`raw_delta` + `columns`），失败回退整行。
- **澧為噺鍚屾**锛氳嚜鍔ㄥ箍鎾暟鎹彉鏇?
- **鍏ㄩ噺鍚屾**锛氫粠鍏朵粬鑺傜偣鎷夊彇瀹屾暣鏁版嵁
- **HLC 鏃堕挓**锛氭贩鍚堥€昏緫鏃堕挓淇濊瘉鍥犳灉涓€鑷存€?

## 鍚姩鑺傜偣

### 鑺傜偣 1锛堢洃鍚鍙?8001锛?
```bash
./tenet_demo.exe -l 8001 -t tenant-1 -p mypassword
```

### 鑺傜偣 2锛堣繛鎺ュ埌鑺傜偣 1锛?
```bash
./tenet_demo.exe -l 8002 -t tenant-1 -p mypassword -c localhost:8001
```

### 鍙傛暟璇存槑
- `-l`: 鐩戝惉绔彛锛? 琛ㄧず闅忔満锛?
- `-t`: 绉熸埛 ID锛堢浉鍚岀鎴风殑鑺傜偣鎵嶈兘浜掔浉閫氫俊锛?
- `-p`: 缃戠粶瀵嗙爜锛堝繀椤荤浉鍚岋級
- `-c`: 杩炴帴鍦板潃锛堝彲閫夛紝鏍煎紡锛歚host:port`锛?
- `-d`: 鍚敤璋冭瘯鏃ュ織

## 鍛戒护鍒楄〃

### 鏁版嵁鎿嶄綔

#### `add <name> <email>`
娣诲姞鏂扮敤鎴凤紝鑷姩骞挎挱鍒板叾浠栬妭鐐广€?

```
> add Alice alice@example.com
鉁?娣诲姞鎴愬姛: 1a2b3c4d (Alice <alice@example.com>)
馃摗 宸插箍鎾師濮?CRDT 鏁版嵁鍒?1 涓妭鐐?
```

#### `update <id> <name> <email>`
鏇存柊鐢ㄦ埛淇℃伅锛堟紨绀?LWW 鍐茬獊瑙ｅ喅锛夈€?

```
> update 1a2b3c4d Alice alice@newdomain.com
鉁?鏇存柊鎴愬姛: 1a2b3c4d (Alice <alice@newdomain.com>)
```

**鍐茬獊瑙ｅ喅婕旂ず**锛?
1. 鍦ㄨ妭鐐?1 鍜岃妭鐐?2 鍚屾椂鏂綉
2. 鍒嗗埆鏇存柊鍚屼竴鐢ㄦ埛鐨?email
3. 閲嶆柊杩炴帴鍚庯紝HLC 鏃堕挓杈冨ぇ鐨勬洿鏂颁細鑳滃嚭

#### `tag <id> <tag>`
涓虹敤鎴锋坊鍔犳爣绛撅紙婕旂ず ORSet锛屾敮鎸佸苟鍙戞坊鍔狅級銆?

```
> tag 1a2b3c4d developer
鉁?鏍囩宸叉坊鍔? 1a2b3c4d -> developer
```

**骞跺彂娣诲姞婕旂ず**锛?
1. 鑺傜偣 1 娣诲姞鏍囩 `developer`
2. 鑺傜偣 2 娣诲姞鏍囩 `admin`
3. 鍚屾鍚庯紝涓や釜鏍囩閮戒細淇濈暀锛圤RSet 鐗规€э級

### 鏌ヨ鎿嶄綔

#### `list`
鍒楀嚭鎵€鏈夌敤鎴枫€?

```
> list
馃搵 鐢ㄦ埛鍒楄〃:
  1. map[_id:1a2b3c4d name:Alice email:alice@example.com tags:[developer admin]]
鍏?1 鏉¤褰?
```

#### `get <id>`
鏌ョ湅鐢ㄦ埛璇︽儏銆?

```
> get 1a2b3c4d
馃懁 鐢ㄦ埛璇︽儏 (1a2b3c4d):
  _id: 1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d
  name: Alice
  email: alice@example.com
  tags: [developer admin]
```

### 鍚屾鎿嶄綔

#### `sync`
鎵嬪姩澧為噺鍚屾锛堝箍鎾墍鏈夋湰鍦版暟鎹級銆?

```
> sync
馃摙 宸插悓姝?5 鏉″師濮?CRDT 鏁版嵁鍒?1 涓妭鐐?(HLC: 1234567890)
```

#### `fullsync <peer-id>`
浠庢寚瀹氳妭鐐瑰叏閲忓悓姝ャ€?

```
> fullsync a1b2c3d4e5f6g7h8
馃攧 寮€濮嬩粠鑺傜偣 a1b2c3d4 鍏ㄩ噺鍚屾...
鉁?鍏ㄩ噺鍚屾瀹屾垚:
   琛ㄦ暟閲? 1
   琛屾暟閲? 10
   鎷掔粷鏁? 0
```

**浣跨敤鍦烘櫙**锛?
- 鏂拌妭鐐瑰姞鍏ラ泦缇?
- 鑺傜偣闀挎椂闂寸绾垮悗閲嶆柊涓婄嚎
- 鏁版嵁涓嶄竴鑷存椂寮哄埗鍚屾

### 鐩戞帶鎿嶄綔

#### `clock`
鏌ョ湅褰撳墠 HLC 鏃堕挓銆?

```
> clock
馃晲 褰撳墠 HLC 鏃堕挓: 1234567890
```

#### `peers`
鏌ョ湅鍦ㄧ嚎鑺傜偣銆?

```
> peers
馃寪 鍦ㄧ嚎鑺傜偣 (2):
  1. a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6
  2. b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7
```

## CRDT 鍐茬獊瑙ｅ喅婕旂ず

### LWW 鍐茬獊锛圠ast-Write-Wins锛?

**鍦烘櫙**锛氫袱涓妭鐐瑰悓鏃舵洿鏂板悓涓€瀛楁

1. **鑺傜偣 1**:
   ```
   > add Bob bob@example.com
   > update <bob-id> Bob bob@company1.com
   ```

2. **鑺傜偣 2**锛堢◢鍚庯級:
   ```
   > update <bob-id> Bob bob@company2.com
   ```

3. **缁撴灉**锛欻LC 鏃堕挓杈冨ぇ鐨勬洿鏂拌儨鍑猴紙閫氬父鏄妭鐐?2锛?

### ORSet 骞跺彂娣诲姞

**鍦烘櫙**锛氫袱涓妭鐐瑰悓鏃舵坊鍔犱笉鍚屾爣绛?

1. **鑺傜偣 1**:
   ```
   > tag <alice-id> developer
   ```

2. **鑺傜偣 2**:
   ```
   > tag <alice-id> admin
   ```

3. **缁撴灉**锛氫袱涓爣绛鹃兘淇濈暀 `[developer, admin]`

## 鎶€鏈粏鑺?

### 数据同步流程

1. **本地写入** -> `Table.Set()/Add()/Remove()/Insert*()` -> `MapCRDT.Apply()`
2. **提取变更状态** -> 优先 `Table.GetRawRowColumns()`，失败回退 `Table.GetRawRow()`
3. **网络广播** -> 优先 `BroadcastRawDelta()`（`MsgTypeRawDelta` + `columns`），失败回退 `BroadcastRawData()`
4. **远端接收** -> `OnReceiveDelta()` 或 `OnReceiveMerge()` -> `Table.MergeRawRow()`
5. **CRDT 合并** -> `MapCRDT.Merge()` -> 无冲突收敛

### HLC 时钟机制

- **鏈湴鍐欏叆**锛歚HLC.Tick()` 閫掑鏃堕挓
- **鎺ユ敹娑堟伅**锛歚HLC.Update(remoteClock)` 鍚屾鏃堕挓
- **鏃堕棿鎴虫鏌?*锛氭嫆缁濊繃鏈熸暟鎹紙`timestamp < localClock`锛?

### 鍏ㄩ噺鍚屾鏈哄埗

1. 鍙戦€?`MsgTypeFetchRawRequest` 璇锋眰
2. 杩滅璋冪敤 `Table.ScanRawRows()` 鎵弿鎵€鏈夎
3. 閫愯鍙戦€?`MsgTypeFetchRawResponse` 鍝嶅簲
4. 鏈湴璋冪敤 `Table.MergeRawRow()` 鍚堝苟姣忎竴琛?

## 鏁呴殰鎺掓煡

### 鑺傜偣鏃犳硶杩炴帴
- 妫€鏌ョ綉缁滃瘑鐮佹槸鍚︿竴鑷达紙`-p` 鍙傛暟锛?
- 妫€鏌ョ鎴?ID 鏄惁涓€鑷达紙`-t` 鍙傛暟锛?
- 妫€鏌ラ槻鐏璁剧疆

### 鏁版嵁鏈悓姝?
- 浣跨敤 `peers` 鍛戒护妫€鏌ヨ妭鐐规槸鍚﹀湪绾?
- 浣跨敤 `clock` 鍛戒护妫€鏌ユ椂閽熸槸鍚︽甯?
- 鎵嬪姩鎵ц `sync` 鎴?`fullsync` 寮哄埗鍚屾

### 鍐茬獊瑙ｅ喅寮傚父
- 妫€鏌?HLC 鏃堕挓鏄惁姝ｅ父閫掑
- 浣跨敤 `get <id>` 鏌ョ湅璇︾粏鏁版嵁
- 蹇呰鏃朵娇鐢?`fullsync` 閲嶆柊鍚屾


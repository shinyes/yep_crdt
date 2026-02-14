# Yep CRDT Database

涓€涓畝鍗曠殑銆佹敮鎸?CRDT 鐨勬湰鍦颁紭鍏?(Local-First) 鏁版嵁搴擄紝鏋勫缓鍦?BadgerDB 涔嬩笂锛屾彁渚涚被浼?SQL 鐨勬煡璇㈣兘鍔涖€佽嚜鍔ㄧ储寮曚互鍙婇拡瀵圭壒瀹?CRDT 绫诲瀷鐨勯珮绾ф搷浣滄敮鎸併€?

## 鐗规€?

*   **鏈湴浼樺厛 (Local-First)**: 鏁版嵁瀛樺偍鍦ㄦ湰鍦帮紝鏀寔绂荤嚎鎿嶄綔銆?
*   **CRDT 鏀寔**: 鍐呯疆澶氱 CRDT 绫诲瀷锛屾敮鎸佺粏绮掑害鐨勬棤鍐茬獊鏇存柊銆?
    *   `LWW-Register`: 鏈€鍚庡啓鍏ヨ儨鍑?(Last-Write-Wins)锛岄€傜敤浜庢櫘閫氬瓧娈点€?
    *   `OR-Set`: 瑙傚療-绉婚櫎闆嗗悎 (Observed-Remove Set)锛屾敮鎸佹硾鍨?`ORSet[T]`锛岄€傜敤浜庢爣绛俱€佺被鍒瓑銆?
    *   `PN-Counter`: 姝ｈ礋璁℃暟鍣?(Positive-Negative Counter)锛岄€傜敤浜庣偣璧炴暟銆佹祻瑙堥噺绛夈€?
    *   `RGA`: 澶嶅埗鍙闀挎暟缁?(Replicated Growable Array)锛屾敮鎸佹硾鍨?`RGA[T]`锛岄€傜敤浜庢湁搴忓垪琛ㄣ€乀ODO 鍒楄〃绛夈€?
    *   `LocalFile`: 鏈湴鏂囦欢鍏宠仈 CRDT锛屽瓨鍌ㄦ枃浠跺厓鏁版嵁锛堣矾寰勩€佸ぇ灏忋€佸搱甯岋級骞舵彁渚涘唴瀹硅鍙栬兘鍔涳紝閫傜敤浜庡浘鐗囥€佹枃妗ｉ檮浠剁瓑銆?
*   **SQL-Like 鏌ヨ**: 鎻愪緵娴佸紡 API 杩涜鏁版嵁鏌ヨ銆?
    *   鏀寔 `Where`, `And`, `Limit`, `Offset`, `OrderBy` 绛夋搷浣溿€?
    *   鏀寔 `=`, `!=`, `>`, `>=`, `<`, `<=`, `IN` 绛夋潯浠躲€?
*   **鏅鸿兘鏌ヨ瑙勫垝鍣?*:
    *   瀹炵幇浜?**鏈€闀垮墠缂€鍖归厤 (Longest Prefix Match)** 绠楁硶銆?
    *   鑷姩閫夋嫨鏈€浣崇殑澶嶅悎绱㈠紩鎴栧崟鍒楃储寮曘€?
*   **鍒嗗竷寮忓熀纭€**:
    *   **NodeID 鎸佷箙鍖?*: 鑺傜偣韬唤鍦ㄩ噸鍚悗淇濇寔涓嶅彉銆?
    *   **娣峰悎閫昏緫鏃堕挓 (HLC)**: 鎻愪緵鍥犳灉涓€鑷寸殑鏃堕棿鎴筹紝涓哄垎甯冨紡鍚屾濂犲畾鍩虹銆?
*   **鑷姩绱㈠紩**: 瀹氫箟 Schema 鍚庤嚜鍔ㄧ淮鎶や簩绾х储寮曘€?
*   **澶氱鎴锋敮鎸?*: 鍩轰簬鏂囦欢绯荤粺鐨勫绉熸埛闅旂銆?
*   **鍒嗗竷寮忓悓姝?(Auto Sync)**: 鍩轰簬 TCP 鐨?P2P 鑷姩鍙戠幇鍜屽閲忓悓姝ワ紝浠呴渶涓€琛屼唬鐮佸嵆鍙惎鐢ㄣ€傛敮鎸侊細
    *   **鑷姩骞挎挱**: 鏈湴鍙樻洿瀹炴椂鎺ㄩ€佸埌杩炴帴鐨勮妭鐐广€?
    *   **鑷姩閲嶈繛**: 缃戠粶鏂紑鍚庤嚜鍔ㄥ皾璇曢噸鏂板缓绔嬭繛鎺ャ€?
    *   **澧為噺/鍏ㄩ噺鍚屾**: 鏅鸿兘鍒囨崲鍚屾绛栫暐锛岀‘淇濇暟鎹渶缁堜竴鑷淬€?
    *   **列级 Delta 优先**: 本地写入优先广播 `raw_delta`（携带 `columns`）；若列级载荷不可用则自动回退为整行 `raw_data`。
*   **娉涘瀷鏀寔 (Generics)**: 鏍稿績 CRDT 绫诲瀷 (`ORSet`, `RGA`) 鍏ㄩ潰鏀寔 Go 娉涘瀷锛屾彁渚涙洿濂界殑绫诲瀷瀹夊叏鍜屽紑鍙戜綋楠屻€?
*   **鍨冨溇鍥炴敹 (Garbage Collection)**:
    *   **绋冲畾鏃堕棿鎴?*: 鍩轰簬 Hybrid Logical Clock (HLC) 鐨?Safe Time 鏈哄埗銆?
    *   **鑷姩娓呯悊**: 鑷姩娓呯悊杩囨湡鐨?Tombstones (ORSet) 鍜岀墿鐞嗙Щ闄ゅ凡鍒犻櫎鐨勮妭鐐?(RGA)锛屽交搴曡В鍐?CRDT 鍏冩暟鎹啫鑳€闂銆?
*   **寮哄埗 UUIDv7**: 涓婚敭蹇呴』鏄湁鏁堢殑 UUIDv7 鏍煎紡锛屼互纭繚鏃堕棿鏈夊簭鎬у拰鍏ㄥ眬鍞竴鎬с€?
*   **浜嬪姟鏀寔**: 鎵€鏈夋洿鏂版搷浣滈兘鍦?BadgerDB 鐨勪簨鍔′腑鍘熷瓙鎵ц銆?

## 蹇€熷紑濮?

### 瀹夎

```bash
go get github.com/shinyes/yep_crdt
```

### 瀹屾暣浣跨敤鎸囧崡

#### 1. 鍒濆鍖栨暟鎹簱鍙婂悓姝?

```go
package main

import (
	"log"
	"os"
	"fmt"

	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
	"github.com/shinyes/yep_crdt/pkg/sync"
	"github.com/google/uuid"
)

func main() {
	// 鍒濆鍖栧瓨鍌ㄨ矾寰?
	dbPath := "./tmp/my_db"
	os.MkdirAll(dbPath, 0755)

	// 鍒涘缓 BadgerDB 瀛樺偍鍚庣
	s, err := store.NewBadgerStore(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// 鎵撳紑鏁版嵁搴撳疄渚?(闇€瑕佹寚瀹氬敮涓€鐨勬暟鎹簱 ID)
	myDB := db.Open(s, "tenant-1")
	defer myDB.Close()
	
	// 璁剧疆鏂囦欢瀛樺偍鏍圭洰褰曪紙鐢ㄤ簬 LocalFileCRDT锛?
	myDB.SetFileStorageDir("./data/files")

	// 鉁?涓€琛屼唬鐮佸紑鍚垎甯冨紡鑷姩鍚屾
	engine, err := sync.EnableSync(myDB, db.SyncConfig{
		ListenPort: 8001,       // 鏈湴鐩戝惉绔彛
		ConnectTo:  "127.00.1:8002", // (鍙€? 杩炴帴鍒板叾浠栬妭鐐?
		Password:   "secret",   // 闆嗙兢瀵嗙爜
	})
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("鑺傜偣 ID: %s, 鐩戝惉鍦板潃: %s\n", engine.LocalID(), engine.LocalAddr())
    // ...
}
```

#### 2. 瀹氫箟 Schema (琛ㄧ粨鏋?

鏀寔涓烘瘡涓€鍒楁寚瀹?CRDT 绫诲瀷銆傚鏋滄湭鎸囧畾锛岄粯璁や负 `LWW`銆?

```go
	err := myDB.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			// 鏅€氬瓧娈?(LWW)
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "age", Type: meta.ColTypeInt, CrdtType: meta.CrdtLWW},
            
			// 璁℃暟鍣?(Counter)
			{Name: "views", Type: meta.ColTypeInt, CrdtType: meta.CrdtCounter},
            
			// 闆嗗悎 (ORSet)
			{Name: "tags", Type: meta.ColTypeString, CrdtType: meta.CrdtORSet},
            
			// 鏈夊簭鍒楄〃 (RGA)
			{Name: "todos", Type: meta.ColTypeString, CrdtType: meta.CrdtRGA},

			// 鏈湴鏂囦欢 (LocalFile)
			{Name: "avatar", Type: meta.ColTypeString, CrdtType: meta.CrdtLocalFile},
		},
		Indexes: []meta.IndexSchema{
			// 澶嶅悎绱㈠紩
			{Name: "idx_name_age", Columns: []string{"name", "age"}, Unique: false},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	table := myDB.Table("users")
```

#### 3. 鍩虹鎿嶄綔 (LWW Register)

**娉ㄦ剰**: 鎵€鏈夐敭蹇呴』鏄湁鏁堢殑 UUIDv7銆?

```go
	// 鐢熸垚 UUIDv7 涓婚敭
	u1ID, _ := uuid.NewV7()

	// 鎻掑叆鎴栨洿鏂版暣琛?(鎴栭儴鍒?LWW 鍒?
	// 浠讳綍鍐欏叆鎿嶄綔閮戒細鑷姩骞挎挱鍒板凡杩炴帴鐨勮妭鐐?
	table.Set(u1ID, map[string]any{
		"name": "Alice", 
		"age": 30,
	})
```

#### 4. 楂樼骇 CRDT 鎿嶄綔

瀵逛簬 `Counter`, `ORSet`, `RGA` 绫诲瀷鐨勫垪锛岃浣跨敤 `Add`, `Remove` 绛変笓鐢ㄦ柟娉曪紝浠ヤ繚鐣欏苟鍙戝悎骞剁壒鎬с€?

##### 璁℃暟鍣?(Counter)

```go
	// 澧炲姞
	table.Add(u1ID, "views", 10) // views = 10
	table.Add(u1ID, "views", 5)  // views = 15

	// 鍑忓皯 (涓ょ鏂瑰紡)
	table.Add(u1ID, "views", -3) // views = 12
	table.Remove(u1ID, "views", 2) // views = 10
```

##### 闆嗗悎 (ORSet)

```go
	// 娣诲姞鍏冪礌
	table.Add(u1ID, "tags", "developer")
	table.Add(u1ID, "tags", "golang")

	// 绉婚櫎鍏冪礌
	table.Remove(u1ID, "tags", "developer")
    // tags 鐜板湪鍙寘鍚?"golang"
```

##### 鏈夊簭鍒楄〃 (RGA)

```go
	// 杩藉姞鍒版湯灏?
	table.Add(u1ID, "todos", "Task 1")
	table.Add(u1ID, "todos", "Task 3")
    
	// 鍦ㄦ寚瀹氬厓绱犲悗鎻掑叆
	table.InsertAfter(u1ID, "todos", "Task 1", "Task 2")
    // 鍒楄〃椤哄簭: [Task 1, Task 2, Task 3]

	// 鎸夌储寮曟彃鍏?(0-based)
	table.InsertAt(u1ID, "todos", 0, "Urgent Task")
    // 鍒楄〃椤哄簭: [Urgent Task, Task 1, Task 2, Task 3]

	// 鎸夌储寮曠Щ闄?
	table.RemoveAt(u1ID, "todos", 3) // 绉婚櫎 "Task 3"
```

##### 鏈湴鏂囦欢 (LocalFile)

```go
    // 鍋囪鐗╃悊鏂囦欢宸插瓨鍦ㄤ簬 /local/path/avatar.jpg
    
    // 鑷姩瀵煎叆鏂囦欢锛氬鍒跺埌鏁版嵁搴撳瓨鍌ㄧ洰褰曞苟璁＄畻鍏冩暟鎹?
    fileImport := db.FileImport{
        LocalPath:    "/local/path/avatar.jpg",
        RelativePath: "images/avatar.jpg",
    }
    
    // 鎻掑叆
    table.Set(u1ID, map[string]any{
        "avatar": fileImport,
    })
    
    // 璇诲彇鏂囦欢鍐呭 (闇€閫氳繃 FindCRDTs 鑾峰彇 ReadOnlyLocalFile)
    rows, _ := table.Where("id", db.OpEq, u1ID).FindCRDTs()
    for _, row := range rows {
        if file, err := row.GetLocalFile("avatar"); err == nil {
            // 璇诲彇鍏ㄩ儴
            content, _ := file.ReadAll()
            // 鎴栭殢鏈鸿鍙?
            header, _ := file.ReadAt(0, 100)
        }
    }
```

#### 5. 鏌ヨ鏁版嵁

鏌ヨ瑙勫垝鍣ㄤ細鑷姩閫夋嫨鏈€浣崇储寮曘€傛敮鎸佷赴瀵岀殑鏌ヨ鎿嶄綔绗﹀拰鍒嗛〉/鎺掑簭銆?

```go
	// 绠€鍗曚富閿煡璇?
	u1, _ := table.Get(u1ID)
	fmt.Println(u1)

	// 鑾峰彇鍘熷 CRDT (鍙) - 鐢ㄤ簬璁块棶 RGA/ORSet 绛夊鏉傜粨鏋?
	crdtU1, _ := table.GetCRDT(u1ID)
	if crdtU1 != nil {
		if rSet, _ := crdtU1.GetSetString("tags"); rSet != nil {
			fmt.Println("Tags:", rSet.Elements())
		}
	}

	// 澶嶆潅鏉′欢鏌ヨ
	// 鑷姩浣跨敤 idx_name_age 绱㈠紩
	results, err := table.Where("name", db.OpEq, "Alice").
                          And("age", db.OpGt, 20).
                          OrderBy("age", true). // 鎸?age 鍊掑簭
                          Offset(0).
                          Limit(10).
                          Find()
    
    // IN 鏌ヨ
    results, err = table.Where("views", db.OpIn, []any{100, 200}).Find()

	for _, row := range results {
		fmt.Printf("Row: %v\n", row)
	}
```

#### 6. 鏁版嵁搴撶骇鍒殑鍨冨溇鍥炴敹 (Database-Level GC)

Yep CRDT 鎻愪緵浜嗙粺涓€鐨勬暟鎹簱绾у埆 GC API锛岀敤浜庢竻鐞嗘墍鏈?CRDT 绫诲瀷鐨勫纰戞暟鎹€?

```go
import "time"

// 鑾峰彇褰撳墠鏃堕棿鎴?
currentTime := myDB.Now()

// 璁＄畻 safeTimestamp锛堜緥濡傦細5 绉掑墠鐨勬暟鎹彲浠ュ畨鍏ㄦ竻鐞嗭級
safeTimestamp := currentTime - 5000 // 5000 姣 = 5 绉?

// 鎵ц GC
result := myDB.GC(safeTimestamp)

fmt.Printf("鎵弿琛ㄦ暟閲? %d\n", result.TablesScanned)
fmt.Printf("鎵弿琛屾暟閲? %d\n", result.RowsScanned)
fmt.Printf("娓呯悊鐨勫纰戞暟閲? %d\n", result.TombstonesRemoved)

if len(result.Errors) > 0 {
    for _, err := range result.Errors {
        log.Printf("GC 閿欒鍊? %v\n", err)
    }
}
```

鎴栬€呬娇鐢ㄦ洿鏂逛究鐨?`GCByTimeOffset` 鏂规硶锛?

```go
// 娓呯悊 1 鍒嗛挓鍓嶇殑鏁版嵁
result := myDB.GCByTimeOffset(1 * time.Minute)

fmt.Printf("娓呯悊浜?%d 涓纰慭n", result.TombstonesRemoved)
```

##### 琛ㄧ骇鍒?GC

濡傛灉鍙渶瑕佹竻鐞嗙壒瀹氳〃锛?

```go
table := myDB.Table("users")

// 鎵ц琛ㄧ骇 GC
result := table.GC(safeTimestamp)

fmt.Printf("鎵弿琛? %d\n", result.RowsScanned)
fmt.Printf("娓呯悊澧撶: %d\n", result.TombstonesRemoved)
```

##### 瀹氭湡 GC 绛栫暐锛堟帹鑽愶級

```go
// 鍚姩瀹氭湡 GC goroutine
func startPeriodicGC(db *db.DB, interval time.Duration, offset time.Duration) {
    ticker := time.NewTicker(interval)
    go func() {
        for range ticker.C {
            result := db.GCByTimeOffset(offset)
            if result.TombstonesRemoved > 0 {
                log.Printf("GC: 娓呯悊浜?%d 涓纰?, result.TombstonesRemoved)
            }
        }
    }()
}

// 浣跨敤锛氭瘡鍒嗛挓娓呯悊 30 绉掑墠鐨勬暟鎹?
startPeriodicGC(myDB, 1*time.Minute, 30*time.Second)
```

##### 鍒嗗竷寮忓悓姝ヤ腑鐨?GC 绠＄悊锛堟帹鑽愮敤浜庡鑺傜偣鍦烘櫙锛?

瀵逛簬澶氳妭鐐?CRDT 绯荤粺锛屼娇鐢?`pkg/sync` 涓殑 `Engine` 鍙婂叾鍐呯疆鐨?GC 绠＄悊鍣細

```go
import "github.com/shinyes/yep_crdt/pkg/sync"

// 鍒濆鍖栧苟鍚姩鍚屾寮曟搸
engine, _ := sync.EnableSync(myDB, ...)

// GC 浼氳嚜鍔ㄥ惎鍔紝榛樿鍙傛暟锛?
// - 杩愯闂撮殧: 1 鍒嗛挓
// - 鏃堕棿鍋忕Щ: 30 绉?
// - 瓒呮椂鎺у埗: 30 绉?
// - 鏈€澶ч噸璇? 3 娆?

// 寮曟搸浼氬湪鍏抽棴鏃惰嚜鍔ㄥ仠姝?GC
// engine.Close()
```

##### GC 鍘熺悊

- **Safe Timestamp**: 绯荤粺淇濊瘉鍦ㄨ鏃堕棿鐐逛箣鍓嶇殑鎵€鏈夋搷浣滈兘宸插悓姝ュ埌鎵€鏈夎妭鐐?
- **ORSet**: 娓呯悊鍒犻櫎鏃堕棿鏃╀簬 safeTimestamp 鐨?tombstones
- **RGA**: 娓呯悊宸插垹闄や笖鍒犻櫎鏃堕棿鏃╀簬 safeTimestamp 鐨勮妭鐐?
- **MapCRDT**: 閫掑綊娓呯悊鎵€鏈夊祵濂?CRDT 鐨勫纰?
- **LWW Register & PN Counter**: 鏃犲纰戯紝涓嶆墽琛屾竻鐞?

##### 娉ㄦ剰浜嬮」

- **淇濆畧璁＄畻 safeTimestamp**: 瀹佸彲淇濈暀鏇村澧撶锛屼篃涓嶈杩囨棭娓呯悊
- **缃戠粶鍒嗗尯**: 鑰冭檻鑺傜偣闀挎湡绂荤嚎鐨勬儏鍐?
- **鎬ц兘褰卞搷**: GC 浼氭壂鎻忔墍鏈夎锛屽缓璁湪浣庡嘲鏈熸墽琛?
- **閿欒澶勭悊**: GC 鎿嶄綔鏀堕泦鎵€鏈夐敊璇紝涓嶄細鍥犱负鍗曚釜澶辫触鑰屼腑鏂?
- **鍒嗗竷寮忕幆淇?*: 鍦ㄥ鑺傜偣鍦烘櫙涓紝浣跨敤 GCManager 鑾峰緱鑷姩閲嶈瘯鍜岀洃鎺ц兘鍔?

#### 7. 浜嬪姟鏀寔

浣跨敤 `Update` 鎴?`View` 鏂规硶鎵ц鍘熷瓙鎬ф搷浣溿€?

```go
	err := myDB.Update(func(tx *db.Tx) error {
		// 鑾峰彇缁戝畾鍒颁簨鍔＄殑琛ㄥ彞鏌?
		t := tx.Table("users")
		
		// 鎵€鏈夌殑鎿嶄綔閮藉湪鍚屼竴涓簨鍔′腑鍘熷瓙鎵ц
		err := t.Add(u1ID, "views", 100)
		if err != nil {
			return err // 杩斿洖閿欒灏嗗鑷翠簨鍔″洖婊?
		}

		return t.Add(u2ID, "views", 100)
	})
```

## 鐙珛浣跨敤 CRDT 鍖?(Standalone CRDT Package)

`pkg/crdt` 鍙互浣滀负鐙珛鐨?Go 搴撲娇鐢紝鏀寔娉涘瀷鍜屽瀮鍦惧洖鏀躲€?

```go
package main

import (
    "fmt"
    "github.com/shinyes/yep_crdt/pkg/crdt"
    "github.com/shinyes/yep_crdt/pkg/hlc"
)

func main() {
    // 1. 娉涘瀷 ORSet (鏀寔浠绘剰 comparable 绫诲瀷)
    intSet := crdt.NewORSet[int]()
    intSet.Apply(crdt.OpORSetAdd[int]{Element: 100})
    intSet.Apply(crdt.OpORSetAdd[int]{Element: 200})
    fmt.Println(intSet.Value()) // Output: [100 200]

    // 2. 娉涘瀷 RGA (鏀寔浠绘剰绫诲瀷)
    clock := hlc.New()
    rga := crdt.NewRGA[string](clock)
    
    // 鎻掑叆鎿嶄綔
    rga.Apply(crdt.OpRGAInsert[string]{AnchorID: rga.Head, Value: "Hello"})
    

    // ...
    // 3. 鍨冨溇鍥炴敹 (GC)
    // 鍋囪 safeTimestamp 鏄泦缇や腑鏈€灏忕殑宸茬煡 HLC 鏃堕棿
    safeTime := clock.Now() 
    
    // 鎵ц GC锛岀墿鐞嗙Щ闄ゅ凡鍒犻櫎涓旇繃鏈熺殑鑺傜偣
    removed := rga.GC(safeTime)
    fmt.Printf("鍨冨溇鍥炴敹绉婚櫎鑺傜偣鏁? %d\n", removed)
}
```

# 鑾峰彇褰撳墠鑺傜偣 HLC
```go
package main

import (
    "fmt"
    "github.com/shinyes/yep_crdt/pkg/db"
    "github.com/shinyes/yep_crdt/pkg/store"
)

func main() {
    // ... 鍒濆鍖?DB ...
    
    // 鑾峰彇褰撳墠閫昏緫鏃堕棿鎴?
    hlcTime := myDB.Now()
    fmt.Printf("Current HLC Time: %d\n", hlcTime)
    
    // 鑾峰彇 Clock 瀹炰緥 (鐢ㄤ簬鍚屾)
    clock := myDB.Clock()
    // clock.Update(remoteTimestamp)
}
```

### 鍨冨溇鍥炴敹鏈哄埗璇﹁В (Garbage Collection)

Yep CRDT 閲囩敤 **鍩轰簬绋冲畾鏃堕棿鎴?(Safe Timestamp)** 鐨勫瀮鍦惧洖鏀舵満鍒讹紝浠ラ槻姝?CRDT 鍏冩暟鎹紙澧撶锛夋棤闄愯啫鑳€銆?

#### 鏍稿績姒傚康锛歋afe Time
Safe Time 鏄竴涓椂闂寸偣锛岀郴缁熶繚璇佸湪璇ユ椂闂寸偣涔嬪墠鐨勬墍鏈夋搷浣滈兘宸插悓姝ュ埌鎵€鏈夎妭鐐广€傞€氬父锛宍SafeTime = min(All nodes' current HLC time) - max_network_delay`銆?

#### 宸ヤ綔鍘熺悊
1.  **ORSet**: 閬嶅巻 `Tombstones`锛岀墿鐞嗗垹闄?`DeletionTime < SafeTime` 鐨勮褰曘€?
2.  **RGA**: 閬嶅巻閾捐〃锛岀墿鐞嗗垹闄ゆ弧瓒充互涓嬫潯浠剁殑鑺傜偣锛?
    *   **宸叉爣璁板垹闄?* (`Deleted == true`)
    *   **杩囨湡** (`DeletedAt < SafeTime`)
    *   **鍙跺瓙鑺傜偣** (鏃犲瓙鑺傜偣鐨勮妭鐐?锛岄槻姝㈢牬鍧忔爲缁撴瀯銆?

#### 浣跨敤寤鸿
寤鸿鍦ㄤ笂灞傚簲鐢ㄤ腑瀹氭湡锛堝姣忓垎閽熸垨姣忓皬鏃讹級璁＄畻闆嗙兢鐨?`SafeTime` 骞惰皟鐢?`GC` 鎺ュ彛銆?

#### 绂荤嚎鑺傜偣澶勭悊绛栫暐 (Handling Offline Nodes)
濡傛灉鍦ㄨ绠?SafeTime 鏃舵湁涓€涓妭鐐归暱鏈熺绾匡紝瀹冧細鎷栨參 `SafeTime` 鐨勬帹杩涳紝瀵艰嚧鏃犳硶鏈夋晥 GC銆傚缓璁噰鍙栦互涓嬬瓥鐣ワ細
1.  **瓒呮椂鍓旈櫎**: 璁惧畾闃堝€硷紙濡?24 灏忔椂锛夈€傝嫢鑺傜偣瓒呮椂鏈彂閫佸績璺筹紝璁＄畻 `SafeTime` 鏃跺皢鍏舵帓闄ゃ€?
2.  **寮哄埗閲嶇疆**: 褰撶绾胯妭鐐归噸鏂颁笂绾挎椂锛屽鏋滃畠钀藉悗浜庡綋鍓嶇殑 `SafeTime`锛岀郴缁熷簲鎷掔粷鍏跺閲忓悓姝ヨ姹傦紝寮哄埗鍏舵竻绌烘湰鍦扮姸鎬佸苟杩涜 **鍏ㄩ噺鍚屾 (Full Sync)**锛屼互闃叉鈥滃兊灏告暟鎹€濆娲汇€?

## 鎬ц兘鏈€浣冲疄璺?(Performance Best Practices)

### 1. 閬嶅巻澶у瀷 RGA (Streaming Large Lists)
褰?RGA 鏁版嵁閲忚緝澶э紙濡傝秴杩?10,000 涓厓绱狅級鏃讹紝璇烽伩鍏嶄娇鐢?`.Value()` 鏂规硶锛屽洜涓哄畠浼氫竴娆℃€у垎閰嶅法澶х殑鍐呭瓨鍒囩墖銆?
鎺ㄨ崘浣跨敤 `Query.FindCRDTs()` 鑾峰彇鍘熷瀵硅薄锛屽苟缁撳悎 `.Iterator()` 杩涜闆跺垎閰嶉亶鍘嗐€?
**娉ㄦ剰锛歚Query.FindCRDTs()` 杩斿洖鐨勬槸鍙鎺ュ彛 (姣斿锛歚ReadOnlyMap`銆乣ReadOnlyRGA`)锛屽己鍒剁姝慨鏀癸紝浠ラ槻姝㈣鐢ㄥ拰闈炴寔涔呭寲鐨勫彉鏇淬€?*

#### 鍙鎺ュ彛姒傝

`Query.FindCRDTs()` 杩斿洖 `[]crdt.ReadOnlyMap`銆傝鎺ュ彛鎻愪緵浜嗙被鍨嬪畨鍏ㄧ殑鍙璁块棶鏂规硶锛?

**ReadOnlyMap**:
- `Get(key string) (any, bool)`: 鑾峰彇浠绘剰绫诲瀷鐨勫€笺€?
- `GetString(key string) (string, bool)`: 鑾峰彇瀛楃涓插€笺€?
- `GetInt(key string) (int, bool)`: 鑾峰彇鏁存暟鍊笺€?
- `GetRGAString(key string) (ReadOnlyRGA[string], error)`: 鑾峰彇鍙鐨?RGA[string]銆?
- `GetRGABytes(key string) (ReadOnlyRGA[[]byte], error)`: 鑾峰彇鍙鐨?RGA[[]byte]銆?
- `GetSetString(key string) (ReadOnlySet[string], error)`: 鑾峰彇鍙鐨?ORSet[string]銆?
- `GetSetInt(key string) (ReadOnlySet[int], error)`: 鑾峰彇鍙鐨?ORSet[int]銆?
- `GetLocalFile(key string) (ReadOnlyLocalFile, error)`: 鑾峰彇鍙鐨?LocalFileCRDT銆?

**ReadOnlyRGA[T]**:
- `Value() any`: 鑾峰彇鍏ㄩ噺鍒囩墖锛堟厧鐢級銆?
- `Iterator() func() (T, bool)`: 鑾峰彇杩唬鍣紝鐢ㄤ簬娴佸紡閬嶅巻銆?

**ReadOnlySet[T]**:
- `Value() any`: 鑾峰彇鍏ㄩ噺鍒囩墖銆?
- `Contains(element T) bool`: 妫€鏌ュ厓绱犳槸鍚﹀瓨鍦ㄣ€?
- `Elements() []T`: 鑾峰彇鎵€鏈夊厓绱犮€?

```go
// 1. 鑾峰彇鍖呭惈鍘熷 CRDT 鐨勭粨鏋滈泦锛堜笉鑷姩鍙嶅簭鍒楀寲 Value锛?
// 杩斿洖 []crdt.ReadOnlyMap
crdts, _ := table.Where("id", db.OpEq, "doc1").FindCRDTs()

for _, doc := range crdts {
    // 2. 鎸夐渶鑾峰彇 RGA 瀹炰緥 (鍙)
    // 浣跨敤绫诲瀷瀹夊叏鐨勬柟娉曡幏鍙?(渚嬪 GetRGABytes 鎴?GetRGAString)
    rga, _ := doc.GetRGABytes("content")
    
    // 3. 浣跨敤 Iterator 娴佸紡閬嶅巻
    if rga != nil {
        iter := rga.Iterator()
        for {
            val, ok := iter()
            if !ok {
                break
            }
            // 澶勭悊 val (鏃犻渶鍏ㄩ噺鍔犺浇鍒板唴瀛?
        }
    }
}
```

### 2. MapCRDT 缂撳瓨
MapCRDT 鍐呴儴瀹炵幇浜嗗啓鍥炵紦瀛?(Write-Back Cache)銆?
*   **璇诲彇**: 鍦?`Apply` 鍜?`Value` 鎿嶄綔涓紝CRDT 瀵硅薄浼氫繚鐣欏湪鍐呭瓨涓紝澶у箙鎻愬崌杩炵画鎿嶄綔鐨勬€ц兘銆?
*   **鍐欏叆**: 鍙湁鍦ㄨ皟鐢?`.Bytes()` 鎴栨寔涔呭寲鏃讹紝鎵嶄細瑙﹀彂搴忓垪鍖栥€?

## 鏋舵瀯姒傝

*   **pkg/store**: 搴曞眰 KV 瀛樺偍鎶借薄 (BadgerDB 瀹炵幇)銆?
*   **pkg/crdt**: 鏍稿績 CRDT 鏁版嵁缁撴瀯瀹炵幇 (LWW, ORSet, PNCounter, RGA, Map)銆?
*   **pkg/meta**: 鍏冩暟鎹拰 Schema 绠＄悊 (Catalog)銆?
*   **pkg/index**: 绱㈠紩缂栫爜鍜岀鐞嗐€?
*   **pkg/db**: 椤跺眰鏁版嵁搴?API锛岄泦鎴?Schema銆両ndex 鍜?Storage锛屽寘鍚煡璇㈣鍒掑櫒銆?
*   **pkg/sync**: **[NEW]** 鑷姩鍚屾寮曟搸锛屾敮鎸佽妭鐐瑰彂鐜般€佺増鏈矡閫氥€佸閲?鍏ㄩ噺鍚屾銆?

## 寰呭姙浜嬮」
*   [x] 实现 CRDT 状态的 P2P 同步逻辑（版本摘要 + 列级/整行增量同步）。
*   [x] 瀹炵幇 CRDT 鐘舵€佺殑 P2P 鍚屾閫昏緫 (鐗堟湰鍚戦噺 / 澧為噺鍚屾)銆?
*   [ ] 澧炲姞 HTTP/RPC 鎺ュ彛銆?
*   [ ] 澧炲姞 Mobile (Android/iOS) 缁戝畾鏀寔銆?


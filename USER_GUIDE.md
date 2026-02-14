# Yep CRDT 鏁版嵁搴撲娇鐢ㄦ墜鍐?

## 1. 绠€浠?

**Yep CRDT** (Conflict-free Replicated Data Type Database) 鏄竴涓珮鎬ц兘銆佹湰鍦颁紭鍏?(Local-First) 鐨勫祵鍏ュ紡鏁版嵁搴擄紝涓撲负鏋勫缓绂荤嚎鍙敤銆佸疄鏃跺崗浣滅殑搴旂敤绋嬪簭鑰岃璁°€傚畠鏋勫缓鍦?BadgerDB 涔嬩笂锛屾彁渚涗簡寮哄ぇ鐨?CRDT 鏀寔锛岀‘淇濆湪鍒嗗竷寮忓拰鏂綉鐜涓嬬殑鏁版嵁鏈€缁堜竴鑷存€с€?

### 鏍稿績鐗规€?

*   **鏈湴浼樺厛 (Local-First)**: 鏁版嵁瀛樺偍鍦ㄦ湰鍦拌澶囷紝搴旂敤绋嬪簭闅忔椂鍙敤锛屾棤闇€鎸佺画鑱旂綉銆?
*   **CRDT 鍐呮牳**: 鍐呯疆澶氱 CRDT 绫诲瀷锛岃嚜鍔ㄥ鐞嗗苟鍙戝啿绐侊紝鏃犻渶澶嶆潅鐨勯攣鏈哄埗銆?
*   **SQL-Like 鏌ヨ**: 鎻愪緵娴佺晠鐨勯摼寮?API 鐢ㄤ簬鏁版嵁鏌ヨ锛屾敮鎸佺储寮曘€佸鏉傛潯浠剁瓫閫夊拰鎺掑簭銆?
*   **鑷姩绱㈠紩**: 鍩轰簬 Schema 瀹氫箟鑷姩缁存姢浜岀骇绱㈠紩锛屾煡璇㈤€熷害蹇€?
*   **绫诲瀷瀹夊叏**: 鏍稿績 API 鏀寔娉涘瀷锛屽噺灏戣繍琛屾椂閿欒銆?
*   **楂樻€ц兘**: 搴曞眰浣跨敤 BadgerDB KV 瀛樺偍锛岀粨鍚?LSM Tree 鎻愪緵楂樺悶鍚愰噺鐨勮鍐欍€?
*   **鍨冨溇鍥炴敹 (GC)**: 鍩轰簬 HLC (Hybrid Logical Clock) 鐨勬棤绛夊緟鍨冨溇鍥炴敹鏈哄埗锛屾湁鏁堥槻姝㈠厓鏁版嵁鑶ㄨ儉銆?

---

## 2. 瀹夎

浣跨敤 Go Modules 瀹夎锛?

```bash
go get github.com/shinyes/yep_crdt
```

纭繚浣犵殑 Go 鐗堟湰 >= 1.20銆?

---

## 3. 蹇€熷紑濮?

### 3.1 鍒濆鍖栨暟鎹簱

Yep CRDT 闇€瑕佷竴涓湰鍦扮洰褰曟潵瀛樺偍鏁版嵁锛堝熀浜?BadgerDB锛夈€?

```go
package main

import (
	"log"
	"os"

	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func main() {
	// 1. 纭繚瀛樺偍璺緞瀛樺湪
	dbPath := "./data/mydb"
	os.MkdirAll(dbPath, 0755)

	// 2. 鍒濆鍖?BadgerDB 瀛樺偍鍚庣
	s, err := store.NewBadgerStore(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// 3. 鎵撳紑鏁版嵁搴撳疄渚?
	// Open 浼氬惎鍔ㄥ悗鍙版湇鍔★紙濡?HLC 鏃堕挓锛夛紝骞舵牎楠?DatabaseID
	myDB := db.Open(s, "my-database-id")
	defer myDB.Close()

	// (鍙€? 璁剧疆鏂囦欢瀛樺偍鏍圭洰褰曪紝鐢ㄤ簬 LocalFileCRDT
	myDB.SetFileStorageDir("./data/files")

	// 4. [NEW] 寮€鍚嚜鍔ㄥ悓姝?
	// 浠呴渶涓€琛屼唬鐮侊紝鍗冲彲鍔犲叆 P2P 缃戠粶
	engine, err := sync.EnableSync(myDB, db.SyncConfig{
		ListenPort: 8001,       // 鏈湴鐩戝惉绔彛 (0 琛ㄧず闅忔満)
		ConnectTo:  "192.168.1.100:8001", // (鍙€? 鍒濆杩炴帴鑺傜偣
		Password:   "my-secret-password", // 闆嗙兢瀵嗙爜
	})
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("Node ID: %s running at %s\n", engine.LocalID(), engine.LocalAddr())
    
    // ... 鍚庣画鎿嶄綔
}
```

### 3.2 瀹氫箟 Schema (琛ㄧ粨鏋?

鍦ㄤ娇鐢ㄨ〃涔嬪墠锛屽繀椤诲畾涔夊叾 Schema銆?

`DefineTable` 鐨勪富瑕佷綔鐢ㄦ槸 **娉ㄥ唽琛ㄧ粨鏋?(Schema)**锛屽畠鐨勮涓虹被浼间簬 **"Create If Not Exists" (濡傛灉涓嶅瓨鍦ㄥ垯鍒涘缓)**銆?

1. **濡傛灉琛ㄤ笉瀛樺湪**锛氬畠浼氬湪 Catalog 涓垱寤烘柊琛紝鍒嗛厤 Table ID 骞舵寔涔呭寲銆?
2. **濡傛灉琛ㄥ凡瀛樺湪**锛氬畠**涓嶄細**瑕嗙洊鏃х粨鏋勶紝鐩存帴杩斿洖鎴愬姛锛岀‘淇濇搷浣滅殑骞傜瓑鎬с€?

Schema 鍐冲畾浜嗘瘡涓€鍒楃殑鏁版嵁绫诲瀷鍜?CRDT 绫诲瀷锛屼互鍙婄储寮曠瓥鐣ャ€?

```go
import "github.com/shinyes/yep_crdt/pkg/meta"

// ...

err := myDB.DefineTable(&meta.TableSchema{
    Name: "products",
    Columns: []meta.ColumnSchema{
        // 1. LWW (Last-Write-Wins): 鏅€氬瓧娈?
        // 閫傜敤浜庣敤鎴峰悕銆佹爣棰樸€佷环鏍肩瓑涓嶉渶瑕佸悎骞堕€昏緫鐨勫瓧娈点€?
        // 鏈€鍚庡啓鍏ョ殑鍊间細瑕嗙洊鏃у€笺€?
        {Name: "title", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
        {Name: "price", Type: meta.ColTypeInt, CrdtType: meta.CrdtLWW},
        
        // 2. Counter (PN-Counter): 璁℃暟鍣?
        // 閫傜敤浜庣偣璧炴暟銆佸簱瀛樸€佹祻瑙堥噺銆傛敮鎸佸苟鍙戝姞鍑忋€?
        {Name: "views", Type: meta.ColTypeInt, CrdtType: meta.CrdtCounter},
        
        // 3. ORSet (Observed-Remove Set): 闆嗗悎
        // 閫傜敤浜庢爣绛俱€佸垎绫汇€佸叧娉ㄥ垪琛ㄣ€傛敮鎸佸苟鍙戞坊鍔?绉婚櫎鍏冪礌銆?
        {Name: "tags", Type: meta.ColTypeString, CrdtType: meta.CrdtORSet},
        
        // 4. RGA (Replicated Growable Array): 鏈夊簭鍒楄〃
        // 閫傜敤浜庢枃妗ｅ唴瀹广€佽瘎璁哄垪琛ㄣ€佸緟鍔炰簨椤广€傛敮鎸佸苟鍙戞彃鍏?鎺掑簭銆?
        {Name: "comments", Type: meta.ColTypeString, CrdtType: meta.CrdtRGA},

        // 5. LocalFile (鏈湴鏂囦欢): 寮曠敤澶栭儴鏂囦欢
        // 閫傜敤浜庡浘鐗囥€侀檮浠躲€佸ぇ鏂囦欢銆傚彧瀛樺偍鍏冩暟鎹紝鏀寔鎸夐渶璇诲彇鍐呭銆?
        {Name: "avatar", Type: meta.ColTypeString, CrdtType: meta.CrdtLocalFile},
    },
    Indexes: []meta.IndexSchema{
        // 绠€鍗曠储寮?
        {Name: "idx_price", Columns: []string{"price"}, Unique: false},
        // 澶嶅悎绱㈠紩 (鏀寔澶氬垪缁勫悎鏌ヨ)
        {Name: "idx_title_price", Columns: []string{"title", "price"}, Unique: false},
    },
})

if err != nil {
    log.Fatal("Failed to define table:", err)
}

// 鑾峰彇琛ㄥ彞鏌勭敤浜庡悗缁搷浣?
table := myDB.Table("products")
```

---

## 4. 鏁版嵁鎿嶄綔

鎵€鏈夋暟鎹搷浣滈兘闇€瑕侀€氳繃 `Table` 瀵硅薄杩涜銆?*娉ㄦ剰锛氫富閿繀椤绘槸 UUIDv7 鏍煎紡**锛屼互淇濊瘉鏃堕棿鏈夊簭鎬у拰鍏ㄥ眬鍞竴鎬э紝杩欏鍒嗗竷寮忕郴缁熻嚦鍏抽噸瑕併€?

### 4.1 鍩虹澧炲垹鏀?(LWW Register)

閫傜敤浜?`LWW Register` 绫诲瀷鐨勫瓧娈碉紙濡備笂闈㈢殑 `title`, `price`锛夈€?

```go
import "github.com/google/uuid"

// 鐢熸垚 UUIDv7 涓婚敭
id, _ := uuid.NewV7()

// 鎻掑叆鎴栨洿鏂版暣琛?(鎴栭儴鍒?LWW 鍒?
// Set 鎺ュ彈 map[string]any锛屼細瑕嗙洊鎸囧畾鐨勫瓧娈?
err := table.Set(id, map[string]any{
    "title": "Go Programming Guide",
    "price": 59,
})
if err != nil {
    log.Fatal(err)
}
```

### 4.2 璁℃暟鍣ㄦ搷浣?(PN Counter)

閫傜敤浜?`PN Counter` 绫诲瀷鐨勫瓧娈点€俙PN Counter` 绫诲瀷淇濊瘉鍦ㄥ苟鍙戠幆澧冧笅璁℃暟鐨勫噯纭€с€?

```go
// 澧炲姞娴忚閲?(+1)
table.Add(id, "views", 1)

// 澧炲姞澶氫釜 (+10)
table.Add(id, "views", 10)

// 鍑忓皯娴忚閲?(涓ょ鏂瑰紡)
table.Add(id, "views", -1)
// 鎴栬€?
table.Remove(id, "views", 1)
```

### 4.3 闆嗗悎鎿嶄綔 (ORSet)

閫傜敤浜?`ORSet` 绫诲瀷鐨勫瓧娈点€俙ORSet` 鍏佽骞跺彂娣诲姞鍜屽垹闄ゅ厓绱狅紝涓斿垹闄ゆ搷浣滀紭鍏堜簬娣诲姞鎿嶄綔锛圤bserved-Remove锛夈€?

```go
// 娣诲姞鏍囩
table.Add(id, "tags", "book")
table.Add(id, "tags", "coding")

// 绉婚櫎鏍囩
table.Remove(id, "tags", "book")

// 鏌ヨ鏃讹紝tags 闆嗗悎灏嗗彧鍖呭惈 ["coding"]
```

### 4.4 鏈夊簭鍒楄〃鎿嶄綔 (RGA)

閫傜敤浜?`RGA` 绫诲瀷鐨勫瓧娈点€俙RGA` 鏄笓涓哄崗鍚岀紪杈戣璁＄殑鏈夊簭搴忓垪锛屾敮鎸佸湪浠绘剰浣嶇疆鎻掑叆鍜屽垹闄ゃ€?

```go
// 1. 杩藉姞鍒版湯灏?
table.Add(id, "comments", "First comment")
table.Add(id, "comments", "Second comment")
// 鍒楄〃: ["First comment", "Second comment"]

// 2. 鍦ㄦ寚瀹氬厓绱犲悗鎻掑叆 (InsertAfter)
// 鍋囪鎴戜滑瑕佹彃鍦?"First comment" 鍚庨潰
table.InsertAfter(id, "comments", "First comment", "Reply to first")
// 鍒楄〃: ["First comment", "Reply to first", "Second comment"]

// 3. 鎸夌储寮曚綅缃彃鍏?(InsertAt) - 0-based
// 鎻掑湪鏈€鍓嶉潰 (绱㈠紩 0)
table.InsertAt(id, "comments", 0, "Top comment")
// 鍒楄〃: ["Top comment", "First comment", "Reply to first", "Second comment"]

// 4. 鎸夌储寮曚綅缃Щ闄?(RemoveAt)
table.RemoveAt(id, "comments", 1) // 绉婚櫎绱㈠紩涓?1 鐨勫厓绱?("First comment")
```


> **娉ㄦ剰**: `InsertAfter` 闇€瑕佹彁渚涢敋鐐瑰厓绱犵殑鍊笺€傚鏋滄湁澶氫釜鐩稿悓鍊肩殑鍏冪礌锛屽畠浼氭煡鎵剧涓€涓湭琚垹闄ょ殑鍖归厤椤广€?

### 4.5 鏈湴鏂囦欢鎿嶄綔 (LocalFile)

LocalFileCRDT 浠呭瓨鍌ㄦ枃浠剁殑鍏冩暟鎹紙Path, Size, Hash锛夛紝瀹為檯鏂囦欢瀛樺偍鍦ㄦ湰鍦版枃浠剁郴缁熴€?


```go
// 鎻掑叆鏈湴鏂囦欢 (鑷姩瀵煎叆)
// 鏁版嵁搴撲細鑷姩灏?/tmp/img.png 澶嶅埗鍒?<FileStorageDir>/images/avatar.png
// 骞惰嚜鍔ㄨ绠楀搱甯屽拰鏂囦欢澶у皬
err = table.Set(id, map[string]any{
    "avatar": db.FileImport{
        LocalPath:    "/tmp/img.png",
        RelativePath: "images/avatar.png",
    },
})

// 璇诲彇鏂囦欢 (蹇呴』浣跨敤 FindCRDTs 鑾峰彇寮虹被鍨嬪璞?
rows, _ := table.Where("id", db.OpEq, id).FindCRDTs()
for _, row := range rows {
    if file, err := row.GetLocalFile("avatar"); err == nil {
        // 璇诲彇鍏ㄩ儴鍐呭 -> []byte
        content, _ := file.ReadAll()
        
        // 闅忔満璇诲彇 (offset, length) -> []byte
        header, _ := file.ReadAt(0, 100)
        
        // 鑾峰彇鍏冩暟鎹?
        meta := file.Value().(crdt.FileMetadata)
        fmt.Println("File Size:", meta.Size)
    }
}
```

---

## 5. 鏁版嵁鏌ヨ (Query Builder)

Yep CRDT 鎻愪緵浜嗙被浼?SQL 鐨?Fluent API 鏌ヨ鏋勫缓鍣ㄣ€傚畠浼氳嚜鍔ㄦ牴鎹?`Where` 鏉′欢閫夋嫨鏈€浣崇储寮曪紙鏈€闀垮墠缂€鍖归厤绠楁硶锛夈€?

### 5.1 绠€鍗曟煡璇?(Get)

```go
// 鏍规嵁 ID 鑾峰彇鍗曡鏁版嵁
row, err := table.Get(id)
// row 鏄?map[string]any 绫诲瀷
if row != nil {
    fmt.Println("Title:", row["title"])
}
```

### 5.1.1 鑾峰彇鍗曡鍘熷 CRDT (GetCRDT)

濡傛灉浣犻渶瑕佽闂崟琛岀殑鍘熷 CRDT 缁撴瀯锛堜緥濡備负浜嗚幏鍙?RGA 鐨勮凯浠ｅ櫒锛夛紝鍗冲彲浠ヤ娇鐢?`GetCRDT`銆傚畠杩斿洖 `crdt.ReadOnlyMap` 鎺ュ彛銆?

```go
// 杩斿洖 crdt.ReadOnlyMap
crdtRow, err := table.GetCRDT(id)
if err == nil && crdtRow != nil {
    // 浣跨敤鍙鎺ュ彛瀹夊叏璁块棶宓屽 CRDT
    if rSet, _ := crdtRow.GetSetString("tags"); rSet != nil {
        fmt.Println("Tags:", rSet.Elements())
    }
}
```

### 5.2 鏉′欢鏌ヨ (Where/And)

鏀寔鐨勮繍绠楃锛?
*   `db.OpEq` (`=`): 绛変簬
*   `db.OpNe` (`!=`): 涓嶇瓑浜?
*   `db.OpGt` (`>`): 澶т簬
*   `db.OpGte` (`>=`): 澶т簬绛変簬
*   `db.OpLt` (`<`): 灏忎簬
*   `db.OpLte` (`<=`): 灏忎簬绛変簬
*   `db.OpIn` (`IN`): 鍖呭惈浜庢暟缁?

```go
// 鏌ヨ浠锋牸 > 50 鐨勪功
results, err := table.Where("price", db.OpGt, 50).Find()

// 缁勫悎鏉′欢 (AND)
// 鏌ヨ浠锋牸 > 50 涓?娴忚閲?< 1000 鐨勪功
// 杩欏皢鑷姩鍒╃敤澶嶅悎绱㈠紩 `idx_title_price` (濡傛灉鏉′欢鍖归厤绱㈠紩鍓嶇紑) 鎴栬€呭洖閫€鍒拌〃鎵弿
results, err := table.Where("price", db.OpGt, 50).
                      And("views", db.OpLt, 1000).
                      Find()

// IN 鏌ヨ
results, err := table.Where("title", db.OpIn, []any{"Go Guide", "Rust Guide"}).Find()
```

### 5.3 鎺掑簭涓庡垎椤?(OrderBy/Limit/Offset)

```go
results, err := table.Where("price", db.OpGt, 0).
                      OrderBy("price", true). // true = 闄嶅簭 (DESC), false = 鍗囧簭 (ASC)
                      Offset(10).             // 璺宠繃鍓?10 鏉?(Pagination)
                      Limit(5).               // 鏈€澶氬彇 5 鏉?
                      Find()
```

### 5.4 楂樼骇鏌ヨ锛氳幏鍙?CRDT 瀵硅薄 (FindCRDTs)

榛樿鐨?`Find()` 鏂规硶杩斿洖 `[]map[string]any`锛屽嵆鏁版嵁鐨?JSON 鍙嬪ソ蹇収銆傚鏋滀綘闇€瑕侀亶鍘嗗ぇ鍨?RGA 鍒楄〃锛屾垨鑰呴渶瑕侀珮鎬ц兘璁块棶锛屽彲浠ヤ娇鐢?`FindCRDTs()`銆?

`FindCRDTs()` 杩斿洖 `[]crdt.ReadOnlyMap` 鎺ュ彛锛屽厑璁镐綘鍦ㄤ笉鍙嶅簭鍒楀寲鏁翠釜瀵硅薄鐨勬儏鍐典笅璁块棶閮ㄥ垎鏁版嵁锛屼絾鏄槸鍙鐨勩€?

```go
// 杩斿洖 []crdt.ReadOnlyMap
docs, err := table.Where("id", db.OpEq, id).FindCRDTs()

for _, doc := range docs {
    // 1. 楂樻晥鑾峰彇 LWW 瀛楁
    if title, ok := doc.GetString("title"); ok {
        fmt.Println("Title:", title)
    }

    // 2. 楂樻晥鑾峰彇 RGA 杩唬鍣紝閬垮厤涓€娆℃€у姞杞芥暣涓垏鐗?
    // 杩欏浜庡惈鏈夋垚鍗冧笂涓囨潯璁板綍鐨勫垪琛ㄩ潪甯告湁鐢?
    rga, _ := doc.GetRGAString("comments")
    if rga != nil {
        iter := rga.Iterator()
        for {
            val, ok := iter() // 姣忔璋冪敤杩斿洖涓嬩竴涓厓绱?
            if !ok { break }
            fmt.Println("Comment:", val)
        }
    }
}
```

---

## 6. 鍨冨溇鍥炴敹 (Garbage Collection)

Yep CRDT 鎻愪緵浜嗗己澶х殑鍨冨溇鍥炴敹鏈哄埗锛岀敤浜庢竻鐞?CRDT 鎿嶄綔浜х敓鐨勫纰戞暟鎹紙TMD锛夛紝闃叉鍏冩暟鎹棤闄愯啫鑳€銆傚瀮鍦惧洖鏀跺櫒鍩轰簬娣峰悎閫昏緫鏃堕挓锛圚LC锛夊拰 Safe Timestamp 鏈哄埗銆?

### 6.1 鏍稿績姒傚康

#### Safe Timestamp
**Safe Timestamp** 鏄竴涓椂闂寸偣锛岀郴缁熶繚璇佸湪璇ユ椂闂寸偣涔嬪墠鐨勬墍鏈夋搷浣滈兘宸插悓姝ュ埌鎵€鏈夎妭鐐广€傚畠閫氬父閫氳繃浠ヤ笅鏂瑰紡璁＄畻锛?
- **鏈€灏忔椂閽熷€?*锛氭墍鏈夎妭鐐瑰綋鍓嶆椂閽熷€肩殑鏈€灏忓€硷紙闇€瑕佸叏灞€鍚屾锛?
- **淇濆畧浼拌**锛氬綋鍓嶆椂闂村噺鍘荤綉缁滃欢杩熷宸紙濡?`currentTime - 5s`锛?
- **纭鍗忚**锛氫娇鐢ㄤ竴鑷存€у崗璁紙濡?Raft锛夎褰曠殑纭鏃堕棿鎴?

#### 涓轰粈涔堥渶瑕?GC锛?
鍦?CRDT 绯荤粺涓紝鍒犻櫎鎿嶄綔涓嶄細绔嬪嵆鍒犻櫎鏁版嵁锛岃€屾槸鏍囪涓哄凡鍒犻櫎锛圱ombstone锛夈€傝繖纭繚浜嗭細
- **鍒嗗竷寮忎竴鑷存€?*锛氬嵆浣跨綉缁滃欢杩熸垨鍒嗗尯锛屾墍鏈夎妭鐐归兘鑳芥纭鐞嗗垹闄?
- **鏃犲啿绐佸悎骞?*锛氬欢杩熷垹闄ょ殑鍏冪礌涓嶄細琚敊璇湴鎭㈠

浣嗚繖涔熷鑷达細
- **鍐呭瓨澧為暱**锛歍ombstones 鎸佺画绉疮锛屽崰鐢ㄥ唴瀛?
- **瀛樺偍鑶ㄨ儉**锛氬簭鍒楀寲鏁版嵁鍖呭惈鎵€鏈夊纰戯紝澧炲姞瀛樺偍鎴愭湰

鍥犳闇€瑕佸畾鏈熸墽琛?GC 鏉ユ竻鐞嗚繃鏈熺殑澧撶銆?

### 6.2 鏁版嵁搴撶骇鍒殑 GC API

Yep CRDT 鎻愪緵浜嗘暟鎹簱绾у埆鐨勭粺涓€ GC 鎺ュ彛锛屽彲浠ヤ竴娆℃€ф竻鐞嗘墍鏈夎〃鐨勫纰戞暟鎹€?

```go
import "time"

// 鑾峰彇褰撳墠鏃堕棿鎴?
currentTime := myDB.Now()

// 璁＄畻 safeTimestamp锛堜緥濡傦細5 绉掑墠鐨勬暟鎹彲浠ュ畨鍏ㄦ竻鐞嗭級
safeTimestamp := currentTime - 5000 // 5000 姣 = 5 绉?

// 鎵ц GC
result := myDB.GC(safeTimestamp)

// 妫€鏌ョ粨鏋?
fmt.Printf("鎵弿琛ㄦ暟閲? %d\n", result.TablesScanned)
fmt.Printf("鎵弿琛屾暟閲? %d\n", result.RowsScanned)
fmt.Printf("娓呯悊鐨勫纰戞暟閲? %d\n", result.TombstonesRemoved)

if len(result.Errors) > 0 {
    for _, err := range result.Errors {
        log.Printf("GC 閿欒: %v\n", err)
    }
}
```

#### GCByTimeOffset锛堟帹鑽愶級

鏇存柟渚跨殑鏂规硶鏄娇鐢?`GCByTimeOffset`锛屽畠浼氳嚜鍔ㄨ绠?`safeTimestamp`銆?

```go
// 娓呯悊 1 鍒嗛挓鍓嶇殑鏁版嵁
result := myDB.GCByTimeOffset(1 * time.Minute)

fmt.Printf("娓呯悊浜?%d 涓纰慭n", result.TombstonesRemoved)
```

### 6.3 琛ㄧ骇鍒殑 GC API

濡傛灉鍙渶瑕佹竻鐞嗙壒瀹氳〃鐨勫纰戯紝鍙互浣跨敤琛ㄧ骇鍒殑 GC銆?

```go
table := myDB.Table("users")

// 鎵ц琛ㄧ骇 GC
result := table.GC(safeTimestamp)

fmt.Printf("鎵弿琛? %d\n", result.RowsScanned)
fmt.Printf("娓呯悊澧撶: %d\n", result.TombstonesRemoved)
```

### 6.4 瀹氭湡 GC 绛栫暐锛堟帹鑽愶級

鍦ㄧ敓浜х幆澧冧腑锛屽缓璁惎鍔ㄥ畾鏈?GC 鐨勫悗鍙?goroutine銆?

```go
// 绛栫暐 1: 鍥哄畾闂撮殧 GC
func startPeriodicGC(database *db.DB, interval time.Duration, offset time.Duration) {
    ticker := time.NewTicker(interval)
    go func() {
        for range ticker.C {
            result := database.GCByTimeOffset(offset)
            if result.TombstonesRemoved > 0 {
                log.Printf("GC: 娓呯悊浜?%d 涓纰?, result.TombstonesRemoved)
            }
        }
    }()
}

// 浣跨敤
startPeriodicGC(myDB, 1 * time.Minute, 30 * time.Second)
```

```go
// 绛栫暐 2: 鍩轰簬鏁伴噺鐨勮嚜閫傚簲 GC
func startAdaptiveGC(database *db.DB, interval time.Duration, threshold int) {
    ticker := time.NewTicker(interval)
    go func() {
        for range ticker.C {
            // 妫€鏌ユ渶杩戠殑鎿嶄綔鏁伴噺鎴栧唴瀛樹娇鐢?
            // 濡傛灉瓒呰繃闃堝€硷紝浣跨敤鏇翠繚瀹堢殑 offset
            offset := 30 * time.Second
            if shouldPerformAggressiveGC() {
                offset = 5 * time.Second
            }
            
            result := database.GCByTimeOffset(offset)
            log.Printf("Adaptive GC: 娓呯悊 %d, 鎵弿 %d 琛?, 
                result.TombstonesRemoved, result.RowsScanned)
        }
    }()
}
```

### 6.5 GC 缁撴灉缁熻

GC 鎿嶄綔杩斿洖璇︾粏鐨勭粨鏋滅粺璁★紝渚夸簬鐩戞帶鍜岃皟璇曘€?

```go
type GCResult struct {
    TablesScanned     int      // 鎵弿鐨勮〃鏁伴噺
    RowsScanned       int      // 鎵弿鐨勮鏁伴噺
    TombstonesRemoved int      // 绉婚櫎鐨勫纰戞暟閲?
    Errors           []error  // 閬囧埌鐨勯敊璇垪琛?
}

type TableGCResult struct {
    RowsScanned       int      // 鎵弿鐨勮鏁伴噺
    TombstonesRemoved int      // 绉婚櫎鐨勫纰戞暟閲?
    Errors           []error  // 閬囧埌鐨勯敊璇垪琛?
}
```

### 6.6 娉ㄦ剰浜嬮」

#### Safe Timestamp 鐨勮绠?
- **淇濆畧涓轰笂**锛氬畞鍙繚鐣欐洿澶氬纰戯紝涔熶笉瑕佽繃鏃╂竻鐞?
- **缃戠粶鍒嗗尯**锛氳€冭檻鑺傜偣闀挎湡绂荤嚎鐨勬儏鍐碉紝`safeTimestamp` 鍙兘琚嫋鎱?
- **鍏ㄩ噺鍚屾**锛氱绾胯妭鐐归噸鏂颁笂绾挎椂锛屽簲寮哄埗鍏ㄩ噺鍚屾锛岄伩鍏?鍍靛案鏁版嵁"澶嶆椿

#### 鎬ц兘褰卞搷
- **鎵弿寮€閿€**锛欸C 浼氭壂鎻忔墍鏈夎锛屽彲鑳藉奖鍝嶆€ц兘
- **浣庡嘲鏈熸墽琛?*锛氬缓璁湪绯荤粺璐熻浇浣庢椂鎵ц GC
- **鎵归噺澶勭悊**锛欸C 浣跨敤浜嬪姟鎵归噺鏇存柊锛屽噺灏?I/O 寮€閿€

#### 閿欒澶勭悊
- **涓嶄腑鏂墽琛?*锛氬崟涓鐨勯敊璇笉浼氫腑鏂暣涓?GC 鎿嶄綔
- **鏀堕泦閿欒**锛氭墍鏈夐敊璇兘琚敹闆嗗湪缁撴灉涓紝渚夸簬鍚庣画鍒嗘瀽

---

## 7. 浜嬪姟 (Transactions)

Yep CRDT 鏀寔 ACID 浜嬪姟銆備綘鍙互灏嗗涓搷浣滄墦鍖呭湪涓€涓簨鍔′腑鍘熷瓙鎵ц銆傝繖鍦ㄩ渶瑕佸悓鏃舵洿鏂板涓〃鎴栧琛屾暟鎹椂闈炲父閲嶈銆?

```go
err := myDB.Update(func(tx *db.Tx) error {
    t := tx.Table("products")

    // 姝ラ 1: 淇敼浜у搧 A 鐨勬祻瑙堥噺
    if err := t.Add(idA, "views", 1); err != nil {
        return err // 杩斿洖閿欒灏嗗鑷存暣涓簨鍔″洖婊?
    }

    // 姝ラ 2: 淇敼浜у搧 B 鐨勪环鏍?
    if err := t.Set(idB, map[string]any{"price": 100}); err != nil {
        return err
    }

    // 濡傛灉杩斿洖 nil锛屼簨鍔¤嚜鍔ㄦ彁浜?
    return nil
})
```

鍙浜嬪姟鍙互浣跨敤 `myDB.View(...)`锛岄€氬父姣?`Update` 绋嶅井蹇竴浜涗笖涓嶅崰鐢ㄥ啓閿併€?

---

## 7. 鍒嗗竷寮忎笌鍚屾 (Distributed & Sync)

Yep CRDT 鍐呯疆浜嗗己澶х殑鍒嗗竷寮忓悓姝ュ紩鎿?(`pkg/sync`)锛屾棬鍦ㄨ鏋勫缓鈥滄湰鍦颁紭鍏?(Local-First)鈥濆簲鐢ㄥ彉寰楁瀬鍏剁畝鍗曘€?

### 7.1 鏍稿績鏋舵瀯

Yep CRDT 鐨勫悓姝ュ眰鍩轰簬 TCP 闀胯繛鎺ュ拰 Gossip 鍗忚锛堥儴鍒嗙悊蹇碉級锛屽疄鐜颁簡鍏ㄨ嚜鍔ㄧ殑 P2P 鍚屾銆?

*   **Node Identity (鑺傜偣韬唤)**: 姣忎釜鏁版嵁搴撳疄渚嬪湪鍒濆鍖栨椂浼氱敓鎴愪竴涓熀浜?UUIDv7 鐨勫敮涓€ `NodeID`锛屽苟鎸佷箙鍖栧瓨鍌ㄣ€?
*   **Version Vector (鐗堟湰鍚戦噺)**: 绯荤粺浣跨敤娣峰悎閫昏緫鏃堕挓 (HLC) 鍜岀増鏈悜閲忔潵璺熻釜鏁版嵁鐨勫洜鏋滃叧绯伙紝绮剧‘璇嗗埆鍝簺鏁版嵁闇€瑕佸悓姝ャ€?
*   **Automatic Discovery (鑷姩鍙戠幇)**: 鑺傜偣闂撮€氳繃 TCP 浜掕仈鍚庯紝浼氳嚜鍔ㄤ氦鎹㈣妭鐐瑰垪琛紝褰㈡垚缃戠姸鎷撴墤銆?

### 7.2 鍚敤鍚屾

浣跨敤 `sync.EnableSync` 涓€閿惎鐢ㄥ悓姝ャ€備笉闇€瑕侀澶栭儴缃蹭腑蹇冨寲鏈嶅姟鍣紙濡?Redis 鎴?Kafka锛夈€?

```go
import "github.com/shinyes/yep_crdt/pkg/sync"

engine, err := sync.EnableSync(myDB, db.SyncConfig{
    // 缃戠粶閰嶇疆
    ListenPort: 8080,        // 鏈湴鐩戝惉绔彛
    ConnectTo:  "seed-node:8080", // 绉嶅瓙鑺傜偣鍦板潃
    
    // 瀹夊叏閰嶇疆
    Password: "cluster-secret", // 棰勫叡浜瘑閽?(PSK) 闃叉鏈巿鏉冭闂?
    
    // 璋冭瘯
    Debug: true, // 鎵撳嵃璇︾粏鐨勫悓姝ユ棩蹇?
})
```

### 7.3 同步机制原理

Yep CRDT 当前采用三层机制协同：

#### 1. 实时广播 (Real-time Broadcast)
本地写入成功后（`Set/Add/Remove/InsertAt/InsertAfter/RemoveAt`），同步引擎会立即广播变更：
*   **优先列级增量**：广播 `MsgTypeRawDelta`，携带 `columns` 与部分 `MapCRDT` 状态。
*   **自动回退整行**：若无法生成列级载荷，则回退为 `MsgTypeRawData`（整行状态）。
*   **特点**：低延迟、低带宽，适合在线节点实时收敛。

#### 2. 版本摘要差异同步 (Digest Diff)
节点连接后会交换 `MsgTypeVersionDigest`（每表行级 hash 摘要）：
*   比较本地与远端的行摘要差异。
*   仅发送有差异的行（`SendRawData`），避免无效全量传输。
*   适用于短暂断连后的快速补齐。

#### 3. 全量同步 (Full Sync)
当节点长时间离线、时钟偏差过大或需要强制修复时：
*   发起 `MsgTypeFetchRawRequest`。
*   远端逐行返回 `MsgTypeFetchRawResponse`。
*   本地逐行执行 `Table.MergeRawRow()` 完成合并。

### 7.4 冲突解决

基于 CRDT (Conflict-free Replicated Data Types) 数学理论，Yep CRDT 保证所有副本的数据 **最终强一致 (Strong Eventual Consistency)**。

*   **LWW (Last-Write-Wins)**: 基于 HLC 时间戳，时间戳较大的值胜出。
*   **ORSet / RGA / Counter**: 通过保留必要元数据自动合并并发修改，不丢失操作。
### 7.5 缃戠粶鎷撴墤涓庨槻鐏

*   **P2P Mesh**: 鑺傜偣闂村舰鎴愮綉鐘剁粨鏋勶紝涓嶉渶瑕佹墍鏈夎妭鐐逛袱涓や簰鑱斻€傛秷鎭彲浠ラ€氳繃涓棿鑺傜偣杞彂锛圙ossip 浼犳挱锛夈€?
*   **绌块€忔€?*: 鐩墠涓昏鏀寔鐩磋繛銆傚鏋滃湪 NAT 鍚庯紝闇€瑕佺‘淇濈鍙ｆ槧灏勬垨浣跨敤 VPN/Overlay 缃戠粶锛堝 Tailscale锛夈€?

---

## 8. 闄勫綍锛氭暟鎹被鍨嬪弬鑰冭〃

| Schema 绫诲瀷 | 瀵瑰簲鐨?Go 绫诲瀷 | 鎻忚堪 |
| :--- | :--- | :--- |
| `ColTypeString` | `string` | 鏂囨湰瀛楃涓?|
| `ColTypeInt` | `int`, `int64` | 鏁存暟 |
| `ColTypeBool` | `bool` | 甯冨皵鍊?|

| CRDT 绫诲瀷 | 鎿嶄綔鏂规硶 | 閫傜敤鍦烘櫙 |
| :--- | :--- | :--- |
| **CrdtLWW** | `Set` | 鐢ㄦ埛鍚嶃€佺姸鎬併€侀厤缃」 (Last-Write-Wins) |
| **CrdtCounter** | `Add`, `Remove` | 璁℃暟鍣ㄣ€佺粺璁℃暟鎹?(PN-Counter) |
| **CrdtORSet** | `Add`, `Remove` | 鏍囩銆佸垎绫汇€両D 闆嗗悎 (Observed-Remove Set) |
| **CrdtRGA** | `Add`, `InsertAt`, `InsertAfter`, `Remove`, `RemoveAt` | 鏂囨湰缂栬緫銆佸嵆鏃堕€氳娑堟伅娴併€佷换鍔″垪琛?(RGA) |
| **LocalFile** | `Set` (via `FileMetadata`), `ReadAll`, `ReadAt` (read-only) | 鍥剧墖銆侀檮浠躲€佸ぇ鏂囦欢寮曠敤 |

---


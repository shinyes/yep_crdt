package main

import (
	"fmt"
	"os"
	"time"

	"github.com/shinyes/yep_crdt/crdt"
	"github.com/shinyes/yep_crdt/manager"
)

func main() {
	os.RemoveAll("./db")
	defer os.RemoveAll("./db")

	m, _ := manager.NewManager("./db", "./blobs")
	defer m.Close()

	fmt.Println("=== yep_crdt API 示例 ===")

	// 1. Query API：流式操作 Map
	m.CreateRoot("config", crdt.TypeMap)
	m.From("config").Update().
		Set("name", "Alice").
		Inc("count", 1).
		AddToSet("tags", "go").
		Append("log", "started").
		Commit()

	val, _ := m.From("config").Select("name", "count", "tags", "log").Get()
	fmt.Printf("Query API: %v\n", val)

	// 2. RGA 嵌套 CRDT：序列元素为 Map
	seq := crdt.NewRGA()
	ts := time.Now().UnixNano()

	// 插入 Map 类型元素
	seq.Apply(crdt.RGAOp{OriginID: "n1", TypeCode: 0, PrevID: "start", ElemID: "1", InitType: crdt.TypeMap, Ts: ts})

	// 获取嵌套 Map 并操作
	item := seq.GetElement("1").(*crdt.MapCRDT)
	item.Apply(crdt.MapOp{Key: "title", IsInit: true, InitType: crdt.TypeRegister, Ts: ts})
	item.Apply(crdt.MapOp{Key: "title", ChildOp: crdt.LWWOp{Value: "Hello", Ts: ts}, Ts: ts})

	fmt.Printf("RGA 嵌套: %v\n", seq.Value()) // [map[title:Hello]]

	// 3. ORSet 嵌套 CRDT：集合元素为 Map
	set := crdt.NewORSet()
	set.Apply(crdt.ORSetOp{OriginID: "n1", TypeCode: 0, ElemID: "e1", InitType: crdt.TypeMap, Tag: "t1", Ts: ts})

	elem := set.GetElement("e1").(*crdt.MapCRDT)
	elem.Apply(crdt.MapOp{Key: "name", IsInit: true, InitType: crdt.TypeRegister, Ts: ts})
	elem.Apply(crdt.MapOp{Key: "name", ChildOp: crdt.LWWOp{Value: "Bob", Ts: ts}, Ts: ts})

	fmt.Printf("ORSet 嵌套: %v\n", set.Value()) // [map[name:Bob]]
}

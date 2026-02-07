package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/shinyes/yep_crdt/crdt"
	"github.com/shinyes/yep_crdt/manager"
)

func main() {
	// 清理之前的运行数据
	os.RemoveAll("./example_db")
	os.RemoveAll("./example_blobs")
	defer os.RemoveAll("./example_db")
	defer os.RemoveAll("./example_blobs")

	// 1. 初始化 Manager
	m, err := manager.NewManager("./example_db", "./example_blobs")
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close()

	fmt.Println("=== yep_crdt 示例 ===")

	// 2. 使用新的 SQL-like API 操作 Map
	fmt.Println("\n--- SQL-like API 演示 ---")
	rootID := "app_config"
	m.CreateRoot(rootID, crdt.TypeMap)

	// 使用 fluent API 设置嵌套值
	err = m.From(rootID).Update().
		Set("server.name", "my-app").
		Set("server.port", 8080).
		Set("features.dark_mode", true).
		Commit()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("已设置配置: server.name, server.port, features.dark_mode")

	// 增量计数
	m.From(rootID).Update().Inc("stats.visits", 1).Commit()
	fmt.Println("已增加 visits")

	// 查询数据
	val, err := m.From(rootID).Select("server.name", "server.port", "stats.visits").Get()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("查询结果: %v\n", val)

	// 删除数据
	m.From(rootID).Update().Delete("features.dark_mode").Commit()
	fmt.Println("已删除 features.dark_mode")

	// 验证删除
	deletedVal, _ := m.From(rootID).Select("features.dark_mode").Get()
	fmt.Printf("删除后查询 features.dark_mode: %v (期望为 <nil>)\n", deletedVal)

	// 3. 原有功能演示 (计数器)
	fmt.Println("\n--- 原生 Counter 演示 ---")
	counter, _ := m.CreateRoot("visits", crdt.TypeCounter)

	// 模拟来自不同节点的更新
	op1 := crdt.PNCounterOp{OriginID: "alice", Amount: 1, Ts: time.Now().UnixNano()}
	counter.Apply(op1)
	fmt.Printf("Alice 访问了。当前计数: %v\n", counter.Value())

	op2 := crdt.PNCounterOp{OriginID: "bob", Amount: 5, Ts: time.Now().UnixNano()}
	counter.Apply(op2)
	fmt.Printf("Bob 访问了 (5 次)。累计计数: %v\n", counter.Value())

	// 4. 原有功能演示 (RGA)
	fmt.Println("\n--- 原生 RGA (文本) 演示 ---")
	note, _ := m.CreateRoot("shared_note", crdt.TypeSequence)

	// Alice 输入 "Hi"
	ts := time.Now().UnixNano()
	note.Apply(crdt.RGAOp{OriginID: "alice", TypeCode: 0, PrevID: "start", ElemID: "1", Value: "H", Ts: ts})
	note.Apply(crdt.RGAOp{OriginID: "alice", TypeCode: 0, PrevID: "1", ElemID: "2", Value: "i", Ts: ts + 1})

	// Bob 添加 "!"
	note.Apply(crdt.RGAOp{OriginID: "bob", TypeCode: 0, PrevID: "2", ElemID: "3", Value: "!", Ts: ts + 2})

	// 读取
	chars := note.Value().([]interface{})
	text := ""
	for _, c := range chars {
		text += fmt.Sprintf("%v", c)
	}
	fmt.Printf("笔记内容: %s\n", text)
}

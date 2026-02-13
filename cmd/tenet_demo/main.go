package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
	"github.com/shinyes/yep_crdt/pkg/sync"
)

func main() {
	listenPort := flag.Int("l", 0, "监听端口 (0 表示随机)")
	connectAddr := flag.String("c", "", "连接地址 (可选)")
	password := flag.String("p", "demo-password", "网络密码")
	tenantID := flag.String("t", "tenant-1", "租户 ID")
	debug := flag.Bool("d", false, "启用调试日志")
	flag.Parse()

	storePath := fmt.Sprintf("tmp/tenet_demo_%s_%d", *tenantID, time.Now().UnixNano())
	os.RemoveAll(storePath)
	os.MkdirAll(storePath, 0755)
	defer os.RemoveAll(storePath)

	badgerStore, err := store.NewBadgerStore(storePath)
	if err != nil {
		log.Fatalf("创建存储失败: %v", err)
	}
	defer badgerStore.Close()

	database := db.Open(badgerStore, *tenantID)
	defer database.Close()

	err = database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "email", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	})
	if err != nil {
		log.Fatalf("创建表失败: %v", err)
	}

	// ✨ 一行启动同步
	engine, err := sync.EnableSync(database, db.SyncConfig{
		Password:   *password,
		ListenPort: *listenPort,
		ConnectTo:  *connectAddr,
		Debug:      *debug,
	})
	if err != nil {
		log.Fatalf("启动同步失败: %v", err)
	}

	fmt.Println("==============================================")
	fmt.Printf("  🌐 租户 ID: %s\n", *tenantID)
	fmt.Printf("  🆔 本地节点 ID: %s\n", engine.LocalID()[:8])
	fmt.Printf("  📡 本地监听地址: %s\n", engine.LocalAddr())
	fmt.Println("==============================================")
	fmt.Println("✅ 表 users 已创建")
	fmt.Println("   字段: name (LWW), email (LWW)")
	fmt.Println("✅ 同步已自动启用")
	fmt.Println("   - 数据变更自动广播")
	fmt.Println("   - 节点连接自动版本沟通")
	fmt.Println("")

	reader := bufio.NewReader(os.Stdin)
	printHelp()

	for {
		fmt.Print("> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := parts[0]

		switch cmd {
		case "add":
			handleAdd(database, parts)

		case "update":
			handleUpdate(database, parts)

		case "list":
			handleList(database)

		case "get":
			handleGet(database, parts)

		case "clock":
			handleClock(database)

		case "peers":
			handlePeers(engine)

		case "help":
			printHelp()

		case "quit", "exit":
			return

		default:
			fmt.Printf("❌ 未知命令: %s (输入 help 查看帮助)\n", cmd)
		}
	}
}

func printHelp() {
	fmt.Println("\n📖 命令帮助:")
	fmt.Println("  add <name> <email>           - 添加用户 (自动同步)")
	fmt.Println("  update <id> <name> <email>   - 更新用户 (自动同步)")
	fmt.Println("  list                         - 列出所有用户")
	fmt.Println("  get <id>                     - 查看用户详情")
	fmt.Println("  clock                        - 查看 HLC 时钟")
	fmt.Println("  peers                        - 查看在线节点")
	fmt.Println("  help                         - 显示此帮助")
	fmt.Println("  quit                         - 退出")
	fmt.Println("")
}

func handleAdd(database *db.DB, parts []string) {
	if len(parts) < 3 {
		fmt.Println("❌ 用法: add <name> <email>")
		return
	}

	userKey := uuid.New()
	err := database.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(userKey, map[string]any{
			"name":  parts[1],
			"email": parts[2],
		})
	})
	if err != nil {
		fmt.Printf("❌ 添加失败: %v\n", err)
		return
	}

	fmt.Printf("✅ 添加成功: %s (%s <%s>)\n", userKey.String()[:8], parts[1], parts[2])
	fmt.Println("   📡 已自动广播到所有节点")
}

func handleUpdate(database *db.DB, parts []string) {
	if len(parts) < 4 {
		fmt.Println("❌ 用法: update <id> <name> <email>")
		return
	}

	userID, err := uuid.Parse(parts[1])
	if err != nil {
		fmt.Printf("❌ 无效的 UUID: %v\n", err)
		return
	}

	err = database.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(userID, map[string]any{
			"name":  parts[2],
			"email": parts[3],
		})
	})
	if err != nil {
		fmt.Printf("❌ 更新失败: %v\n", err)
		return
	}

	fmt.Printf("✅ 更新成功: %s\n", userID.String()[:8])
	fmt.Println("   📡 已自动广播到所有节点")
}

func handleList(database *db.DB) {
	fmt.Println("\n📋 用户列表:")
	var users []map[string]any
	database.View(func(tx *db.Tx) error {
		table := tx.Table("users")
		if table != nil {
			users, _ = table.Where("name", "!=", "").Limit(1000).Find()
		}
		return nil
	})

	if len(users) == 0 {
		fmt.Println("  (无数据)")
	} else {
		for i, user := range users {
			fmt.Printf("  %d. %+v\n", i+1, user)
		}
	}
	fmt.Printf("共 %d 条记录\n\n", len(users))
}

func handleGet(database *db.DB, parts []string) {
	if len(parts) < 2 {
		fmt.Println("❌ 用法: get <id>")
		return
	}

	userID, err := uuid.Parse(parts[1])
	if err != nil {
		fmt.Printf("❌ 无效的 UUID: %v\n", err)
		return
	}

	var user map[string]any
	database.View(func(tx *db.Tx) error {
		table := tx.Table("users")
		if table != nil {
			user, _ = table.Get(userID)
		}
		return nil
	})

	if user == nil {
		fmt.Printf("❌ 用户不存在: %s\n", userID.String()[:8])
	} else {
		fmt.Printf("\n👤 用户详情 (%s):\n", userID.String()[:8])
		for k, v := range user {
			fmt.Printf("  %s: %v\n", k, v)
		}
		fmt.Println()
	}
}

func handleClock(database *db.DB) {
	clock := database.Clock().Now()
	fmt.Printf("🕐 当前 HLC 时钟: %d\n", clock)
}

func handlePeers(engine *sync.Engine) {
	peers := engine.Peers()
	if len(peers) == 0 {
		fmt.Println("⚠️  无在线节点")
	} else {
		fmt.Printf("🌐 在线节点 (%d):\n", len(peers))
		for i, peer := range peers {
			fmt.Printf("  %d. %s\n", i+1, peer)
		}
	}
}

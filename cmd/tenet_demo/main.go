package main

import (
	"bufio"
	"context"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	tenetConfig := &sync.TenetConfig{
		Password:    *password,
		ListenPort: *listenPort,
		EnableDebug: *debug,
	}

	mtm := sync.NewMultiTenantManager(tenetConfig)

	tenant, err := mtm.StartTenant(ctx, database)
	if err != nil {
		log.Fatalf("启动租户失败: %v", err)
	}

	localID := tenant.GetNetwork().LocalID()
	localAddr := tenant.GetNetwork().LocalAddr()

	fmt.Println("==============================================")
	fmt.Printf("  租户 ID: %s\n", *tenantID)
	fmt.Printf("  本地节点 ID: %s\n", localID[:8])
	fmt.Printf("  本地监听地址: %s\n", localAddr)
	fmt.Println("==============================================")
	fmt.Println("表 users 已创建，字段: name, email (LWW)")
	fmt.Println("")

	if *connectAddr != "" {
		fmt.Printf("正在连接到 %s...\n", *connectAddr)
		if err := tenant.Connect(*connectAddr); err != nil {
			log.Printf("连接失败: %v", err)
		} else {
			fmt.Println("连接请求已发送")
		}
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("\n命令帮助:")
	fmt.Println("  add <name> <email>  - 添加用户")
	fmt.Println("  list                - 列出所有用户")
	fmt.Println("  sync                - 手动广播同步")
	fmt.Println("  peers               - 查看在线节点")
	fmt.Println("  quit                - 退出")
	fmt.Println("")

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
			if len(parts) < 3 {
				fmt.Println("用法: add <name> <email>")
				continue
			}
			name := parts[1]
			email := parts[2]

			userKey := uuid.New()
			localClock := database.Clock().Now()
			fmt.Printf("🕐 本地 HLC 时钟: %d\n", localClock)
			
			err := database.Update(func(tx *db.Tx) error {
				table := tx.Table("users")
				return table.Set(userKey, map[string]any{
					"name":  name,
					"email": email,
				})
			})
			if err != nil {
				fmt.Printf("❌ 添加失败: %v\n", err)
			} else {
				fmt.Printf("✅ 添加成功: %s (%s <%s>)\n", userKey.String()[:8], name, email)

				peers := tenant.GetNetwork().Peers()
				if len(peers) > 0 {
					data := map[string]any{
						"name":  name,
						"email": email,
					}
					// 写入后的 HLC 时钟
					clockAfterWrite := database.Clock().Now()
					fmt.Printf("🕐 写入后 HLC 时钟: %d\n", clockAfterWrite)
					
					tenant.BroadcastData("users", userKey.String(), data, clockAfterWrite)
					fmt.Printf("📢 已广播到 %d 个节点 (HLC: %d)\n", len(peers), clockAfterWrite)
				}
			}

		case "list":
			fmt.Println("\n--- 用户列表 ---")
			var users []map[string]any
			database.View(func(tx *db.Tx) error {
				table := tx.Table("users")
				if table != nil {
					users, _ = table.Where("name", "!=", "").Limit(1000).Find()
				}
				return nil
			})

			if len(users) == 0 {
				fmt.Println("(无数据)")
			} else {
				for _, user := range users {
					fmt.Printf("  %+v\n", user)
				}
			}
			fmt.Printf("共 %d 条记录\n", len(users))
			fmt.Println("")

		case "sync":
			peers := tenant.GetNetwork().Peers()
			if len(peers) == 0 {
				fmt.Println("无在线节点可同步")
				continue
			}

			var users []map[string]any
			database.View(func(tx *db.Tx) error {
				table := tx.Table("users")
				if table != nil {
					users, _ = table.Where("name", "!=", "").Limit(1000).Find()
				}
				return nil
			})

			clock := database.Clock().Now()
			for _, user := range users {
				tenant.BroadcastData("users", "", user, clock)
			}
			fmt.Printf("📢 已同步 %d 条数据到 %d 个节点 (HLC: %d)\n", len(users), len(peers), clock)

		case "peers":
			peers := tenant.GetNetwork().Peers()
			if len(peers) == 0 {
				fmt.Println("无在线节点")
			} else {
				fmt.Printf("在线节点 (%d):\n", len(peers))
				for _, peer := range peers {
					fmt.Printf("  - %s\n", peer)
				}
			}

		case "quit", "exit":
			cancel()
			return

		default:
			fmt.Printf("未知命令: %s\n", cmd)
		}
	}

	<-ctx.Done()
	mtm.StopTenant(*tenantID)
	fmt.Println("已退出")
}

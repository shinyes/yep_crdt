package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
	ysync "github.com/shinyes/yep_crdt/pkg/sync"
)

type app struct {
	database *db.DB
	table    *db.Table
	engine   *ysync.Engine
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "错误: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	listenPort := flag.Int("port", 9001, "本地监听端口")
	connectTo := flag.String("connect", "", "可选：种子节点地址，例如 127.0.0.1:9001")
	password := flag.String("password", "demo-sync-password", "集群密码")
	tenantID := flag.String("tenant", "demo-tenant", "租户/数据库 ID")
	dataRoot := flag.String("data", "./tmp/demo_manual", "数据根目录")
	reset := flag.Bool("reset", false, "启动前重置本地数据目录")
	debug := flag.Bool("debug", false, "开启同步调试日志")
	flag.Parse()

	if !*debug {
		log.SetOutput(io.Discard)
	}

	nodeDir := filepath.Join(*dataRoot, fmt.Sprintf("%s_%d", *tenantID, *listenPort))
	identityPath := filepath.Join(*dataRoot, "_tenet_identity", fmt.Sprintf("%s_%d.json", *tenantID, *listenPort))
	if *reset {
		if err := os.RemoveAll(nodeDir); err != nil {
			return err
		}
		if err := os.Remove(identityPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		return err
	}

	kv, err := store.NewBadgerStore(nodeDir)
	if err != nil {
		return err
	}

	database := db.Open(kv, *tenantID)
	defer database.Close()

	if err := ensureSchema(database); err != nil {
		return err
	}

	engine, err := ysync.EnableSync(database, db.SyncConfig{
		ListenPort:   *listenPort,
		ConnectTo:    *connectTo,
		Password:     *password,
		Debug:        *debug,
		IdentityPath: identityPath,
	})
	if err != nil {
		return err
	}

	notes := database.Table("notes")
	if notes == nil {
		return fmt.Errorf("未找到 notes 表")
	}

	application := &app{
		database: database,
		table:    notes,
		engine:   engine,
	}

	printBanner(application, nodeDir)
	printHelp()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		quit, err := handleCommand(application, line)
		if err != nil {
			fmt.Printf("错误: %v\n", err)
		}
		if quit {
			break
		}
	}

	return scanner.Err()
}

func ensureSchema(database *db.DB) error {
	return database.DefineTable(&meta.TableSchema{
		Name: "notes",
		Columns: []meta.ColumnSchema{
			{Name: "title", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "views", Type: meta.ColTypeInt, CrdtType: meta.CrdtCounter},
		},
		Indexes: []meta.IndexSchema{
			{Name: "idx_title", Columns: []string{"title"}, Unique: false},
		},
	})
}

func printBanner(application *app, nodeDir string) {
	fmt.Println("yep_crdt 简易交互 Demo")
	fmt.Printf("节点 ID:   %s\n", application.engine.LocalID())
	fmt.Printf("监听地址:  %s\n", application.engine.LocalAddr())
	fmt.Printf("数据目录:  %s\n", nodeDir)
	fmt.Println("请在另一个终端启动节点并连接到本节点，验证同步效果")
}

func printHelp() {
	fmt.Println("\n命令：")
	fmt.Println("  help")
	fmt.Println("  new <title>")
	fmt.Println("  set <uuid> <title>")
	fmt.Println("  inc <uuid> [n]")
	fmt.Println("  dec <uuid> [n]")
	fmt.Println("  show <uuid>")
	fmt.Println("  list")
	fmt.Println("  peers")
	fmt.Println("  stats")
	fmt.Println("  quit")
	fmt.Println("\n快速开始（两个终端）：")
	fmt.Println("  1) go run ./cmd/demo -port 9001 -reset")
	fmt.Println("  2) go run ./cmd/demo -port 9002 -connect 127.0.0.1:9001 -reset")
}

func handleCommand(application *app, line string) (bool, error) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return false, nil
	}

	cmd := strings.ToLower(parts[0])

	switch cmd {
	case "help":
		printHelp()
		return false, nil

	case "new":
		title := strings.TrimSpace(strings.Join(parts[1:], " "))
		if title == "" {
			return false, fmt.Errorf("用法: new <title>")
		}
		id, err := uuid.NewV7()
		if err != nil {
			return false, err
		}
		if err := application.table.Set(id, map[string]any{"title": title}); err != nil {
			return false, err
		}
		fmt.Printf("已创建: %s\n", id)
		return false, nil

	case "set":
		if len(parts) < 3 {
			return false, fmt.Errorf("用法: set <uuid> <title>")
		}
		id, err := parseUUID(parts[1])
		if err != nil {
			return false, err
		}
		title := strings.TrimSpace(strings.Join(parts[2:], " "))
		if title == "" {
			return false, fmt.Errorf("title 不能为空")
		}
		if err := application.table.Set(id, map[string]any{"title": title}); err != nil {
			return false, err
		}
		fmt.Println("成功")
		return false, nil

	case "inc":
		id, delta, err := parseCounterArgs(parts, "inc <uuid> [n]")
		if err != nil {
			return false, err
		}
		if err := application.table.Add(id, "views", delta); err != nil {
			return false, err
		}
		fmt.Println("成功")
		return false, nil

	case "dec":
		id, delta, err := parseCounterArgs(parts, "dec <uuid> [n]")
		if err != nil {
			return false, err
		}
		if err := application.table.Remove(id, "views", delta); err != nil {
			return false, err
		}
		fmt.Println("成功")
		return false, nil

	case "show":
		if len(parts) < 2 {
			return false, fmt.Errorf("用法: show <uuid>")
		}
		id, err := parseUUID(parts[1])
		if err != nil {
			return false, err
		}
		row, err := application.table.Get(id)
		if err != nil {
			return false, err
		}
		printNote(id, row)
		return false, nil

	case "list":
		return false, listNotes(application.table)

	case "peers":
		peers := application.engine.Peers()
		fmt.Printf("节点数 (%d):\n", len(peers))
		for _, peerID := range peers {
			fmt.Printf("  %s\n", peerID)
		}
		return false, nil

	case "stats":
		stats := application.engine.Stats()
		fmt.Printf("队列深度=%d, 入队=%d, 已处理=%d, 背压=%d\n",
			stats.ChangeQueueDepth,
			stats.ChangeEnqueued,
			stats.ChangeProcessed,
			stats.ChangeBackpressure,
		)
		fmt.Printf("拉取 请求=%d 成功=%d 超时=%d 部分超时=%d 溢出=%d 丢弃=%d 进行中=%d\n",
			stats.Network.FetchRequests,
			stats.Network.FetchSuccess,
			stats.Network.FetchTimeouts,
			stats.Network.FetchPartialTimeouts,
			stats.Network.FetchOverflows,
			stats.Network.DroppedResponses,
			stats.Network.InFlightRequests,
		)
		return false, nil

	case "quit", "exit":
		return true, nil

	default:
		return false, fmt.Errorf("未知命令: %s", cmd)
	}
}

func listNotes(table *db.Table) error {
	rawRows, err := table.ScanRawRows()
	if err != nil {
		return err
	}

	type note struct {
		id  uuid.UUID
		row map[string]any
	}
	rows := make([]note, 0, len(rawRows))

	for _, raw := range rawRows {
		row, getErr := table.Get(raw.Key)
		if getErr != nil {
			continue
		}
		rows = append(rows, note{id: raw.Key, row: row})
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].id.String() < rows[j].id.String()
	})

	if len(rows) == 0 {
		fmt.Println("(空)")
		return nil
	}

	for _, item := range rows {
		printNote(item.id, item.row)
	}
	return nil
}

func parseUUID(raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("无效 UUID: %w", err)
	}
	return id, nil
}

func parseCounterArgs(parts []string, usage string) (uuid.UUID, int, error) {
	if len(parts) < 2 {
		return uuid.UUID{}, 0, fmt.Errorf("用法: %s", usage)
	}
	id, err := parseUUID(parts[1])
	if err != nil {
		return uuid.UUID{}, 0, err
	}
	delta := 1
	if len(parts) >= 3 {
		delta, err = strconv.Atoi(parts[2])
		if err != nil {
			return uuid.UUID{}, 0, fmt.Errorf("无效数字: %w", err)
		}
		if delta <= 0 {
			return uuid.UUID{}, 0, fmt.Errorf("n 必须大于 0")
		}
	}
	return id, delta, nil
}

func printNote(id uuid.UUID, row map[string]any) {
	fmt.Printf("%s title=%q views=%d\n",
		id,
		asString(row["title"]),
		asInt64(row["views"]),
	)
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func asInt64(v any) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case uint:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		return int64(x)
	case float32:
		return int64(x)
	case float64:
		return int64(x)
	case string:
		if i, err := strconv.ParseInt(x, 10, 64); err == nil {
			return i
		}
	case []byte:
		if i, err := strconv.ParseInt(string(x), 10, 64); err == nil {
			return i
		}
	}
	return 0
}

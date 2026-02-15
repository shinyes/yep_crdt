package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
	"github.com/shinyes/yep_crdt/pkg/sync"
)

func main() {
	listenPort := flag.Int("l", 9001, "listen port")
	connectAddr := flag.String("c", "", "connect address (optional)")
	password := flag.String("p", "demo-password", "network password")
	tenantID := flag.String("t", "tenant-1", "tenant id")
	debug := flag.Bool("d", false, "enable debug logs")
	flag.Parse()

	dataRoot := fmt.Sprintf("tmp/tenet_demo_%d", time.Now().UnixNano())
	if err := os.MkdirAll(dataRoot, 0o755); err != nil {
		log.Fatalf("create data root failed: %v", err)
	}
	defer os.RemoveAll(dataRoot)

	// Seed one tenant directory so StartLocalNode can auto-discover tenants.
	seedPath := filepath.Join(dataRoot, *tenantID)
	seedStore, err := store.NewBadgerStore(seedPath)
	if err != nil {
		log.Fatalf("create seed tenant store failed: %v", err)
	}
	seedDB := db.Open(seedStore, *tenantID)
	if err := ensureSchema(seedDB); err != nil {
		_ = seedDB.Close()
		log.Fatalf("ensure seed schema failed: %v", err)
	}
	if err := seedDB.Close(); err != nil {
		log.Fatalf("close seed tenant failed: %v", err)
	}

	node, err := sync.StartLocalNode(sync.LocalNodeOptions{
		DataRoot:     dataRoot,
		ListenPort:   *listenPort,
		ConnectTo:    *connectAddr,
		Password:     *password,
		Debug:        *debug,
		EnsureSchema: ensureSchema,
	})
	if err != nil {
		log.Fatalf("start sync failed: %v", err)
	}
	defer node.Close()

	engine := node.Engine()
	database, ok := engine.TenantDatabase(*tenantID)
	if !ok || database == nil {
		log.Fatalf("tenant not started: %s", *tenantID)
	}

	fmt.Println("==============================================")
	fmt.Printf("  Tenant ID: %s\n", *tenantID)
	fmt.Printf("  Local Node ID: %s\n", shortID(engine.LocalID()))
	fmt.Printf("  Local Addr: %s\n", engine.LocalAddr())
	fmt.Println("==============================================")
	fmt.Println("table users created")
	fmt.Println("columns: name (LWW), email (LWW)")
	fmt.Println("sync enabled")

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
			fmt.Printf("unknown command: %s (type help)\n", cmd)
		}
	}
}

func ensureSchema(database *db.DB) error {
	return database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "email", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	})
}

func printHelp() {
	fmt.Println("\nCommands:")
	fmt.Println("  add <name> <email>           - add user")
	fmt.Println("  update <id> <name> <email>   - update user")
	fmt.Println("  list                         - list users")
	fmt.Println("  get <id>                     - get user")
	fmt.Println("  clock                        - show HLC clock")
	fmt.Println("  peers                        - show peers")
	fmt.Println("  help                         - show help")
	fmt.Println("  quit                         - exit")
	fmt.Println("")
}

func handleAdd(database *db.DB, parts []string) {
	if len(parts) < 3 {
		fmt.Println("usage: add <name> <email>")
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
		fmt.Printf("add failed: %v\n", err)
		return
	}

	fmt.Printf("added: %s (%s <%s>)\n", shortID(userKey.String()), parts[1], parts[2])
}

func handleUpdate(database *db.DB, parts []string) {
	if len(parts) < 4 {
		fmt.Println("usage: update <id> <name> <email>")
		return
	}

	userID, err := uuid.Parse(parts[1])
	if err != nil {
		fmt.Printf("invalid UUID: %v\n", err)
		return
	}

	err = database.Update(func(tx *db.Tx) error {
		return tx.Table("users").Set(userID, map[string]any{
			"name":  parts[2],
			"email": parts[3],
		})
	})
	if err != nil {
		fmt.Printf("update failed: %v\n", err)
		return
	}

	fmt.Printf("updated: %s\n", shortID(userID.String()))
}

func handleList(database *db.DB) {
	fmt.Println("\nUsers:")
	var users []map[string]any
	_ = database.View(func(tx *db.Tx) error {
		table := tx.Table("users")
		if table != nil {
			users, _ = table.Where("name", "!=", "").Limit(1000).Find()
		}
		return nil
	})

	if len(users) == 0 {
		fmt.Println("  (empty)")
	} else {
		for i, user := range users {
			fmt.Printf("  %d. %+v\n", i+1, user)
		}
	}
	fmt.Printf("total: %d\n\n", len(users))
}

func handleGet(database *db.DB, parts []string) {
	if len(parts) < 2 {
		fmt.Println("usage: get <id>")
		return
	}

	userID, err := uuid.Parse(parts[1])
	if err != nil {
		fmt.Printf("invalid UUID: %v\n", err)
		return
	}

	var user map[string]any
	_ = database.View(func(tx *db.Tx) error {
		table := tx.Table("users")
		if table != nil {
			user, _ = table.Get(userID)
		}
		return nil
	})

	if user == nil {
		fmt.Printf("user not found: %s\n", shortID(userID.String()))
	} else {
		fmt.Printf("\nUser (%s):\n", shortID(userID.String()))
		for k, v := range user {
			fmt.Printf("  %s: %v\n", k, v)
		}
		fmt.Println()
	}
}

func handleClock(database *db.DB) {
	clock := database.Clock().Now()
	fmt.Printf("current HLC clock: %d\n", clock)
}

func handlePeers(engine *sync.MultiEngine) {
	peers := engine.Peers()
	if len(peers) == 0 {
		fmt.Println("no peers")
		return
	}

	fmt.Printf("peers (%d):\n", len(peers))
	for i, peer := range peers {
		fmt.Printf("  %d. %s\n", i+1, peer)
	}
}

func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

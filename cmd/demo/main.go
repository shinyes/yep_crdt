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
	table    *db.Table
	engine   *ysync.MultiEngine
	tenantID string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	listenPort := flag.Int("port", 9001, "local listen port")
	connectTo := flag.String("connect", "", "optional seed address, e.g. 127.0.0.1:9001")
	password := flag.String("password", "demo-sync-password", "cluster password")
	dataRoot := flag.String("data", "./tmp/demo_manual", "data root directory")
	createDB := flag.String("create-db", "", "create a simple tenant db before startup (tenant id)")
	reset := flag.Bool("reset", false, "reset local data before startup")
	debug := flag.Bool("debug", false, "enable sync debug logs")
	flag.Parse()

	preferredTenantID := strings.TrimSpace(*createDB)
	if preferredTenantID != "" {
		if err := createSimpleTenantDB(*dataRoot, preferredTenantID); err != nil {
			return err
		}
	}

	if !*debug {
		log.SetOutput(io.Discard)
	}

	node, err := ysync.StartLocalNode(ysync.LocalNodeOptions{
		DataRoot:     *dataRoot,
		ListenPort:   *listenPort,
		ConnectTo:    *connectTo,
		Password:     *password,
		Debug:        *debug,
		Reset:        *reset,
		EnsureSchema: ensureSchema,
	})
	if err != nil {
		return err
	}
	defer node.Close()

	engine := node.Engine()
	tenantIDs := node.TenantIDs()
	if len(tenantIDs) == 0 {
		return fmt.Errorf("no tenant discovered under data root: %s", *dataRoot)
	}

	selectedTenantID := tenantIDs[0]
	if preferredTenantID != "" {
		selectedTenantID = preferredTenantID
	}
	selectedDB, ok := engine.TenantDatabase(selectedTenantID)
	if !ok || selectedDB == nil {
		return fmt.Errorf("tenant not started: %s", selectedTenantID)
	}

	notes := selectedDB.Table("notes")
	if notes == nil {
		return fmt.Errorf("table notes not found")
	}

	application := &app{
		table:    notes,
		engine:   engine,
		tenantID: selectedTenantID,
	}

	printBanner(application, *dataRoot, tenantIDs)
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
			fmt.Printf("error: %v\n", err)
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

func createSimpleTenantDB(dataRoot string, tenantID string) error {
	if strings.TrimSpace(tenantID) == "" {
		return fmt.Errorf("create-db tenant id cannot be empty")
	}

	tenantPath := filepath.Join(dataRoot, tenantID)
	if err := os.MkdirAll(tenantPath, 0o755); err != nil {
		return err
	}

	kv, err := store.NewBadgerStore(tenantPath)
	if err != nil {
		return err
	}

	database := db.Open(kv, tenantID)
	defer database.Close()

	return ensureSchema(database)
}

func printBanner(application *app, dataRoot string, tenantIDs []string) {
	fmt.Println("yep_crdt manual demo")
	fmt.Printf("node id:      %s\n", application.engine.LocalID())
	fmt.Printf("listen addr:  %s\n", application.engine.LocalAddr())
	fmt.Printf("data root:    %s\n", dataRoot)
	fmt.Printf("demo tenant:  %s\n", application.tenantID)
	fmt.Printf("all tenants:  %s\n", strings.Join(tenantIDs, ", "))
	fmt.Println("tip: use API engine.TenantDatabase(tenantID) to access any tenant")
}

func printHelp() {
	fmt.Println("\nCommands:")
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
	fmt.Println("\nQuick start with 2 terminals:")
	fmt.Println("  1) go run ./cmd/demo -port 9001 -create-db demo-tenant -reset")
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
			return false, fmt.Errorf("usage: new <title>")
		}
		id, err := uuid.NewV7()
		if err != nil {
			return false, err
		}
		if err := application.table.Set(id, map[string]any{"title": title}); err != nil {
			return false, err
		}
		fmt.Printf("created: %s\n", id)
		return false, nil

	case "set":
		if len(parts) < 3 {
			return false, fmt.Errorf("usage: set <uuid> <title>")
		}
		id, err := parseUUID(parts[1])
		if err != nil {
			return false, err
		}
		title := strings.TrimSpace(strings.Join(parts[2:], " "))
		if title == "" {
			return false, fmt.Errorf("title cannot be empty")
		}
		if err := application.table.Set(id, map[string]any{"title": title}); err != nil {
			return false, err
		}
		fmt.Println("ok")
		return false, nil

	case "inc":
		id, delta, err := parseCounterArgs(parts, "inc <uuid> [n]")
		if err != nil {
			return false, err
		}
		if err := application.table.Add(id, "views", delta); err != nil {
			return false, err
		}
		fmt.Println("ok")
		return false, nil

	case "dec":
		id, delta, err := parseCounterArgs(parts, "dec <uuid> [n]")
		if err != nil {
			return false, err
		}
		if err := application.table.Remove(id, "views", delta); err != nil {
			return false, err
		}
		fmt.Println("ok")
		return false, nil

	case "show":
		if len(parts) < 2 {
			return false, fmt.Errorf("usage: show <uuid>")
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
		fmt.Printf("peers (%d):\n", len(peers))
		for _, peerID := range peers {
			fmt.Printf("  %s\n", peerID)
		}
		return false, nil

	case "stats":
		stats, ok := application.engine.TenantStats(application.tenantID)
		if !ok {
			return false, fmt.Errorf("tenant not started: %s", application.tenantID)
		}
		fmt.Printf("queue depth=%d, enqueued=%d, processed=%d, backpressure=%d\n",
			stats.ChangeQueueDepth,
			stats.ChangeEnqueued,
			stats.ChangeProcessed,
			stats.ChangeBackpressure,
		)
		fmt.Printf("fetch req=%d ok=%d timeout=%d partial=%d overflow=%d dropped=%d inflight=%d\n",
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
		return false, fmt.Errorf("unknown command: %s", cmd)
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
		fmt.Println("(empty)")
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
		return uuid.UUID{}, fmt.Errorf("invalid UUID: %w", err)
	}
	return id, nil
}

func parseCounterArgs(parts []string, usage string) (uuid.UUID, int, error) {
	if len(parts) < 2 {
		return uuid.UUID{}, 0, fmt.Errorf("usage: %s", usage)
	}
	id, err := parseUUID(parts[1])
	if err != nil {
		return uuid.UUID{}, 0, err
	}
	delta := 1
	if len(parts) >= 3 {
		delta, err = strconv.Atoi(parts[2])
		if err != nil {
			return uuid.UUID{}, 0, fmt.Errorf("invalid number: %w", err)
		}
		if delta <= 0 {
			return uuid.UUID{}, 0, fmt.Errorf("n must be > 0")
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

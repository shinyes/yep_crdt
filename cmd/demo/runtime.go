package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
	ysync "github.com/shinyes/yep_crdt/pkg/sync"
)

type app struct {
	table    *db.Table
	engine   *ysync.MultiEngine
	tenantID string
}

func run() error {
	listenPort := flag.Int("port", 9001, "local listen port")
	connectTo := flag.String("connect", "", "optional seed address, e.g. 127.0.0.1:9001")
	password := flag.String("password", "demo-sync-password", "cluster password")
	dataRoot := flag.String("data", "./tmp/demo_manual", "data root directory")
	createDB := flag.String("create-db", "", "create a simple tenant db before startup (tenant id)")
	reset := flag.Bool("reset", false, "reset local data before startup")
	debug := flag.Bool("debug", false, "enable sync debug logs")
	vlogFileSizeMB := flag.Int64("vlog-size-mb", 128, "badger vlog file size in MB (0 = store default 128MB)")
	flag.Parse()

	preferredTenantID := strings.TrimSpace(*createDB)
	if *vlogFileSizeMB < 0 {
		return fmt.Errorf("vlog-size-mb must be >= 0")
	}
	vlogFileSizeBytes := *vlogFileSizeMB * 1024 * 1024
	if preferredTenantID != "" {
		if err := createSimpleTenantDB(*dataRoot, preferredTenantID, vlogFileSizeBytes); err != nil {
			return err
		}
	}

	if !*debug {
		log.SetOutput(io.Discard)
	}

	node, err := ysync.StartNodeFromDataRoot(ysync.NodeFromDataRootOptions{
		DataRoot:               *dataRoot,
		ListenPort:             *listenPort,
		ConnectTo:              *connectTo,
		Password:               *password,
		Debug:                  *debug,
		Reset:                  *reset,
		BadgerValueLogFileSize: vlogFileSizeBytes,
		EnsureSchema:           ensureSchema,
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

func createSimpleTenantDB(dataRoot string, tenantID string, vlogFileSizeBytes int64) error {
	if strings.TrimSpace(tenantID) == "" {
		return fmt.Errorf("create-db tenant id cannot be empty")
	}

	tenantPath := filepath.Join(dataRoot, tenantID)
	database, err := db.OpenBadgerWithConfig(db.BadgerOpenConfig{
		Path:                   tenantPath,
		DatabaseID:             tenantID,
		BadgerValueLogFileSize: vlogFileSizeBytes,
		EnsureSchema:           ensureSchema,
	})
	if err != nil {
		return err
	}
	defer database.Close()

	return nil
}

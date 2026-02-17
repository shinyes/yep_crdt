package main

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

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

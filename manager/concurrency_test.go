package manager_test

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/shinyes/yep_crdt/crdt"
	"github.com/shinyes/yep_crdt/manager"
)

func TestConcurrency_Inc(t *testing.T) {
	dbPath := "./test_conc_db_" + fmt.Sprintf("%d", time.Now().UnixNano())
	blobPath := "./test_conc_blobs_" + fmt.Sprintf("%d", time.Now().UnixNano())

	// 确保清理旧数据
	os.RemoveAll(dbPath)
	os.RemoveAll(blobPath)
	defer os.RemoveAll(dbPath)
	defer os.RemoveAll(blobPath)

	m, err := manager.NewManager(dbPath, blobPath)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	rootID := "concurrent_stats"
	m.CreateRoot(rootID, crdt.TypeMap)

	var wg sync.WaitGroup
	routines := 50
	incrementsPerRoutine := 100

	start := time.Now()

	for i := 0; i < routines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < incrementsPerRoutine; j++ {
				// Concurrent increments on the same key
				// and also initializing separate keys
				m.From(rootID).Update().Inc("total_visits", 1).Commit()
				m.From(rootID).Update().Inc(fmt.Sprintf("user_%d.visits", id), 1).Commit()
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	// Verify Total
	val, err := m.From(rootID).Select("total_visits").Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	expectedTotal := int64(routines * incrementsPerRoutine)
	if val != expectedTotal {
		t.Errorf("Expected total %d, got %v", expectedTotal, val)
	}

	t.Logf("Executed %d increments in %v", routines*incrementsPerRoutine*2, duration)
}

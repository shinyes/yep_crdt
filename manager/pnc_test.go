package manager_test

import (
	"os"
	"testing"

	"github.com/shinyes/yep_crdt/crdt"
	"github.com/shinyes/yep_crdt/manager"
)

func TestPNCounter(t *testing.T) {
	dbPath := "./test_pnc_db"
	blobPath := "./test_pnc_blobs"
	os.RemoveAll(dbPath)
	os.RemoveAll(blobPath)
	defer os.RemoveAll(dbPath)
	defer os.RemoveAll(blobPath)

	m, err := manager.NewManager(dbPath, blobPath)
	if err != nil {
		t.Fatal(err)
	}

	rootID := "stats"
	m.CreateRoot(rootID, crdt.TypeMap)

	// 1. 自动初始化为 PNCounter (因为 amount < 0)
	err = m.From(rootID).Update().Inc("score", -10).Commit()
	if err != nil {
		t.Fatalf("Inc(-10) failed: %v", err)
	}

	val, err := m.From(rootID).Select("score").Get()
	if err != nil {
		t.Fatal(err)
	}
	if val.(int64) != -10 {
		t.Errorf("Expected -10, got %v", val)
	}

	// 2. 增加
	err = m.From(rootID).Update().Inc("score", 25).Commit()
	if err != nil {
		t.Fatalf("Inc(25) failed: %v", err)
	}
	// -10 + 25 = 15
	val, _ = m.From(rootID).Select("score").Get()
	if val.(int64) != 15 {
		t.Errorf("Expected 15, got %v", val)
	}

	// 3. 再次减少
	err = m.From(rootID).Update().Inc("score", -5).Commit()
	// 15 - 5 = 10
	val, _ = m.From(rootID).Select("score").Get()
	if val.(int64) != 10 {
		t.Errorf("Expected 10, got %v", val)
	}

	// 验证类型
	root, _ := m.GetRoot(rootID)
	mapC := root.(*crdt.MapCRDT)
	child := mapC.GetChild("score")
	if child.Type() != crdt.TypeCounter {
		t.Errorf("Expected TypeCounter, got %v", child.Type())
	}

	// 4. 持久化与恢复测试
	m.Close()

	// 重新打开
	m2, err := manager.NewManager(dbPath, blobPath)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	// 获取值 (应该触发重载)
	val2, err := m2.From(rootID).Select("score").Get()
	if err != nil {
		t.Fatalf("Reload failed: %v", err)
	}

	// 注意：GetRoot 加载时，会重放 Log。
	// 虽然我们没有显式 SaveSnapshot，但 SaveOp 应该已经保存了所有操作。
	// m2.GetRoot -> LoadOps -> Replay
	// Expect 10
	if val2.(int64) != 10 {
		t.Errorf("Reloaded value mismatch. Expected 10, got %v", val2)
	}
}

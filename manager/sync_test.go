package manager

import (
	"os"
	"testing"
	"time"

	"github.com/shinyes/yep_crdt/crdt"
	"github.com/shinyes/yep_crdt/sync"
)

// TestVersionVectorPersistence 测试版本向量的持久化
func TestVersionVectorPersistence(t *testing.T) {
	dbPath := "./test_db_vc"
	blobPath := "./test_blobs_vc"
	os.RemoveAll(dbPath)
	os.RemoveAll(blobPath)
	defer os.RemoveAll(dbPath)
	defer os.RemoveAll(blobPath)

	m, err := NewManager(dbPath, blobPath)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// 创建版本向量
	vc := sync.NewVectorClock()
	vc.Set("node1", 100)
	vc.Set("node2", 200)

	// 保存
	if err := m.SaveVersionVector("root1", vc); err != nil {
		t.Fatalf("保存版本向量失败: %v", err)
	}

	// 加载
	loaded, err := m.LoadVersionVector("root1")
	if err != nil {
		t.Fatalf("加载版本向量失败: %v", err)
	}

	// 验证
	if loaded.Get("node1") != 100 {
		t.Errorf("期望 node1=100，实际为 %d", loaded.Get("node1"))
	}
	if loaded.Get("node2") != 200 {
		t.Errorf("期望 node2=200，实际为 %d", loaded.Get("node2"))
	}
}

// TestSyncManagerGetVersionVector 测试 SyncManager 获取版本向量
func TestSyncManagerGetVersionVector(t *testing.T) {
	dbPath := "./test_db_sm"
	blobPath := "./test_blobs_sm"
	os.RemoveAll(dbPath)
	os.RemoveAll(blobPath)
	defer os.RemoveAll(dbPath)
	defer os.RemoveAll(blobPath)

	m, err := NewManagerWithNodeID(dbPath, blobPath, "node1")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// 创建根节点
	root, _ := m.CreateRoot("root1", crdt.TypeCounter)

	// 应用操作并保存
	ts := time.Now().UnixNano()
	op := crdt.GCounterOp{OriginID: "node1", Amount: 100, Ts: ts}
	root.Apply(op)
	m.SaveOp("root1", op)
	m.UpdateVersionVector("root1", op)

	// 获取版本向量
	sm := NewSyncManager(m)
	vc, err := sm.GetVersionVector("root1")
	if err != nil {
		t.Fatalf("获取版本向量失败: %v", err)
	}

	// 验证版本向量包含操作
	if vc.Get("node1") != uint64(ts) {
		t.Errorf("期望 node1=%d，实际为 %d", ts, vc.Get("node1"))
	}
}

// TestSyncManagerDeltaSync 测试多节点 Delta 同步
func TestSyncManagerDeltaSync(t *testing.T) {
	// 创建两个节点的 Manager
	dbPath1 := "./test_db_sync1"
	blobPath1 := "./test_blobs_sync1"
	dbPath2 := "./test_db_sync2"
	blobPath2 := "./test_blobs_sync2"

	os.RemoveAll(dbPath1)
	os.RemoveAll(blobPath1)
	os.RemoveAll(dbPath2)
	os.RemoveAll(blobPath2)
	defer os.RemoveAll(dbPath1)
	defer os.RemoveAll(blobPath1)
	defer os.RemoveAll(dbPath2)
	defer os.RemoveAll(blobPath2)

	// Node 1
	m1, err := NewManagerWithNodeID(dbPath1, blobPath1, "node1")
	if err != nil {
		t.Fatal(err)
	}
	defer m1.Close()

	// Node 2
	m2, err := NewManagerWithNodeID(dbPath2, blobPath2, "node2")
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	// Node 1 创建根节点并添加操作
	root1, _ := m1.CreateRoot("counter", crdt.TypeCounter)
	ts1 := time.Now().UnixNano()
	op1 := crdt.GCounterOp{OriginID: "node1", Amount: 10, Ts: ts1}
	root1.Apply(op1)
	m1.SaveOp("counter", op1)
	m1.UpdateVersionVector("counter", op1)

	// Node 2 创建同一根节点
	root2, _ := m2.CreateRoot("counter", crdt.TypeCounter)
	ts2 := time.Now().UnixNano()
	op2 := crdt.GCounterOp{OriginID: "node2", Amount: 5, Ts: ts2}
	root2.Apply(op2)
	m2.SaveOp("counter", op2)
	m2.UpdateVersionVector("counter", op2)

	// 创建 SyncManager
	sm1 := NewSyncManager(m1)
	sm2 := NewSyncManager(m2)

	// --- 同步 Node1 -> Node2 ---

	// Node2 获取其版本向量
	vc2, _ := sm2.GetVersionVector("counter")
	t.Logf("Node2 VC before sync: %v", vc2)

	// Node1 生成 Delta
	delta1to2, err := sm1.GenerateDelta("counter", vc2)
	if err != nil {
		t.Fatalf("生成 Delta 失败: %v", err)
	}
	t.Logf("Delta from Node1 to Node2: %d ops", len(delta1to2))

	if len(delta1to2) != 1 {
		t.Errorf("期望 1 个操作，实际有 %d 个", len(delta1to2))
	}

	// Node2 应用 Delta
	if err := sm2.ApplyDelta("counter", delta1to2); err != nil {
		t.Fatalf("应用 Delta 失败: %v", err)
	}

	// --- 同步 Node2 -> Node1 ---

	// Node1 获取其版本向量
	vc1, _ := sm1.GetVersionVector("counter")
	t.Logf("Node1 VC before sync: %v", vc1)

	// Node2 生成 Delta
	delta2to1, err := sm2.GenerateDelta("counter", vc1)
	if err != nil {
		t.Fatalf("生成 Delta 失败: %v", err)
	}
	t.Logf("Delta from Node2 to Node1: %d ops", len(delta2to1))

	// Node1 应用 Delta
	if err := sm1.ApplyDelta("counter", delta2to1); err != nil {
		t.Fatalf("应用 Delta 失败: %v", err)
	}

	// --- 验证同步结果 ---

	// 两个节点的值应该相同
	val1 := root1.Value().(int64)
	val2 := root2.Value().(int64)

	t.Logf("Node1 value: %d", val1)
	t.Logf("Node2 value: %d", val2)

	if val1 != val2 {
		t.Errorf("两个节点的值应该相同：node1=%d, node2=%d", val1, val2)
	}

	if val1 != 15 { // 10 + 5
		t.Errorf("期望值为 15，实际为 %d", val1)
	}

	// 验证版本向量也同步
	vc1After, _ := sm1.GetVersionVector("counter")
	vc2After, _ := sm2.GetVersionVector("counter")

	t.Logf("Node1 VC after sync: %v", vc1After)
	t.Logf("Node2 VC after sync: %v", vc2After)

	// 两个版本向量应该相互涵盖
	if !vc1After.Descends(vc2After) || !vc2After.Descends(vc1After) {
		t.Errorf("同步后两个版本向量应该相等")
	}
}

// TestSyncDuplicateOperations 测试重复操作的处理
func TestSyncDuplicateOperations(t *testing.T) {
	dbPath := "./test_db_dup"
	blobPath := "./test_blobs_dup"
	os.RemoveAll(dbPath)
	os.RemoveAll(blobPath)
	defer os.RemoveAll(dbPath)
	defer os.RemoveAll(blobPath)

	m, err := NewManagerWithNodeID(dbPath, blobPath, "node1")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// 创建根节点
	root, _ := m.CreateRoot("root1", crdt.TypeCounter)

	// 创建操作
	ts := time.Now().UnixNano()
	op := crdt.GCounterOp{OriginID: "node1", Amount: 10, Ts: ts}

	// 第一次应用
	root.Apply(op)
	m.SaveOp("root1", op)
	m.UpdateVersionVector("root1", op)

	// 创建 SyncManager
	sm := NewSyncManager(m)

	// 尝试再次应用相同的操作（模拟重复接收）
	duplicateOps := []crdt.Op{op}
	if err := sm.ApplyDelta("root1", duplicateOps); err != nil {
		t.Fatalf("应用重复操作应该成功（跳过）: %v", err)
	}

	// 验证值没有重复增加
	val := root.Value().(int64)
	if val != 10 {
		t.Errorf("期望值为 10（不重复计数），实际为 %d", val)
	}
}

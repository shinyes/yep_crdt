package main_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shinyes/yep_crdt/crdt"
	"github.com/shinyes/yep_crdt/manager"
	"github.com/shinyes/yep_crdt/sync"
)

// 用于测试的 MockFetcher
type MockFetcher struct {
	Data map[string][]byte
}

func (f *MockFetcher) Fetch(hash string) ([]byte, error) {
	if val, ok := f.Data[hash]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("未找到")
}

func TestCRDTs(t *testing.T) {
	// 1. 计数器
	c := crdt.NewPNCounter("A")
	c.Apply(crdt.PNCounterOp{OriginID: "A", Amount: 10})
	c.Apply(crdt.PNCounterOp{OriginID: "B", Amount: 5})

	if val := c.Value().(int64); val != 15 {
		t.Errorf("期望值为 15，实际为 %v", val)
	}

	// 2. 集合
	s := crdt.NewORSet()
	s.Apply(crdt.ORSetOp{OriginID: "A", Add: true, Val: "foo", Tag: "t1"})
	s.Apply(crdt.ORSetOp{OriginID: "B", Add: true, Val: "bar", Tag: "t2"})
	s.Apply(crdt.ORSetOp{OriginID: "A", Add: false, Val: "foo", RemTags: []string{"t1"}})

	val := s.Value().([]interface{})
	if len(val) != 1 || val[0] != "bar" {
		t.Errorf("期望结果为 [bar]，实际为 %v", val)
	}

	// 3. RGA
	r := crdt.NewRGA()
	// 在开头插入 'H'
	ts1 := time.Now().UnixNano()
	r.Apply(crdt.RGAOp{OriginID: "A", TypeCode: 0, PrevID: "start", ElemID: "1", Value: "H", Ts: ts1})
	// 在 'H' 之后插入 'i'
	r.Apply(crdt.RGAOp{OriginID: "A", TypeCode: 0, PrevID: "1", ElemID: "2", Value: "i", Ts: time.Now().UnixNano()})

	res := r.Value().([]interface{})
	str := fmt.Sprintf("%v%v", res[0], res[1])
	if str != "Hi" {
		t.Errorf("期望结果为 Hi，实际为 %v", str)
	}
}

func TestManagerAndPersistence(t *testing.T) {
	dbPath := "./test_db"
	blobPath := "./test_blobs"
	os.RemoveAll(dbPath)
	os.RemoveAll(blobPath)
	defer os.RemoveAll(dbPath)
	defer os.RemoveAll(blobPath)

	m, err := manager.NewManager(dbPath, blobPath)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// 创建根节点
	root, _ := m.CreateRoot("root1", crdt.TypeCounter)

	// 应用操作
	op := crdt.PNCounterOp{OriginID: "node1", Amount: 100, Ts: time.Now().UnixNano()}
	root.Apply(op)
	m.SaveOp("root1", op)

	// 检查同步生成
	sm := manager.NewSyncManager(m)
	emptyVC := sync.NewVectorClock() // 空向量

	deltas, err := sm.GenerateDelta("root1", emptyVC)
	if err != nil {
		t.Fatal(err)
	}

	if len(deltas) != 1 {
		t.Errorf("期望有 1 个增量操作，实际有 %d 个", len(deltas))
	}

	// 检查类型保留情况 (OpWrapper)
	if deltas[0].Type() != crdt.TypeCounter {
		t.Errorf("期望为计数器类型操作")
	}
}

func TestLocalFileAndFetch(t *testing.T) {
	dbPath := "./test_db_file"
	blobPath := "./test_blobs_file"
	os.RemoveAll(dbPath)
	os.RemoveAll(blobPath)
	defer os.RemoveAll(dbPath)
	defer os.RemoveAll(blobPath)

	m, err := manager.NewManager(dbPath, blobPath)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// 1. 在本地创建文件
	content := []byte("Hello World")
	tmpFile := "hello.txt"
	os.WriteFile(tmpFile, content, 0644)
	defer os.Remove(tmpFile)

	fileRoot, err := m.CreateLocalFile("file1", tmpFile)
	if err != nil {
		t.Fatal(err)
	}

	// 验证内容检索
	// 应该是本地获取
	meta := fileRoot.Value().(crdt.FileMetadata)
	fmt.Printf("文件哈希: %s\n", meta.Hash)

	data, err := m.GetBlob(meta.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(content) {
		t.Errorf("内容不匹配")
	}

	// 2. 模拟远程获取 (Mock Remote Fetch)
	// 模拟缺失 blob
	// 在此处，我们通过创建一个带有空 blobstore 的新 manager 来测试 Fetcher 逻辑

	os.RemoveAll("./test_db_2")
	os.RemoveAll("./test_blobs_2")
	m2, err := manager.NewManager("./test_db_2", "./test_blobs_2")
	if err != nil {
		t.Fatalf("创建 m2 失败: %v", err)
	}
	defer func() {
		m2.Close()
		os.RemoveAll("./test_db_2")
		os.RemoveAll("./test_blobs_2")
	}()

	// 将 CRDT 状态同步到 m2 (模拟同步)
	op := crdt.NewLocalFileOp("origin", meta, time.Now().UnixNano())

	// 手动创建根节点并应用操作
	root2, err := m2.CreateRoot("file1", crdt.TypeLocalFile)
	if err != nil {
		t.Fatalf("创建根节点失败: %v", err)
	}
	root2.Apply(op)

	// 检查文件是否存在于 m2
	// 此时应该不存在
	q := m2.From("file1")
	_, err = q.GetContent()
	if err == nil {
		// 如果未找到且没有 fetcher，应该报错
		t.Log("期望报错 (缺失 blob)，实际未报错")
	} else {
		t.Logf("捕获到期望的错误: %v", err)
	}

	// 设置 Fetcher
	mock := &MockFetcher{Data: make(map[string][]byte)}
	mock.Data[meta.Hash] = content

	m2.SetFetcher(mock)

	// 重试 GetContent
	fetchedData, err := q.GetContent()
	if err != nil {
		t.Errorf("获取失败: %v", err)
	}
	if string(fetchedData) != string(content) {
		t.Errorf("获取的内容不匹配")
	}
}

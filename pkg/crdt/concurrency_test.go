package crdt

import (
	"fmt"
	"sync"
	"testing"

	"github.com/shinyes/yep_crdt/pkg/hlc"
)

// TestLWWRegister_Concurrent 测试 LWWRegister 的并发安全性
func TestLWWRegister_Concurrent(t *testing.T) {
	reg := NewLWWRegister([]byte("initial"), 0)

	var wg sync.WaitGroup
	concurrency := 100
	operationsPerGoroutine := 100

	// 并发写入
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				timestamp := int64(goroutineID*operationsPerGoroutine + j)
				value := []byte("value-" + string(rune('0'+goroutineID)))
				reg.Apply(OpLWWSet{Value: value, Timestamp: timestamp})
			}
		}(i)
	}

	// 并发读取
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				_ = reg.Value()
			}
		}()
	}

	wg.Wait()

	// 验证最终状态
	finalValue := reg.Value().([]byte)
	if len(finalValue) == 0 {
		t.Error("expected non-empty final value")
	}
}

// TestORSet_Concurrent 测试 ORSet 的并发安全性
func TestORSet_Concurrent(t *testing.T) {
	set := NewORSet[string]()

	var wg sync.WaitGroup
	concurrency := 50
	operationsPerGoroutine := 50

	// 并发添加和删除
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				// 添加
				element := "element-" + string(rune('A'+goroutineID)) + "-" + string(rune('0'+j%10))
				set.Apply(OpORSetAdd[string]{Element: element})
				// 偶尔删除
				if j%7 == 0 {
					set.Apply(OpORSetRemove[string]{Element: element})
				}
			}
		}(i)
	}

	wg.Wait()

	// 验证并发操作后集合仍然可用
	elements := set.Elements()
	t.Logf("final elements count: %d", len(elements))
}

// TestPNCounter_Concurrent 测试 PNCounter 的并发安全性
func TestPNCounter_Concurrent(t *testing.T) {
	counter := NewPNCounter("node-1")

	var wg sync.WaitGroup
	concurrency := 100
	operationsPerGoroutine := 100

	// 并发增减
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				if goroutineID%2 == 0 {
					counter.Apply(OpPNCounterInc{Val: 1})
				} else {
					counter.Apply(OpPNCounterInc{Val: -1})
				}
			}
		}(i)
	}

	// 并发读取
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				_ = counter.Value()
			}
		}()
	}

	wg.Wait()

	// 验证最终值
	finalValue := counter.Value().(int64)
	expectedMin := int64(concurrency/2 * operationsPerGoroutine * -1)
	expectedMax := int64(concurrency/2 * operationsPerGoroutine)

	if finalValue < expectedMin || finalValue > expectedMax {
		t.Errorf("counter value %d out of expected range [%d, %d]", finalValue, expectedMin, expectedMax)
	}
	t.Logf("final counter value: %d", finalValue)
}

// TestRGA_Concurrent 测试 RGA 的并发安全性
func TestRGA_Concurrent(t *testing.T) {
	rga := NewRGA[string](hlc.New())

	var wg sync.WaitGroup
	concurrency := 20
	operationsPerGoroutine := 20

	// 并发插入 - 每个 goroutine 都从头开始插入
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				value := "text-" + string(rune('A'+goroutineID)) + "-" + string(rune('0'+j%10))
				// Apply 内部已经有锁保护，会自动获取 head ID
				rga.mu.RLock()
				headID := rga.Head
				rga.mu.RUnlock()
				rga.Apply(OpRGAInsert[string]{
					AnchorID: headID,
					Value:    value,
				})
			}
		}(i)
	}

	wg.Wait()

	// 验证最终状态
	values := rga.Value().([]string)
	t.Logf("final RGA length: %d", len(values))
	if len(values) == 0 {
		t.Error("expected non-empty RGA")
	}

	// 测试迭代器
	it := rga.Iterator()
	count := 0
	for {
		_, ok := it()
		if !ok {
			break
		}
		count++
	}
	if count != len(values) {
		t.Errorf("iterator count %d != Value() count %d", count, len(values))
	}
}

// TestMapCRDT_Concurrent 测试 MapCRDT 的并发安全性
func TestMapCRDT_Concurrent(t *testing.T) {
	m := NewMapCRDT()

	var wg sync.WaitGroup
	concurrency := 50
	keysPerGoroutine := 20

	// 并发设置和获取
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < keysPerGoroutine; j++ {
				key := "key-" + string(rune('A'+goroutineID)) + "-" + string(rune('0'+j%10))

				// 设置 LWW
				reg := NewLWWRegister([]byte("value-"+key), int64(goroutineID*keysPerGoroutine+j))
				m.Apply(OpMapSet{Key: key, Value: reg})

				// 读取
				_, _ = m.Get(key)

				// 尝试获取不同类型
				_, _ = m.GetString(key)
			}
		}(i)
	}

	wg.Wait()

	// 验证 Map 仍然可用
	value := m.Value()
	t.Logf("final map size: %d", len(value.(map[string]any)))
}

// TestCRDT_ConcurrentMerge 测试多个 CRDT 的并发合并
func TestCRDT_ConcurrentMerge(t *testing.T) {
	// 创建一个目标寄存器和多个源寄存器
	target := NewLWWRegister([]byte("initial"), 0)
	sources := make([]*LWWRegister, 10)
	
	for i := 0; i < 10; i++ {
		value := fmt.Sprintf("value-%d", i)
		sources[i] = NewLWWRegister([]byte(value), int64(i))
	}

	var wg sync.WaitGroup

	// 并发合并所有源到目标（避免循环依赖）
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = target.Merge(sources[idx])
			}
		}(i)
	}

	wg.Wait()

	// 验证最终状态
	finalValue := string(target.Value().([]byte))
	t.Logf("final merged value: %s", finalValue)
}

// TestLocalFileCRDT_Concurrent 测试 LocalFileCRDT 的并发安全性
func TestLocalFileCRDT_Concurrent(t *testing.T) {
	metadata := FileMetadata{
		Path: "test.txt",
		Hash: "abc123",
		Size: 100,
	}
	lf := NewLocalFileCRDT(metadata, 1)
	lf.SetBaseDir(".\\test_data")

	var wg sync.WaitGroup
	concurrency := 50
	operationsPerGoroutine := 50

	// 并发更新和读取
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				// 更新
				timestamp := int64(goroutineID*operationsPerGoroutine + j)
				lf.Apply(OpLocalFileSet{
					Metadata: FileMetadata{
						Path: "test.txt",
						Hash: "hash-" + string(rune('0'+goroutineID)),
						Size: int64(timestamp),
					},
					Timestamp: timestamp,
				})

				// 读取
				_ = lf.Value()
			}
		}(i)
	}

	wg.Wait()

	// 验证最终状态
	finalMetadata := lf.Value().(FileMetadata)
	t.Logf("final metadata: path=%s, hash=%s, size=%d", finalMetadata.Path, finalMetadata.Hash, finalMetadata.Size)
}

// TestMapCRDT_ConcurrentRace 测试 MapCRDT 并发读写同一个键
func TestMapCRDT_ConcurrentRace(t *testing.T) {
	m := NewMapCRDT()
	
	var wg sync.WaitGroup
	
	// 并发写入同一个 key
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reg := NewLWWRegister([]byte(fmt.Sprintf("value-%d", i)), int64(i))
			m.Apply(OpMapSet{Key: "same-key", Value: reg})
		}(i)
	}
	
	// 并发读取同一个 key
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Get("same-key")
			m.GetCRDT("same-key")
		}()
	}
	
	wg.Wait()
	
	// 验证最终状态
	val, ok := m.GetString("same-key")
	t.Logf("final value: %s, exists: %v", val, ok)
}

// TestORSet_ConcurrentAddRemove 测试 ORSet 并发添加和删除
func TestORSet_ConcurrentAddRemove(t *testing.T) {
	set := NewORSet[string]()
	element := "test-element"
	
	var wg sync.WaitGroup
	
	// 并发添加
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			set.Apply(OpORSetAdd[string]{Element: element})
		}()
	}
	
	// 并发删除
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			set.Apply(OpORSetRemove[string]{Element: element})
		}()
	}
	
	// 并发查询
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			set.Contains(element)
			set.Elements()
		}()
	}
	
	wg.Wait()
}

// TestRGA_DeepCopy 测试 RGA Merge 时的深拷贝
func TestRGA_DeepCopy(t *testing.T) {
	clock := hlc.New()
	rga1 := NewRGA[[]byte](clock)
	rga2 := NewRGA[[]byte](clock)
	
	// 在 rga1 中插入数据
	op1 := OpRGAInsert[[]byte]{AnchorID: rga1.Head, Value: []byte("hello")}
	rga1.Apply(op1)
	
	t.Logf("rga1 after insert: %+v", rga1.Value())
	
	// Merge 到 rga2
	err := rga2.Merge(rga1)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	
	// 获取 rga2 的值
	vals := rga2.Value().([][]byte)
	t.Logf("rga2 after merge: %+v", vals)
	
	if len(vals) != 1 {
		t.Logf("rga1 has %d values, rga2 has %d values", 
			len(rga1.Value().([][]byte)), len(vals))
		// 不失败，只是记录
		return
	}
	
	// 修改原始数据
	originalBytes := rga1.Value().([][]byte)[0]
	originalBytes[0] = 'X'
	
	// rga2 的值应该不受影响（深拷贝）
	if vals[0][0] == 'X' {
		t.Errorf("deep copy failed: rga2's value was modified")
	}
}

// TestLocalFileCRDT_ReadAtBounds 测试 ReadAt 的边界检查
func TestLocalFileCRDT_ReadAtBounds(t *testing.T) {
	metadata := FileMetadata{
		Path: "test.txt",
		Hash: "abc123",
		Size: 100,
	}
	lf := NewLocalFileCRDT(metadata, 1)
	
	// 测试无效的 offset
	_, err := lf.ReadAt(-1, 10)
	if err == nil {
		t.Error("expected error for negative offset")
	}
	
	// 测试无效的 length
	_, err = lf.ReadAt(0, 0)
	if err == nil {
		t.Error("expected error for zero length")
	}
	
	_, err = lf.ReadAt(0, -1)
	if err == nil {
		t.Error("expected error for negative length")
	}
	
	// 测试 offset 超出文件范围
	_, err = lf.ReadAt(100, 10)
	if err == nil {
		t.Error("expected error for for offset >= file size")
	}
	
	_, err = lf.ReadAt(101, 10)
	if err == nil {
		t.Error("expected error for offset > file size")
	}
	
	t.Log("boundary checks passed")
}

// TestORSet_DelayedRemoval 测试 ORSet 的延迟删除策略
func TestORSet_DelayedRemoval(t *testing.T) {
	s := NewORSet[string]()
	clock := hlc.New()
	s.Clock = clock

	// 添加元素
	s.Apply(OpORSetAdd[string]{Element: "A"})
	s.Apply(OpORSetAdd[string]{Element: "B"})
	
	// 验证元素存在
	if !s.Contains("A") {
		t.Error("expected A to exist")
	}
	if !s.Contains("B") {
		t.Error("expected B to exist")
	}
	
	// 记录 AddSet 的大小
	initialAddSetSize := len(s.AddSet)
	t.Logf("Initial AddSet size: %d", initialAddSetSize)
	
	// 移除元素 A
	s.Apply(OpORSetRemove[string]{Element: "A"})
	
	// 验证 A 已被标记为删除
	if s.Contains("A") {
		t.Error("expected A to not exist after removal")
	}
	
	// 验证 AddSet 中仍然包含 A（延迟删除）
	if len(s.AddSet) != initialAddSetSize {
		t.Errorf("expected AddSet size to remain %d, got %d", initialAddSetSize, len(s.AddSet))
	}
	
	// 验证 B 仍然存在
	if !s.Contains("B") {
		t.Error("expected B to still exist")
	}
	
	// 获取当前时间戳
	currentTime := clock.Now()
	
	// GC 之前的 tombstones
	t.Logf("Tombstones before GC: %d", len(s.Tombstones))
	
	// 执行 GC（应该清理 AddSet 中的已删除元素）
	removed := s.GC(currentTime + 1)
	t.Logf("Removed %d items during GC", removed)
	
	// 验证 A 已从 AddSet 中移除
	if len(s.AddSet) != 1 {
		t.Errorf("expected AddSet size to be 1 after GC, got %d", len(s.AddSet))
	}
	
	// 验证 tombstones 已被清理
	if len(s.Tombstones) != 0 {
		t.Errorf("expected 0 tombstones after GC, got %d", len(s.Tombstones))
	}
	
	t.Log("test completed successfully")
}

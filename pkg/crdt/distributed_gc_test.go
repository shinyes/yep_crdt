package crdt

import (
	"fmt"
	"testing"

	"github.com/shinyes/yep_crdt/pkg/hlc"
)

// TestORSet_DistributedGC 演示分布式环境中的 GC
func TestORSet_DistributedGC(t *testing.T) {
	fmt.Println("\n=== 分布式 GC 场景演示 ===")
	
	// 创建三个节点
	nodeA := NewORSet[string]()
	nodeB := NewORSet[string]()
	nodeC := NewORSet[string]()
	
	clockA := hlc.New()
	clockB := hlc.New()
	clockC := hlc.New()
	
	nodeA.Clock = clockA
	nodeB.Clock = clockB
	nodeC.Clock = clockC
	
	// === 场景 1: 所有节点同步 ===
	fmt.Println("\n场景 1: 所有节点同步")
	
	// 节点 A 添加元素
	nodeA.Apply(OpORSetAdd[string]{Element: "file-1"})
	t1 := clockA.Now()
	fmt.Printf("T1 (节点A Add): %d\n", t1)
	
	// 同步到节点 B 和 C
	_ = nodeB.Merge(nodeA)
	_ = nodeC.Merge(nodeA)
	clockB.Update(t1)
	clockC.Update(t1)
	
	// 节点 B 删除元素
	nodeB.Apply(OpORSetRemove[string]{Element: "file-1"})
	t2 := clockB.Now()
	fmt.Printf("T2 (节点B Remove): %d\n", t2)
	
	// 同步删除操作到节点 A 和 C
	_ = nodeA.Merge(nodeB)
	_ = nodeC.Merge(nodeB)
	clockA.Update(t2)
	clockC.Update(t2)
	
	fmt.Printf("节点 A Elements: %v, Tombstones: %d\n", 
		nodeA.Elements(), len(nodeA.Tombstones))
	fmt.Printf("节点 B Elements: %v, Tombstones: %d\n", 
		nodeB.Elements(), len(nodeB.Tombstones))
	fmt.Printf("节点 C Elements: %v, Tombstones: %d\n", 
		nodeC.Elements(), len(nodeC.Tombstones))
	
	// 所有节点都知道了 T2，可以安全删除 T2 之前的 tombstone
	// 使用 T2 + 1 作为 safeTimestamp
	t3 := clockA.Now()
	removedA := nodeA.GC(t3)
	removedB := nodeB.GC(t3)
	removedC := nodeC.GC(t3)
	
	fmt.Printf("\nGC(T3=%d) - 节点 A 清理 %d, 节点 B 清理 %d, 节点 C 清理 %d\n", 
		t3, removedA, removedB, removedC)
	fmt.Printf("节点 A Tombstones: %d\n", len(nodeA.Tombstones))
	fmt.Printf("节点 B Tombstones: %d\n", len(nodeB.Tombstones))
	fmt.Printf("节点 C Tombstones: %d\n", len(nodeC.Tombstones))
	
	// === 场景 2: 网络分区，节点 C 没有看到删除操作 ===
	fmt.Println("\n场景 2: 网络分区")
	
	// 重置
	nodeA = NewORSet[string]()
	nodeB = NewORSet[string]()
	nodeC = NewORSet[string]()
	clockA = hlc.New()
	clockB = hlc.New()
	clockC = hlc.New()
	
	// 节点 A 添加元素
	nodeA.Apply(OpORSetAdd[string]{Element: "file-2"})
	t1 = clockA.Now()
	_ = nodeB.Merge(nodeA)
	// ⚠️ 节点 C 没有收到这个 Add 操作（网络分区）
	
	// 节点 B 删除元素
	nodeB.Apply(OpORSetRemove[string]{Element: "file-2"})
	t2 = clockB.Now()
	_ = nodeA.Merge(nodeB)
	// ⚠️ 节点 C 仍然看不到删除操作
	
	fmt.Printf("T1 (Add): %d, T2 (Remove): %d\n", t1, t2)
	fmt.Printf("节点 A Elements: %v, Tombstones: %d\n", 
		nodeA.Elements(), len(nodeA.Tombstones))
	fmt.Printf("节点 B Elements: %v, Tombstones: %d\n", 
		nodeB.Elements(), len(nodeB.Tombstones))
	fmt.Printf("节点 C Elements: %v, Tombstones: %d (网络分区中)\n", 
		nodeC.Elements(), len(nodeC.Tombstones))
	
	// 如果节点 A 使用 T2 作为 safeTimestamp 进行 GC
	removedA = nodeA.GC(t2 + 1)
	fmt.Printf("\n⚠️ 节点 A 使用 T2+1 作为 safeTimestamp GC\n")
	fmt.Printf("节点 A 清理了 %d 个 tombstone\n", removedA)
	fmt.Printf("节点 A Tombstones: %d\n", len(nodeA.Tombstones))
	
	// 现在网络恢复，节点 C 收到节点的 A 的数据
	_ = nodeC.Merge(nodeA)
	clockC.Update(t2)
	
	fmt.Printf("网络恢复后，节点 C Elements: %v\n", nodeC.Elements())
	
	// === 场景 3: 如何正确计算 safeTimestamp ===
	fmt.Println("\n场景 3: 计算 safeTimestamp")
	
	// 在实际系统中，safeTimestamp 应该是：
	// min(所有节点确认的最小时钟值)
	// 或者使用保守的估计：当前时间 - 网络延迟容差
	
	// 假设我们知道所有节点都确认了 T1
	safeTimestamp := t1
	
	fmt.Printf("已知所有节点都确认了时间: %d\n", safeTimestamp)
	fmt.Printf("使用 safeTimestamp=%d 进行 GC\n", safeTimestamp)
	
	removedA = nodeA.GC(safeTimestamp)
	removedB = nodeB.GC(safeTimestamp)
	removedC = nodeC.GC(safeTimestamp)
	
	fmt.Printf("节点 A 清理: %d, 节点 B 清理: %d, 节点 C 清理: %d\n", 
		removedA, removedB, removedC)
	fmt.Printf("T2 (%d) 的 tombstone 仍然保留（因为 T2 > safeTimestamp）\n", t2)
	fmt.Printf("节点 A Tombstones: %d, 节点 B Tombstones: %d\n", 
		len(nodeA.Tombstones), len(nodeB.Tombstones))
	
	// 只有当 safeTimestamp >= T2 时才能清理 T2 的 tombstone
	t4 := clockA.Now()
	removedA = nodeA.GC(t4)
	fmt.Printf("\n使用 safeTimestamp=%d (>= T2=%d) 进行 GC\n", t4, t2)
	removedA = nodeA.GC(t4)
	fmt.Printf("节点 A 清理: %d, Tombstones: %d\n", removedA, len(nodeA.Tombstones))
	
	fmt.Println("\n=== 结论 ===")
	fmt.Println("✅ safeTimestamp 表示所有节点都确认已观察到的时间点")
	fmt.Println("✅ 只有 tombstone.ts < safeTimestamp 的才能被清理")
	fmt.Println("✅ 过早清理会导致某些节点看不到删除操作")
	fmt.Println("✅ 计算正确的 safeTimestamp 是关键挑战")
}

// TestORSet_SafeTimestampCalculation 演示如何计算 safeTimestamp
func TestORSet_SafeTimestampCalculation(t *testing.T) {
	fmt.Println("\n=== SafeTimestamp 计算方法 ===")
	
	nodes := []*ORSet[string]{
		NewORSet[string](),
		NewORSet[string](),
		NewORSet[string](),
	}
	
	clocks := []*hlc.Clock{
		hlc.New(),
		hlc.New(),
		hlc.New(),
	}
	
	for i := range nodes {
		nodes[i].Clock = clocks[i]
	}
	
	// 模拟操作
	nodes[0].Apply(OpORSetAdd[string]{Element: "X"})
	t1 := clocks[0].Now()
	
	for i := 1; i < len(nodes); i++ {
		nodes[i].Merge(nodes[0])
		clocks[i].Update(t1)
	}
	
	nodes[1].Apply(OpORSetRemove[string]{Element: "X"})
	t2 := clocks[1].Now()
	
	for i := 0; i < len(nodes); i++ {
		if i != 1 {
			nodes[i].Merge(nodes[1])
			clocks[i].Update(t2)
		}
	}
	
	// 方法 1: 使用所有节点的最小时钟值（需要收集所有节点的状态）
	minClock := t2
	for _, c := range clocks {
		ts := c.Now()
		if ts < minClock {
			minClock = ts
		}
	}
	fmt.Printf("方法1 (最小时钟值): safeTimestamp = %d\n", minClock)
	
	// 方法 2: 使用保守估计（当前时间 - 容差）
	maxNetworkDelay := int64(1000) // 假设最大网络延迟 1秒
	currentTime := clocks[0].Now()
	safeTimestamp2 := currentTime - maxNetworkDelay
	fmt.Printf("方法2 (保守估计): safeTimestamp = %d (当前=%d - 容差=%d)\n", 
		safeTimestamp2, currentTime, maxNetworkDelay)
	
	// 方法 3: 使用确认消息（如 Raft/Quorum）
	// 假设我们知道 3 个节点中至少 2 个确认了 T1
	confirmedTime := t1
	fmt.Printf("方法3 (确认消息): safeTimestamp = %d (多数节点确认的时间)\n", confirmedTime)
	
	// 使用方法 3 进行 GC
	fmt.Printf("\n使用 safeTimestamp=%d 进行 GC\n", confirmedTime)
	for i, n := range nodes {
		removed := n.GC(confirmedTime)
		fmt.Printf("节点 %d 清理: %d, Tombstones: %d\n", i, removed, len(n.Tombstones))
	}
	
	fmt.Println("\n⚠️ 注意：在实际系统中，safeTimestamp 的计算需要")
	fmt.Println("   - 定期同步所有节点的状态")
	fmt.Println("   - 使用一致性协议（如 Raft）")
	fmt.Println("   - 或者使用保守的时间估计")
}

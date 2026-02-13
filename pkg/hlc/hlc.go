package hlc

import (
	"sync"
	"time"
)

// Clock 代表混合逻辑时钟。
// 它保证单调递增，并跟踪因果关系。
// 时间戳被打包为 int64：
//   - 高 48 位：物理时间 (毫秒)，从 Unix Epoch 开始。
//   - 低 16 位：逻辑计数器。
type Clock struct {
	mu     sync.Mutex
	latest int64 // 当前已知的最大 HLC 时间戳 (packed)
}

const (
	logicalMask = 0xFFFF
	physicalAnd = ^int64(logicalMask)
)

// New 创建一个新的 HLC 时钟。
func New() *Clock {
	return &Clock{
		latest: 0,
	}
}

// Now 返回当前的 HLC 时间戳，并更新内部状态。
// 它确保返回的时间戳严格大于任何先前返回的时间戳或更新的时间戳。
func (c *Clock) Now() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	phys := time.Now().UnixMilli()

	// 从 current latest 中解包
	oldPhys := c.latest >> 16
	oldLogical := c.latest & logicalMask

	var newPhys int64
	var newLogical int64

	if phys > oldPhys {
		// 物理时间推进：重置逻辑计数
		newPhys = phys
		newLogical = 0
	} else {
		// 物理时间倒退或相等：增加逻辑计数
		newPhys = oldPhys
		newLogical = oldLogical + 1
	}

	// 重新打包
	// 确保逻辑计数不溢出 (16位 = 65535)
	// 在极端高并发下如果不处理溢出可能会有问题，但在 DB 这种粒度一般够用。
	// 这里我们允许溢出回绕？或者物理时间强制进位？
	// 简单起见，如果不幸溢出，我们强行让物理时间+1 (借位思路)
	if newLogical > 0xFFFF {
		newPhys++
		newLogical = 0
	}

	c.latest = (newPhys << 16) | newLogical
	return c.latest
}

// Update 根据接收到的远程时间戳更新本地时钟。
// 用于处理同步消息。
func (c *Clock) Update(remoteTs int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	phys := time.Now().UnixMilli()

	// 解包 remote
	remotePhys := remoteTs >> 16
	remoteLogical := remoteTs & logicalMask

	// 解包 local
	oldPhys := c.latest >> 16
	oldLogical := c.latest & logicalMask

	var newPhys int64
	var newLogical int64

	// HLC Update Logic (send/recv event)
	// newPhys = max(oldPhys, remotePhys, phys)
	if oldPhys > remotePhys {
		newPhys = oldPhys
	} else {
		newPhys = remotePhys
	}
	if phys > newPhys {
		newPhys = phys
	}

	// Calculate logical
	if newPhys == oldPhys && newPhys == remotePhys {
		// Both equal, verify logical
		if oldLogical > remoteLogical {
			newLogical = oldLogical + 1
		} else {
			newLogical = remoteLogical + 1
		}
	} else if newPhys == oldPhys {
		newLogical = oldLogical + 1
	} else if newPhys == remotePhys {
		newLogical = remoteLogical + 1
	} else {
		newLogical = 0
	}

	// Overflow check
	if newLogical > 0xFFFF {
		newPhys++
		newLogical = 0
	}

	c.latest = (newPhys << 16) | newLogical
}

// Physical 返回时间戳的物理部分 (Unix Milli)。
func Physical(ts int64) int64 {
	return ts >> 16
}

// Logical 返回时间戳的逻辑部分。
func Logical(ts int64) int16 {
	return int16(ts & logicalMask)
}

// Compare 比较两个 HLC 时间戳。
// 返回值:
//   - 如果 a > b: 返回 1
//   - 如果 a == b: 返回 0
//   - 如果 a < b: 返回 -1
func Compare(a, b int64) int {
	aPhys := a >> 16
	bPhys := b >> 16
	aLog := a & logicalMask
	bLog := b & logicalMask

	// 首先比较物理时间
	if aPhys > bPhys {
		return 1
	}
	if aPhys < bPhys {
		return -1
	}

	// 物理时间相等，比较逻辑时间
	if aLog > bLog {
		return 1
	}
	if aLog < bLog {
		return -1
	}

	return 0
}

// IsStale 判断时间戳是否过于陈旧（基于物理时间）。
// 如果 remote 的物理时间比本地落后超过 maxDiffMs 毫秒，返回 true。
func IsStale(remote, local int64, maxDiffMs int64) bool {
	remotePhys := remote >> 16
	localPhys := local >> 16
	return localPhys-remotePhys > maxDiffMs
}

package hlc

import (
	"sync"
	"time"
)

// Clock is a Hybrid Logical Clock (HLC) packed into one int64:
//   - high 48 bits: physical milliseconds since Unix epoch
//   - low 16 bits: logical counter
type Clock struct {
	mu     sync.Mutex
	latest int64
}

const (
	logicalBits = 16
	logicalMask = int64(0xFFFF)
	// maxPhysicalMillis is the largest millisecond value that still fits in a
	// signed int64 when packed as (physical<<16)|logical.
	maxPhysicalMillis = int64(^uint64(0)>>1) >> logicalBits
)

// New creates one new HLC clock.
func New() *Clock {
	return &Clock{}
}

// Now returns the next local timestamp and keeps monotonicity.
func (c *Clock) Now() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	phys := time.Now().UnixMilli()
	oldPhys := c.latest >> logicalBits
	oldLogical := c.latest & logicalMask

	newPhys := oldPhys
	newLogical := oldLogical + 1
	if phys > oldPhys {
		newPhys = phys
		newLogical = 0
	}

	if newLogical > logicalMask {
		newPhys++
		newLogical = 0
	}

	c.latest = Pack(newPhys, uint16(newLogical))
	return c.latest
}

// Update merges one remote timestamp into local clock state.
func (c *Clock) Update(remoteTs int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	phys := time.Now().UnixMilli()

	remotePhys := remoteTs >> logicalBits
	remoteLogical := remoteTs & logicalMask

	oldPhys := c.latest >> logicalBits
	oldLogical := c.latest & logicalMask

	newPhys := oldPhys
	if remotePhys > newPhys {
		newPhys = remotePhys
	}
	if phys > newPhys {
		newPhys = phys
	}

	var newLogical int64
	if newPhys == oldPhys && newPhys == remotePhys {
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

	if newLogical > logicalMask {
		newPhys++
		newLogical = 0
	}

	c.latest = Pack(newPhys, uint16(newLogical))
}

// Pack builds one packed HLC timestamp from physical millis + logical.
func Pack(physicalMillis int64, logical uint16) int64 {
	if physicalMillis < 0 {
		physicalMillis = 0
	} else if physicalMillis > maxPhysicalMillis {
		physicalMillis = maxPhysicalMillis
	}
	return (physicalMillis << logicalBits) | int64(logical)
}

// AddPhysical shifts only the physical milliseconds and preserves logical bits.
// It saturates on overflow.
func AddPhysical(ts int64, deltaMillis int64) int64 {
	phys := Physical(ts)
	logical := ts & logicalMask

	nextPhys := phys + deltaMillis
	if deltaMillis > 0 && nextPhys < phys {
		nextPhys = maxPhysicalMillis
	} else if deltaMillis < 0 && nextPhys > phys {
		nextPhys = 0
	}

	if nextPhys < 0 {
		nextPhys = 0
	} else if nextPhys > maxPhysicalMillis {
		nextPhys = maxPhysicalMillis
	}

	return (nextPhys << logicalBits) | logical
}

// SubPhysical subtracts milliseconds from only the physical component.
func SubPhysical(ts int64, deltaMillis int64) int64 {
	if deltaMillis == 0 {
		return ts
	}
	return AddPhysical(ts, -deltaMillis)
}

// PhysicalDiff returns physical milliseconds difference a-b.
func PhysicalDiff(a, b int64) int64 {
	return Physical(a) - Physical(b)
}

// Physical returns the physical milliseconds component.
func Physical(ts int64) int64 {
	return ts >> logicalBits
}

// Logical returns the logical component.
func Logical(ts int64) int16 {
	return int16(ts & logicalMask)
}

// Compare compares two packed HLC timestamps.
// Returns 1 if a>b, 0 if a==b, -1 if a<b.
func Compare(a, b int64) int {
	aPhys := a >> logicalBits
	bPhys := b >> logicalBits
	aLog := a & logicalMask
	bLog := b & logicalMask

	if aPhys > bPhys {
		return 1
	}
	if aPhys < bPhys {
		return -1
	}
	if aLog > bLog {
		return 1
	}
	if aLog < bLog {
		return -1
	}
	return 0
}

// IsStale checks whether remote physical time lags local beyond maxDiffMs.
func IsStale(remote, local int64, maxDiffMs int64) bool {
	remotePhys := remote >> logicalBits
	localPhys := local >> logicalBits
	return localPhys-remotePhys > maxDiffMs
}

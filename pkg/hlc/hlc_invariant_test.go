package hlc

import (
	"math/rand"
	"testing"
	"time"
)

func TestSubPhysicalPreservesLogicalAndSubtractsMillis(t *testing.T) {
	ts := Pack(1_700_000_000_000, 12_345)
	offsetMs := int64(30_000)

	got := SubPhysical(ts, offsetMs)
	if Physical(got) != Physical(ts)-offsetMs {
		t.Fatalf("unexpected physical value: want=%d got=%d", Physical(ts)-offsetMs, Physical(got))
	}
	if got&logicalMask != ts&logicalMask {
		t.Fatalf("logical bits changed: want=%d got=%d", ts&logicalMask, got&logicalMask)
	}
}

func TestSubPhysicalSaturatesAtZeroPhysical(t *testing.T) {
	ts := Pack(10, 7)
	got := SubPhysical(ts, 1_000)

	if Physical(got) != 0 {
		t.Fatalf("physical part should saturate to 0, got=%d", Physical(got))
	}
	if got&logicalMask != ts&logicalMask {
		t.Fatalf("logical bits changed: want=%d got=%d", ts&logicalMask, got&logicalMask)
	}
}

func TestClockUpdateMonotonicInvariantRandomized(t *testing.T) {
	clock := New()
	prev := clock.Now()
	rng := rand.New(rand.NewSource(20260308))

	for i := 0; i < 10_000; i++ {
		var remote int64
		switch rng.Intn(4) {
		case 0:
			remote = SubPhysical(prev, int64(rng.Intn(5_000)))
		case 1:
			remote = AddPhysical(prev, int64(rng.Intn(5_000)))
		case 2:
			remote = Pack(Physical(prev), uint16(rng.Intn(1<<16)))
		default:
			remotePhys := time.Now().UnixMilli() + int64(rng.Intn(10_000)-5_000)
			remote = Pack(remotePhys, uint16(rng.Intn(1<<16)))
		}

		clock.Update(remote)
		next := clock.Now()

		if next <= prev {
			t.Fatalf("clock lost monotonicity at iter=%d: prev=%d next=%d", i, prev, next)
		}
		if Compare(next, remote) < 0 {
			t.Fatalf("clock moved behind merged remote timestamp at iter=%d: remote=%d next=%d", i, remote, next)
		}
		prev = next
	}
}

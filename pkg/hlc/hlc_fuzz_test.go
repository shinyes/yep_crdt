package hlc

import "testing"

func FuzzAddSubPhysicalRoundTrip(f *testing.F) {
	f.Add(uint64(100), uint16(7), uint32(1))
	f.Add(uint64(10_000_000), uint16(65_535), uint32(30_000))
	f.Add(uint64(0), uint16(0), uint32(999))

	f.Fuzz(func(t *testing.T, physical uint64, logical uint16, delta uint32) {
		p := int64(physical % uint64(maxPhysicalMillis+1))
		ts := Pack(p, logical)
		d := int64(delta % 86_400_000) // 0..24h

		sub := SubPhysical(ts, d)
		if sub&logicalMask != ts&logicalMask {
			t.Fatalf("logical bits changed after SubPhysical: want=%d got=%d", ts&logicalMask, sub&logicalMask)
		}
		if p < d && Physical(sub) != 0 {
			t.Fatalf("expected saturation at zero, p=%d d=%d got=%d", p, d, Physical(sub))
		}

		back := AddPhysical(sub, d)
		if p >= d && back != ts {
			t.Fatalf("round-trip mismatch: p=%d logical=%d d=%d want=%d got=%d", p, logical, d, ts, back)
		}
	})
}

func FuzzCompareConsistentWithTupleOrder(f *testing.F) {
	f.Add(uint64(100), uint16(1), uint64(101), uint16(0))
	f.Add(uint64(100), uint16(7), uint64(100), uint16(8))
	f.Add(uint64(100), uint16(9), uint64(100), uint16(9))

	f.Fuzz(func(t *testing.T, p1 uint64, l1 uint16, p2 uint64, l2 uint16) {
		phys1 := int64(p1 % uint64(maxPhysicalMillis+1))
		phys2 := int64(p2 % uint64(maxPhysicalMillis+1))
		a := Pack(phys1, l1)
		b := Pack(phys2, l2)

		want := 0
		switch {
		case phys1 > phys2:
			want = 1
		case phys1 < phys2:
			want = -1
		case l1 > l2:
			want = 1
		case l1 < l2:
			want = -1
		}

		got := Compare(a, b)
		if got != want {
			t.Fatalf("unexpected compare result: a=(%d,%d) b=(%d,%d) want=%d got=%d", phys1, l1, phys2, l2, want, got)
		}
	})
}

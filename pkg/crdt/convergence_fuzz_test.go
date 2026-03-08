package crdt

import (
	"fmt"
	"testing"

	"github.com/shinyes/yep_crdt/pkg/hlc"
)

func FuzzRGA_MergeCodecConvergence(f *testing.F) {
	f.Add([]byte{0x00, 0x01, 0x23, 0x45, 0x67})
	f.Add([]byte{0xFF, 0x10, 0x20, 0x30, 0x40, 0x50})
	f.Add([]byte("rga-merge-codec"))

	f.Fuzz(func(t *testing.T, script []byte) {
		if len(script) == 0 {
			return
		}

		base := NewRGA[[]byte](hlc.New())
		replicas := []*RGA[[]byte]{
			cloneRGABytes(t, base, hlc.New()),
			cloneRGABytes(t, base, hlc.New()),
			cloneRGABytes(t, base, hlc.New()),
		}

		const maxOps = 128
		limit := len(script)
		if limit > maxOps {
			limit = maxOps
		}

		for i := 0; i < limit; i++ {
			b := script[i]
			target := int(b % byte(len(replicas)))
			current := replicas[target]

			switch b & 0x03 {
			case 0, 1:
				anchors := rgaAllVertexIDsSorted(current)
				anchorID := anchors[int(b)%len(anchors)]
				payload := []byte(fmt.Sprintf("i%03d-%02x", i, b))
				if err := current.Apply(OpRGAInsert[[]byte]{AnchorID: anchorID, Value: payload}); err != nil {
					t.Fatalf("fuzz insert failed: idx=%d b=%d err=%v", i, b, err)
				}
			case 2:
				live := rgaLiveVertexIDsSorted(current)
				if len(live) == 0 {
					continue
				}
				removeID := live[int(b)%len(live)]
				if err := current.Apply(OpRGARemove{ID: removeID}); err != nil {
					t.Fatalf("fuzz remove failed: idx=%d b=%d err=%v", i, b, err)
				}
			default:
				// Keep local-op phase focused on randomized user operations.
			}
		}

		// Script-driven merge schedule with duplicate merges.
		for i := 0; i < limit; i++ {
			b := script[i]
			from := int((b >> 2) % byte(len(replicas)))
			to := int((b >> 4) % byte(len(replicas)))
			if from == to {
				to = (to + 1) % len(replicas)
			}
			if err := replicas[to].Merge(replicas[from]); err != nil {
				t.Fatalf("fuzz scheduled merge failed: from=%d to=%d err=%v", from, to, err)
			}
			if (b & 0x80) != 0 {
				if err := replicas[to].Merge(replicas[from]); err != nil {
					t.Fatalf("fuzz scheduled duplicate merge failed: from=%d to=%d err=%v", from, to, err)
				}
			}
		}

		convergeRGAAllToAll(t, replicas, 3)
		expected := rgaValueStrings(replicas[0])
		for i := 1; i < len(replicas); i++ {
			got := rgaValueStrings(replicas[i])
			if !equalStringSlices(expected, got) {
				t.Fatalf("fuzz rga convergence failed: replica=%d want=%v got=%v", i, expected, got)
			}
		}
		for i := range replicas {
			rgaRoundTripEqualValues(t, replicas[i])
		}
	})
}

func FuzzORSet_MergeCodecGC(f *testing.F) {
	f.Add([]byte{0x00, 0x11, 0x22, 0x33, 0x44})
	f.Add([]byte("orset-merge-codec-gc"))
	f.Add([]byte{0xFF, 0xFE, 0xFD, 0xFC, 0xFB})

	f.Fuzz(func(t *testing.T, script []byte) {
		if len(script) == 0 {
			return
		}

		replicas := []*ORSet[string]{
			NewORSet[string](),
			NewORSet[string](),
			NewORSet[string](),
		}
		replicas[0].Clock = hlc.New()
		replicas[1].Clock = hlc.New()
		replicas[2].Clock = hlc.New()

		elements := []string{"a", "b", "c", "d", "e", "f", "g"}
		const maxOps = 160
		limit := len(script)
		if limit > maxOps {
			limit = maxOps
		}

		for i := 0; i < limit; i++ {
			b := script[i]
			target := int(b % byte(len(replicas)))
			element := elements[int(b)%len(elements)]
			switch (b >> 1) & 0x03 {
			case 0, 1:
				if err := replicas[target].Apply(OpORSetAdd[string]{Element: element}); err != nil {
					t.Fatalf("fuzz orset add failed: idx=%d b=%d err=%v", i, b, err)
				}
			case 2:
				if err := replicas[target].Apply(OpORSetRemove[string]{Element: element}); err != nil {
					t.Fatalf("fuzz orset remove failed: idx=%d b=%d err=%v", i, b, err)
				}
			default:
				from := int((b >> 3) % byte(len(replicas)))
				to := int((b >> 5) % byte(len(replicas)))
				if from == to {
					to = (to + 1) % len(replicas)
				}
				if err := replicas[to].Merge(replicas[from]); err != nil {
					t.Fatalf("fuzz orset merge failed: from=%d to=%d err=%v", from, to, err)
				}
			}
		}

		convergeORSetAllToAll(t, replicas, 3)
		baseline := orsetSortedValues(replicas[0])
		for i := 1; i < len(replicas); i++ {
			got := orsetSortedValues(replicas[i])
			if !equalStringSlices(baseline, got) {
				t.Fatalf("fuzz orset convergence failed: replica=%d want=%v got=%v", i, baseline, got)
			}
		}

		safeTimestamp := maxORSetTombstoneTS(replicas) + 1
		for i := range replicas {
			_ = replicas[i].GC(safeTimestamp)

			raw, err := replicas[i].Bytes()
			if err != nil {
				t.Fatalf("fuzz orset encode failed: replica=%d err=%v", i, err)
			}
			decoded, err := FromBytesORSet[string](raw)
			if err != nil {
				t.Fatalf("fuzz orset decode failed: replica=%d err=%v", i, err)
			}
			got := orsetSortedValues(decoded)
			want := orsetSortedValues(replicas[i])
			if !equalStringSlices(want, got) {
				t.Fatalf("fuzz orset codec changed value: replica=%d want=%v got=%v", i, want, got)
			}
		}
	})
}

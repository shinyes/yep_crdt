package crdt

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/shinyes/yep_crdt/pkg/hlc"
)

func TestRGA_MergeConvergenceAndIdempotence_Randomized(t *testing.T) {
	seeds := []int64{1, 7, 17, 23, 29, 41, 55, 72, 89, 1447}

	for _, seed := range seeds {
		seed := seed
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))

			base := NewRGA[[]byte](hlc.New())
			replicas := []*RGA[[]byte]{
				cloneRGABytes(t, base, hlc.New()),
				cloneRGABytes(t, base, hlc.New()),
				cloneRGABytes(t, base, hlc.New()),
			}

			const operations = 160
			for step := 0; step < operations; step++ {
				target := rng.Intn(len(replicas))
				current := replicas[target]
				liveIDs := rgaLiveVertexIDsSorted(current)

				doInsert := len(liveIDs) == 0 || rng.Intn(100) < 70
				if doInsert {
					anchorIDs := rgaAllVertexIDsSorted(current)
					anchorID := anchorIDs[rng.Intn(len(anchorIDs))]
					payload := []byte(fmt.Sprintf("s%03d-r%d-%02x", step, target, rng.Intn(256)))
					if err := current.Apply(OpRGAInsert[[]byte]{AnchorID: anchorID, Value: payload}); err != nil {
						t.Fatalf("apply insert failed: seed=%d step=%d err=%v", seed, step, err)
					}
				} else {
					removeID := liveIDs[rng.Intn(len(liveIDs))]
					if err := current.Apply(OpRGARemove{ID: removeID}); err != nil {
						t.Fatalf("apply remove failed: seed=%d step=%d err=%v", seed, step, err)
					}
				}

			}

			// Shuffle merge order and inject duplicate merges to test idempotence.
			type mergePair struct {
				from int
				to   int
			}
			pairs := []mergePair{
				{from: 0, to: 1}, {from: 0, to: 2},
				{from: 1, to: 0}, {from: 1, to: 2},
				{from: 2, to: 0}, {from: 2, to: 1},
			}
			for round := 0; round < 4; round++ {
				rng.Shuffle(len(pairs), func(i, j int) {
					pairs[i], pairs[j] = pairs[j], pairs[i]
				})
				for _, p := range pairs {
					if err := replicas[p.to].Merge(replicas[p.from]); err != nil {
						t.Fatalf("scheduled merge failed: seed=%d round=%d from=%d to=%d err=%v", seed, round, p.from, p.to, err)
					}
					if rng.Intn(100) < 50 {
						if err := replicas[p.to].Merge(replicas[p.from]); err != nil {
							t.Fatalf("scheduled duplicate merge failed: seed=%d round=%d from=%d to=%d err=%v", seed, round, p.from, p.to, err)
						}
					}
				}
			}

			convergeRGAAllToAll(t, replicas, 3)

			expectedValues := rgaValueStrings(replicas[0])
			for i := 1; i < len(replicas); i++ {
				got := rgaValueStrings(replicas[i])
				if !equalStringSlices(expectedValues, got) {
					t.Fatalf("rga divergence after convergence: seed=%d replica=%d want=%v got=%v", seed, i, expectedValues, got)
				}
			}

			before := rgaStateFingerprint(replicas[0])
			if err := replicas[0].Merge(replicas[1]); err != nil {
				t.Fatalf("post-convergence merge failed: %v", err)
			}
			afterFirst := rgaStateFingerprint(replicas[0])
			if err := replicas[0].Merge(replicas[1]); err != nil {
				t.Fatalf("post-convergence duplicate merge failed: %v", err)
			}
			afterSecond := rgaStateFingerprint(replicas[0])
			if afterFirst != afterSecond {
				t.Fatalf("rga merge not idempotent: seed=%d before=%q after1=%q after2=%q", seed, before, afterFirst, afterSecond)
			}

			for i := range replicas {
				rgaRoundTripEqualValues(t, replicas[i])
			}
		})
	}
}

func TestORSet_GCSafeTimestamp_ConvergenceAndIdempotence_Randomized(t *testing.T) {
	seeds := []int64{3, 8, 21, 34, 55, 89, 144}
	elements := []string{"a", "b", "c", "d", "e", "f"}

	for _, seed := range seeds {
		seed := seed
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))

			replicas := []*ORSet[string]{
				NewORSet[string](),
				NewORSet[string](),
				NewORSet[string](),
			}
			replicas[0].Clock = hlc.New()
			replicas[1].Clock = hlc.New()
			replicas[2].Clock = hlc.New()

			const operations = 220
			for step := 0; step < operations; step++ {
				target := rng.Intn(len(replicas))
				element := elements[rng.Intn(len(elements))]
				if rng.Intn(100) < 65 {
					if err := replicas[target].Apply(OpORSetAdd[string]{Element: element}); err != nil {
						t.Fatalf("orset add failed: seed=%d step=%d err=%v", seed, step, err)
					}
				} else {
					if err := replicas[target].Apply(OpORSetRemove[string]{Element: element}); err != nil {
						t.Fatalf("orset remove failed: seed=%d step=%d err=%v", seed, step, err)
					}
				}

				if rng.Intn(100) < 40 {
					from := rng.Intn(len(replicas))
					to := rng.Intn(len(replicas) - 1)
					if to >= from {
						to++
					}
					if err := replicas[to].Merge(replicas[from]); err != nil {
						t.Fatalf("intermediate orset merge failed: seed=%d step=%d from=%d to=%d err=%v", seed, step, from, to, err)
					}
				}
			}

			convergeORSetAllToAll(t, replicas, 3)

			baseline := orsetSortedValues(replicas[0])
			for i := 1; i < len(replicas); i++ {
				got := orsetSortedValues(replicas[i])
				if !equalStringSlices(baseline, got) {
					t.Fatalf("orset divergence after convergence: seed=%d replica=%d want=%v got=%v", seed, i, baseline, got)
				}
			}

			safeTimestamp := maxORSetTombstoneTS(replicas) + 1
			for i := range replicas {
				_ = replicas[i].GC(safeTimestamp)
				removedSecond := replicas[i].GC(safeTimestamp)
				if removedSecond != 0 {
					t.Fatalf("orset gc not idempotent on second run: seed=%d replica=%d removed=%d", seed, i, removedSecond)
				}
			}

			convergeORSetAllToAll(t, replicas, 3)
			for i := 0; i < len(replicas); i++ {
				got := orsetSortedValues(replicas[i])
				if !equalStringSlices(baseline, got) {
					t.Fatalf("orset value changed after safe gc+merge: seed=%d replica=%d want=%v got=%v", seed, i, baseline, got)
				}
			}

			before := orsetStateFingerprint(replicas[0])
			if err := replicas[0].Merge(replicas[1]); err != nil {
				t.Fatalf("post-gc merge failed: %v", err)
			}
			afterFirst := orsetStateFingerprint(replicas[0])
			if err := replicas[0].Merge(replicas[1]); err != nil {
				t.Fatalf("post-gc duplicate merge failed: %v", err)
			}
			afterSecond := orsetStateFingerprint(replicas[0])
			if afterFirst != afterSecond {
				t.Fatalf("orset merge not idempotent: seed=%d before=%q after1=%q after2=%q", seed, before, afterFirst, afterSecond)
			}
		})
	}
}

func TestORSet_GCUnsafeTimestamp_CanResurrectRemovedElement(t *testing.T) {
	clockA := hlc.New()
	clockB := hlc.New()

	nodeA := NewORSet[string]()
	nodeB := NewORSet[string]()
	nodeA.Clock = clockA
	nodeB.Clock = clockB

	if err := nodeA.Apply(OpORSetAdd[string]{Element: "x"}); err != nil {
		t.Fatalf("add failed: %v", err)
	}
	if err := nodeB.Merge(nodeA); err != nil {
		t.Fatalf("initial merge failed: %v", err)
	}

	if err := nodeA.Apply(OpORSetRemove[string]{Element: "x"}); err != nil {
		t.Fatalf("remove failed: %v", err)
	}

	unsafeSafeTimestamp := maxORSetTombstoneTS([]*ORSet[string]{nodeA}) + 1
	removed := nodeA.GC(unsafeSafeTimestamp)
	if removed == 0 {
		t.Fatalf("expected unsafe gc to remove tombstone on nodeA")
	}

	// NodeB still has stale AddSet without corresponding tombstone.
	if err := nodeA.Merge(nodeB); err != nil {
		t.Fatalf("merge A<-B failed: %v", err)
	}
	if err := nodeB.Merge(nodeA); err != nil {
		t.Fatalf("merge B<-A failed: %v", err)
	}

	if !nodeA.Contains("x") || !nodeB.Contains("x") {
		t.Fatalf("expected element resurrection under unsafe gc, got A=%v B=%v",
			orsetSortedValues(nodeA), orsetSortedValues(nodeB))
	}
}

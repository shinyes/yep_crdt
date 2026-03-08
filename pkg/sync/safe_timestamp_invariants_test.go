package sync

import (
	"testing"
	"time"

	"github.com/shinyes/yep_crdt/pkg/hlc"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func TestNodeManager_CalculateSafeTimestamp_UsesPhysicalOffset(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "test-node")
	nm := NewNodeManager(database, "node-1")

	seedPhysical := time.Now().Add(2 * time.Minute).UnixMilli()
	database.Clock().Update(hlc.Pack(seedPhysical, 42))

	got := nm.CalculateSafeTimestamp()
	wantPhysical := seedPhysical - defaultSafeTimestampOffset.Milliseconds()
	if hlc.Physical(got) != wantPhysical {
		t.Fatalf("unexpected safe timestamp physical part: want=%d got=%d", wantPhysical, hlc.Physical(got))
	}
}

func TestNodeManager_CalculateSafeTimestamp_UsesMinimumPeerClock(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "test-node")
	nm := NewNodeManager(database, "node-1")

	seedPhysical := time.Now().Add(2 * time.Minute).UnixMilli()
	database.Clock().Update(hlc.Pack(seedPhysical, 1))

	peerFast := hlc.Pack(seedPhysical-10_000, 7)
	peerSlow := hlc.Pack(seedPhysical-20_000, 9)
	nm.OnHeartbeat("node-2", peerFast, 0)
	nm.OnHeartbeat("node-3", peerSlow, 0)

	got := nm.CalculateSafeTimestamp()
	want := hlc.SubPhysical(peerSlow, defaultSafeTimestampOffset.Milliseconds())
	if got != want {
		t.Fatalf("unexpected safe timestamp: want=%d got=%d", want, got)
	}
}

func TestClockSync_HandleNodeRejoin_UsesPhysicalClockDiff(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer s.Close()

	database := mustOpenDB(t, s, "test-node")
	if err := database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
		},
	}); err != nil {
		t.Fatalf("define table failed: %v", err)
	}

	nm := NewNodeManager(database, "node-1",
		WithTimeoutThreshold(0),
		WithClockThreshold(1),
	)
	net := &rejoinFullSyncNetwork{}
	nm.RegisterNetwork(net)

	phys := time.Now().Add(2 * time.Minute).UnixMilli()
	localClock := hlc.Pack(phys, 60_000)
	remoteClock := hlc.Pack(phys, 1)
	database.Clock().Update(localClock)

	nm.clockSync.HandleNodeRejoin("node-2", remoteClock, 0)

	if got := net.FetchCalls(); got != 0 {
		t.Fatalf("expected no full sync when physical clock diff is 0, got fetch calls=%d", got)
	}
}

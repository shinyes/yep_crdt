package crdt

import (
	"bytes"
	"encoding/base64"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/shinyes/yep_crdt/pkg/hlc"
)

func cloneRGABytes(t *testing.T, src *RGA[[]byte], clock *hlc.Clock) *RGA[[]byte] {
	t.Helper()
	raw, err := src.Bytes()
	if err != nil {
		t.Fatalf("encode RGA failed: %v", err)
	}
	dst, err := FromBytesRGA[[]byte](raw)
	if err != nil {
		t.Fatalf("decode RGA failed: %v", err)
	}
	dst.Clock = clock
	return dst
}

func cloneORSetString(t *testing.T, src *ORSet[string], clock *hlc.Clock) *ORSet[string] {
	t.Helper()
	raw, err := src.Bytes()
	if err != nil {
		t.Fatalf("encode ORSet failed: %v", err)
	}
	dst, err := FromBytesORSet[string](raw)
	if err != nil {
		t.Fatalf("decode ORSet failed: %v", err)
	}
	dst.Clock = clock
	return dst
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func rgaAllVertexIDsSorted(r *RGA[[]byte]) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.Vertices))
	for id := range r.Vertices {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func rgaLiveVertexIDsSorted(r *RGA[[]byte]) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.Vertices))
	for id, v := range r.Vertices {
		if id == r.Head || v.Deleted {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func rgaValueStrings(r *RGA[[]byte]) []string {
	values := r.Value().([][]byte)
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, string(v))
	}
	return out
}

func rgaStateFingerprint(r *RGA[[]byte]) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.Vertices))
	for id := range r.Vertices {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var b strings.Builder
	b.WriteString("head=")
	b.WriteString(r.Head)
	b.WriteString(";")
	for _, id := range ids {
		v := r.Vertices[id]
		if v == nil {
			continue
		}
		b.WriteString(id)
		b.WriteString("|")
		b.WriteString(base64.StdEncoding.EncodeToString(v.Value))
		b.WriteString("|")
		b.WriteString(v.Origin)
		b.WriteString("|")
		b.WriteString(strconv.FormatInt(v.Timestamp, 10))
		b.WriteString("|")
		b.WriteString(strconv.FormatBool(v.Deleted))
		b.WriteString("|")
		b.WriteString(strconv.FormatInt(v.DeletedAt, 10))
		b.WriteString("|")
		b.WriteString(v.Next)
		b.WriteString(";")
	}
	return b.String()
}

func convergeRGAAllToAll(t *testing.T, replicas []*RGA[[]byte], rounds int) {
	t.Helper()
	for n := 0; n < rounds; n++ {
		for i := range replicas {
			for j := range replicas {
				if i == j {
					continue
				}
				if err := replicas[i].Merge(replicas[j]); err != nil {
					t.Fatalf("rga merge failed: i=%d j=%d err=%v", i, j, err)
				}
			}
		}
	}
}

func orsetSortedValues(s *ORSet[string]) []string {
	values := s.Value().([]string)
	sort.Strings(values)
	return values
}

func orsetStateFingerprint(s *ORSet[string]) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	elements := make([]string, 0, len(s.AddSet))
	for e := range s.AddSet {
		elements = append(elements, e)
	}
	sort.Strings(elements)

	var b strings.Builder
	for _, e := range elements {
		b.WriteString("e=")
		b.WriteString(e)
		b.WriteString(":")
		ids := make([]string, 0, len(s.AddSet[e]))
		for id := range s.AddSet[e] {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			b.WriteString(id)
			b.WriteString(",")
		}
		b.WriteString(";")
	}

	tombstoneIDs := make([]string, 0, len(s.Tombstones))
	for id := range s.Tombstones {
		tombstoneIDs = append(tombstoneIDs, id)
	}
	sort.Strings(tombstoneIDs)
	for _, id := range tombstoneIDs {
		b.WriteString("t=")
		b.WriteString(id)
		b.WriteString(":")
		b.WriteString(strconv.FormatInt(s.Tombstones[id], 10))
		b.WriteString(";")
	}
	return b.String()
}

func convergeORSetAllToAll(t *testing.T, replicas []*ORSet[string], rounds int) {
	t.Helper()
	for n := 0; n < rounds; n++ {
		for i := range replicas {
			for j := range replicas {
				if i == j {
					continue
				}
				if err := replicas[i].Merge(replicas[j]); err != nil {
					t.Fatalf("orset merge failed: i=%d j=%d err=%v", i, j, err)
				}
			}
		}
	}
}

func maxORSetTombstoneTS(replicas []*ORSet[string]) int64 {
	maxTS := int64(0)
	for _, s := range replicas {
		s.mu.RLock()
		for _, ts := range s.Tombstones {
			if ts > maxTS {
				maxTS = ts
			}
		}
		s.mu.RUnlock()
	}
	return maxTS
}

func rgaRoundTripEqualValues(t *testing.T, src *RGA[[]byte]) {
	t.Helper()
	raw, err := src.Bytes()
	if err != nil {
		t.Fatalf("encode rga failed: %v", err)
	}
	decoded, err := FromBytesRGA[[]byte](raw)
	if err != nil {
		t.Fatalf("decode rga failed: %v", err)
	}
	got := decoded.Value().([][]byte)
	want := src.Value().([][]byte)
	if len(got) != len(want) {
		t.Fatalf("rga codec changed value length: want=%d got=%d", len(want), len(got))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("rga codec changed value at index=%d: want=%q got=%q", i, want[i], got[i])
		}
	}
}

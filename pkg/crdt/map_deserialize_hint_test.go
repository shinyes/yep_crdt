package crdt

import (
	"fmt"
	"strings"
	"testing"

	"github.com/shinyes/yep_crdt/pkg/hlc"
)

func TestDeserializeWithHint_ORSetUnknownHintFails(t *testing.T) {
	set := NewORSet[string]()
	set.Apply(OpORSetAdd[string]{Element: "tag-a"})
	data, err := set.Bytes()
	if err != nil {
		t.Fatalf("ORSet bytes failed: %v", err)
	}

	_, err = DeserializeWithHint(TypeORSet, data, "int")
	if err == nil {
		t.Fatalf("expected unknown ORSet type hint to fail")
	}
	if !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("unexpected ORSet error: %v", err)
	}
}

func TestDeserializeWithHint_ORSetSerializerErrorDoesNotFallback(t *testing.T) {
	const hint = "broken_orset_hint"
	originalSerializer, hadSerializer := TypeRegistry.ORSetSerializers[hint]
	originalTypeHint, hadTypeHint := TypeRegistry.ORSetTypeHints[hint]
	defer func() {
		if hadSerializer {
			TypeRegistry.ORSetSerializers[hint] = originalSerializer
		} else {
			delete(TypeRegistry.ORSetSerializers, hint)
		}
		if hadTypeHint {
			TypeRegistry.ORSetTypeHints[hint] = originalTypeHint
		} else {
			delete(TypeRegistry.ORSetTypeHints, hint)
		}
	}()

	RegisterORSet[string](hint, func([]byte) (*ORSet[string], error) {
		return nil, fmt.Errorf("broken serializer")
	})

	set := NewORSet[string]()
	set.Apply(OpORSetAdd[string]{Element: "tag-a"})
	data, err := set.Bytes()
	if err != nil {
		t.Fatalf("ORSet bytes failed: %v", err)
	}

	_, err = DeserializeWithHint(TypeORSet, data, hint)
	if err == nil {
		t.Fatalf("expected serializer failure")
	}
	if !strings.Contains(err.Error(), "broken serializer") {
		t.Fatalf("unexpected serializer error: %v", err)
	}
}

func TestDeserializeWithHint_RGAUnknownHintFails(t *testing.T) {
	rga := NewRGA[[]byte](hlc.New())
	rga.Apply(OpRGAInsert[[]byte]{AnchorID: rga.Head, Value: []byte("A")})
	data, err := rga.Bytes()
	if err != nil {
		t.Fatalf("RGA bytes failed: %v", err)
	}

	_, err = DeserializeWithHint(TypeRGA, data, "string")
	if err == nil {
		t.Fatalf("expected unknown RGA type hint to fail")
	}
	if !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("unexpected RGA error: %v", err)
	}
}

func TestDeserializeWithHint_DefaultHintsStillWork(t *testing.T) {
	set := NewORSet[string]()
	set.Apply(OpORSetAdd[string]{Element: "tag-a"})
	setBytes, err := set.Bytes()
	if err != nil {
		t.Fatalf("ORSet bytes failed: %v", err)
	}
	if _, err := DeserializeWithHint(TypeORSet, setBytes, ""); err != nil {
		t.Fatalf("default ORSet deserialization should work: %v", err)
	}

	rga := NewRGA[[]byte](hlc.New())
	rga.Apply(OpRGAInsert[[]byte]{AnchorID: rga.Head, Value: []byte("A")})
	rgaBytes, err := rga.Bytes()
	if err != nil {
		t.Fatalf("RGA bytes failed: %v", err)
	}
	if _, err := DeserializeWithHint(TypeRGA, rgaBytes, ""); err != nil {
		t.Fatalf("default RGA deserialization should work: %v", err)
	}
}

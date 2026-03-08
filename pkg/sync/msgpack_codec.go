package sync

import (
	"github.com/vmihailenco/msgpack/v5"
)

func marshalSyncWire(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func unmarshalSyncWire(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}

func decodeNetworkMessage(data []byte, msg *NetworkMessage) error {
	*msg = NetworkMessage{}
	return unmarshalSyncWire(data, msg)
}

func decodeFetchRawResponseLite(data []byte, lite *fetchRawResponseLite) error {
	*lite = fetchRawResponseLite{}
	return unmarshalSyncWire(data, lite)
}

func marshalVersionDigest(digest *VersionDigest) ([]byte, error) {
	return marshalSyncWire(digest)
}

func unmarshalVersionDigest(data []byte, digest *VersionDigest) error {
	*digest = VersionDigest{}
	return unmarshalSyncWire(data, digest)
}

package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/store"
)

const (
	merkleMaxLevel     = 4
	merkleNodeHashSize = 32
	merkleBuiltVersion = "v1"
)

// MerkleMaxLevel returns the fixed leaf depth used by table Merkle digest.
func MerkleMaxLevel() int {
	return merkleMaxLevel
}

func merkleBuiltKey(tableName string) []byte {
	return []byte("/m/meta/" + tableName + "/built")
}

func merkleRowPrefix(tableName string) []byte {
	return []byte("/m/r/" + tableName + "/")
}

func merkleRowKey(tableName string, key uuid.UUID) []byte {
	p := merkleRowPrefix(tableName)
	k := make([]byte, 0, len(p)+16)
	k = append(k, p...)
	k = append(k, key[:]...)
	return k
}

func merkleNodeLevelPrefix(tableName string, level int) []byte {
	return []byte("/m/n/" + tableName + "/" + fmt.Sprintf("%02d", level) + "/")
}

func merkleNodeKey(tableName string, level int, nibblePrefix string) []byte {
	p := merkleNodeLevelPrefix(tableName, level)
	k := make([]byte, 0, len(p)+len(nibblePrefix))
	k = append(k, p...)
	k = append(k, nibblePrefix...)
	return k
}

func normalizeNibblePrefix(prefix string) string {
	return strings.ToLower(strings.TrimSpace(prefix))
}

func nibblePrefixFromUUID(u uuid.UUID, level int) string {
	if level <= 0 {
		return ""
	}
	hexKey := hex.EncodeToString(u[:])
	if level > len(hexKey) {
		level = len(hexKey)
	}
	return hexKey[:level]
}

func nibblePrefixFromUUIDBytes(uuidBytes []byte, level int) string {
	if len(uuidBytes) < 16 || level <= 0 {
		return ""
	}
	hexKey := hex.EncodeToString(uuidBytes[:16])
	if level > len(hexKey) {
		level = len(hexKey)
	}
	return hexKey[:level]
}

func nibblePrefixToBytes(prefix string) ([]byte, error) {
	if len(prefix)%2 != 0 {
		return nil, fmt.Errorf("odd nibble prefix length: %d", len(prefix))
	}
	if prefix == "" {
		return nil, nil
	}
	b, err := hex.DecodeString(prefix)
	if err != nil {
		return nil, fmt.Errorf("decode nibble prefix failed: %w", err)
	}
	return b, nil
}

func hashRowSHA256Bytes(data []byte) []byte {
	sum := sha256.Sum256(data)
	out := make([]byte, len(sum))
	copy(out, sum[:])
	return out
}

func decodeMerkleHashHex(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	return hex.EncodeToString(raw)
}

func validNibblePrefix(prefix string, level int) bool {
	if len(prefix) != level {
		return false
	}
	for _, ch := range prefix {
		if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') {
			continue
		}
		return false
	}
	return true
}

func (t *Table) ensureMerkleIndex(txn store.Tx) error {
	builtVal, err := txn.Get(merkleBuiltKey(t.schema.Name))
	if err == nil && string(builtVal) == merkleBuiltVersion {
		return nil
	}
	if err != nil && err != store.ErrKeyNotFound {
		return fmt.Errorf("read merkle built marker failed: %w", err)
	}
	if err := t.rebuildMerkleIndex(txn); err != nil {
		return err
	}
	if err := txn.Set(merkleBuiltKey(t.schema.Name), []byte(merkleBuiltVersion), 0); err != nil {
		return fmt.Errorf("write merkle built marker failed: %w", err)
	}
	return nil
}

func (t *Table) rebuildMerkleIndex(txn store.Tx) error {
	if err := clearPrefix(txn, merkleRowPrefix(t.schema.Name)); err != nil {
		return fmt.Errorf("clear merkle row hash prefix failed: %w", err)
	}
	for level := 0; level <= merkleMaxLevel; level++ {
		if err := clearPrefix(txn, merkleNodeLevelPrefix(t.schema.Name, level)); err != nil {
			return fmt.Errorf("clear merkle node hash prefix failed: level=%d, err=%w", level, err)
		}
	}

	dataPrefix := t.tablePrefix()
	it := txn.NewIterator(store.IteratorOptions{Prefix: dataPrefix})
	defer it.Close()

	it.Seek(dataPrefix)
	for it.ValidForPrefix(dataPrefix) {
		keyRaw, valRaw, err := it.Item()
		if err != nil {
			return fmt.Errorf("scan table rows for merkle rebuild failed: %w", err)
		}
		uuidBytes := keyRaw[len(dataPrefix):]
		if len(uuidBytes) != 16 {
			return fmt.Errorf("scan table rows for merkle rebuild found invalid key length: %d", len(uuidBytes))
		}

		var rowKey uuid.UUID
		copy(rowKey[:], uuidBytes)
		rowHash := hashRowSHA256Bytes(valRaw)
		if err := txn.Set(merkleRowKey(t.schema.Name, rowKey), rowHash, 0); err != nil {
			return fmt.Errorf("write merkle row hash failed: %w", err)
		}
		it.Next()
	}

	if err := t.rebuildMerkleLeavesFromRows(txn); err != nil {
		return err
	}
	if err := t.rebuildMerkleInternalNodes(txn); err != nil {
		return err
	}
	return nil
}

func (t *Table) rebuildMerkleLeavesFromRows(txn store.Tx) error {
	rowPrefix := merkleRowPrefix(t.schema.Name)
	it := txn.NewIterator(store.IteratorOptions{Prefix: rowPrefix})
	defer it.Close()

	var currentLeaf string
	var hasCurrentLeaf bool
	leafHasher := sha256.New()

	flushCurrent := func() error {
		if !hasCurrentLeaf {
			return nil
		}
		sum := leafHasher.Sum(nil)
		if len(sum) != merkleNodeHashSize {
			return fmt.Errorf("invalid leaf hash size: %d", len(sum))
		}
		if err := txn.Set(merkleNodeKey(t.schema.Name, merkleMaxLevel, currentLeaf), sum, 0); err != nil {
			return fmt.Errorf("write leaf node hash failed: leaf=%s, err=%w", currentLeaf, err)
		}
		return nil
	}

	it.Seek(rowPrefix)
	for it.ValidForPrefix(rowPrefix) {
		keyRaw, valRaw, err := it.Item()
		if err != nil {
			return fmt.Errorf("scan row hashes for leaf rebuild failed: %w", err)
		}
		uuidBytes := keyRaw[len(rowPrefix):]
		if len(uuidBytes) != 16 {
			return fmt.Errorf("scan row hashes for leaf rebuild found invalid key length: %d", len(uuidBytes))
		}
		if len(valRaw) != merkleNodeHashSize {
			return fmt.Errorf("scan row hashes for leaf rebuild found invalid hash length: %d", len(valRaw))
		}

		leafPrefix := nibblePrefixFromUUIDBytes(uuidBytes, merkleMaxLevel)
		if !hasCurrentLeaf || leafPrefix != currentLeaf {
			if err := flushCurrent(); err != nil {
				return err
			}
			leafHasher.Reset()
			currentLeaf = leafPrefix
			hasCurrentLeaf = true
		}

		leafHasher.Write([]byte{0x00})
		leafHasher.Write(uuidBytes)
		leafHasher.Write(valRaw)
		it.Next()
	}

	return flushCurrent()
}

func (t *Table) rebuildMerkleInternalNodes(txn store.Tx) error {
	for level := merkleMaxLevel - 1; level >= 0; level-- {
		childLevel := level + 1
		childPrefix := merkleNodeLevelPrefix(t.schema.Name, childLevel)
		it := txn.NewIterator(store.IteratorOptions{Prefix: childPrefix})

		var currentParent string
		var hasCurrentParent bool
		parentHasher := sha256.New()

		flushCurrent := func() error {
			if !hasCurrentParent {
				return nil
			}
			sum := parentHasher.Sum(nil)
			if len(sum) != merkleNodeHashSize {
				return fmt.Errorf("invalid internal hash size: %d", len(sum))
			}
			if err := txn.Set(merkleNodeKey(t.schema.Name, level, currentParent), sum, 0); err != nil {
				return fmt.Errorf("write internal node hash failed: level=%d, prefix=%s, err=%w", level, currentParent, err)
			}
			return nil
		}

		it.Seek(childPrefix)
		for it.ValidForPrefix(childPrefix) {
			keyRaw, valRaw, err := it.Item()
			if err != nil {
				it.Close()
				return fmt.Errorf("scan child nodes failed: level=%d, err=%w", childLevel, err)
			}
			if len(valRaw) != merkleNodeHashSize {
				it.Close()
				return fmt.Errorf("scan child nodes found invalid hash length: level=%d, len=%d", childLevel, len(valRaw))
			}

			fullChildPrefix := string(keyRaw[len(childPrefix):])
			if len(fullChildPrefix) != childLevel {
				it.Close()
				return fmt.Errorf("scan child nodes found invalid child prefix length: level=%d, child=%q", childLevel, fullChildPrefix)
			}
			parentPrefix := fullChildPrefix[:level]
			childNibble := fullChildPrefix[level]

			if !hasCurrentParent || parentPrefix != currentParent {
				if err := flushCurrent(); err != nil {
					it.Close()
					return err
				}
				parentHasher.Reset()
				currentParent = parentPrefix
				hasCurrentParent = true
			}

			parentHasher.Write([]byte{0x01, childNibble})
			parentHasher.Write(valRaw)
			it.Next()
		}
		it.Close()

		if err := flushCurrent(); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) updateMerkleAfterRowSave(txn store.Tx, rowKey uuid.UUID, rowBytes []byte) error {
	builtVal, err := txn.Get(merkleBuiltKey(t.schema.Name))
	if err == store.ErrKeyNotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read merkle built marker failed: %w", err)
	}
	if string(builtVal) != merkleBuiltVersion {
		return nil
	}

	rowHashKey := merkleRowKey(t.schema.Name, rowKey)
	newHash := hashRowSHA256Bytes(rowBytes)

	oldHash, err := txn.Get(rowHashKey)
	if err != nil && err != store.ErrKeyNotFound {
		return fmt.Errorf("read old row hash failed: %w", err)
	}
	if err == nil && bytes.Equal(oldHash, newHash) {
		return nil
	}

	if err := txn.Set(rowHashKey, newHash, 0); err != nil {
		return fmt.Errorf("write new row hash failed: %w", err)
	}

	leafPrefix := nibblePrefixFromUUID(rowKey, merkleMaxLevel)
	if err := t.recomputeMerkleLeaf(txn, leafPrefix); err != nil {
		return err
	}

	for level := merkleMaxLevel - 1; level >= 0; level-- {
		parentPrefix := leafPrefix[:level]
		if err := t.recomputeMerkleInternal(txn, level, parentPrefix); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) recomputeMerkleLeaf(txn store.Tx, leafPrefix string) error {
	rowPrefix := merkleRowPrefix(t.schema.Name)
	prefixBytes, err := nibblePrefixToBytes(leafPrefix)
	if err != nil {
		return err
	}
	targetPrefix := make([]byte, 0, len(rowPrefix)+len(prefixBytes))
	targetPrefix = append(targetPrefix, rowPrefix...)
	targetPrefix = append(targetPrefix, prefixBytes...)

	it := txn.NewIterator(store.IteratorOptions{Prefix: targetPrefix})
	defer it.Close()

	hasher := sha256.New()
	rowCount := 0
	it.Seek(targetPrefix)
	for it.ValidForPrefix(targetPrefix) {
		keyRaw, valRaw, err := it.Item()
		if err != nil {
			return fmt.Errorf("scan leaf rows failed: leaf=%s, err=%w", leafPrefix, err)
		}
		uuidBytes := keyRaw[len(rowPrefix):]
		if len(uuidBytes) != 16 {
			return fmt.Errorf("scan leaf rows found invalid key length: %d", len(uuidBytes))
		}
		if len(valRaw) != merkleNodeHashSize {
			return fmt.Errorf("scan leaf rows found invalid hash length: %d", len(valRaw))
		}

		hasher.Write([]byte{0x00})
		hasher.Write(uuidBytes)
		hasher.Write(valRaw)
		rowCount++
		it.Next()
	}

	nodeKey := merkleNodeKey(t.schema.Name, merkleMaxLevel, leafPrefix)
	if rowCount == 0 {
		if err := txn.Delete(nodeKey); err != nil {
			return fmt.Errorf("delete empty leaf node failed: leaf=%s, err=%w", leafPrefix, err)
		}
		return nil
	}

	if err := txn.Set(nodeKey, hasher.Sum(nil), 0); err != nil {
		return fmt.Errorf("write leaf node failed: leaf=%s, err=%w", leafPrefix, err)
	}
	return nil
}

func (t *Table) recomputeMerkleInternal(txn store.Tx, level int, prefix string) error {
	childLevel := level + 1
	childLevelPrefix := merkleNodeLevelPrefix(t.schema.Name, childLevel)
	targetPrefix := make([]byte, 0, len(childLevelPrefix)+len(prefix))
	targetPrefix = append(targetPrefix, childLevelPrefix...)
	targetPrefix = append(targetPrefix, prefix...)

	it := txn.NewIterator(store.IteratorOptions{Prefix: targetPrefix})
	defer it.Close()

	hasher := sha256.New()
	childCount := 0
	it.Seek(targetPrefix)
	for it.ValidForPrefix(targetPrefix) {
		keyRaw, valRaw, err := it.Item()
		if err != nil {
			return fmt.Errorf("scan child nodes failed: level=%d, prefix=%s, err=%w", level, prefix, err)
		}
		fullChildPrefix := string(keyRaw[len(childLevelPrefix):])
		if len(fullChildPrefix) != childLevel {
			return fmt.Errorf("invalid child prefix length: level=%d, child=%q", childLevel, fullChildPrefix)
		}
		if !strings.HasPrefix(fullChildPrefix, prefix) {
			it.Next()
			continue
		}
		if len(valRaw) != merkleNodeHashSize {
			return fmt.Errorf("invalid child hash length: level=%d, len=%d", childLevel, len(valRaw))
		}
		childNibble := fullChildPrefix[level]
		hasher.Write([]byte{0x01, childNibble})
		hasher.Write(valRaw)
		childCount++
		it.Next()
	}

	nodeKey := merkleNodeKey(t.schema.Name, level, prefix)
	if childCount == 0 {
		if err := txn.Delete(nodeKey); err != nil {
			return fmt.Errorf("delete empty node failed: level=%d, prefix=%s, err=%w", level, prefix, err)
		}
		return nil
	}
	if err := txn.Set(nodeKey, hasher.Sum(nil), 0); err != nil {
		return fmt.Errorf("write internal node failed: level=%d, prefix=%s, err=%w", level, prefix, err)
	}
	return nil
}

func clearPrefix(txn store.Tx, prefix []byte) error {
	it := txn.NewIterator(store.IteratorOptions{Prefix: prefix})
	defer it.Close()

	keys := make([][]byte, 0, 64)
	it.Seek(prefix)
	for it.ValidForPrefix(prefix) {
		keyRaw, _, err := it.Item()
		if err != nil {
			return err
		}
		k := make([]byte, len(keyRaw))
		copy(k, keyRaw)
		keys = append(keys, k)
		it.Next()
	}

	for _, k := range keys {
		if err := txn.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) MerkleRootHash() (string, error) {
	var out string
	err := t.inTx(true, func(txn store.Tx) error {
		if err := t.ensureMerkleIndex(txn); err != nil {
			return err
		}
		raw, err := txn.Get(merkleNodeKey(t.schema.Name, 0, ""))
		if err == store.ErrKeyNotFound {
			out = ""
			return nil
		}
		if err != nil {
			return fmt.Errorf("read merkle root hash failed: %w", err)
		}
		out = decodeMerkleHashHex(raw)
		return nil
	})
	return out, err
}

func (t *Table) MerkleChildren(level int, prefix string) (map[string]string, error) {
	if level < 0 || level >= merkleMaxLevel {
		return nil, fmt.Errorf("invalid merkle level: %d", level)
	}
	prefix = normalizeNibblePrefix(prefix)
	if !validNibblePrefix(prefix, level) {
		return nil, fmt.Errorf("invalid merkle prefix %q for level %d", prefix, level)
	}

	children := make(map[string]string)
	err := t.inTx(true, func(txn store.Tx) error {
		if err := t.ensureMerkleIndex(txn); err != nil {
			return err
		}

		childLevel := level + 1
		childPrefixBase := merkleNodeLevelPrefix(t.schema.Name, childLevel)
		targetPrefix := make([]byte, 0, len(childPrefixBase)+len(prefix))
		targetPrefix = append(targetPrefix, childPrefixBase...)
		targetPrefix = append(targetPrefix, prefix...)

		it := txn.NewIterator(store.IteratorOptions{Prefix: targetPrefix})
		defer it.Close()

		it.Seek(targetPrefix)
		for it.ValidForPrefix(targetPrefix) {
			keyRaw, valRaw, err := it.Item()
			if err != nil {
				return fmt.Errorf("scan merkle children failed: %w", err)
			}
			fullChildPrefix := string(keyRaw[len(childPrefixBase):])
			if len(fullChildPrefix) != childLevel || !strings.HasPrefix(fullChildPrefix, prefix) {
				it.Next()
				continue
			}
			nibble := fullChildPrefix[level : level+1]
			children[nibble] = decodeMerkleHashHex(valRaw)
			it.Next()
		}
		return nil
	})
	return children, err
}

func (t *Table) MerkleLeafRows(prefix string) (map[string]string, error) {
	prefix = normalizeNibblePrefix(prefix)
	if !validNibblePrefix(prefix, merkleMaxLevel) {
		return nil, fmt.Errorf("invalid merkle leaf prefix %q", prefix)
	}

	out := make(map[string]string)
	err := t.inTx(true, func(txn store.Tx) error {
		if err := t.ensureMerkleIndex(txn); err != nil {
			return err
		}

		rowPrefix := merkleRowPrefix(t.schema.Name)
		prefixBytes, err := nibblePrefixToBytes(prefix)
		if err != nil {
			return err
		}
		targetPrefix := make([]byte, 0, len(rowPrefix)+len(prefixBytes))
		targetPrefix = append(targetPrefix, rowPrefix...)
		targetPrefix = append(targetPrefix, prefixBytes...)

		it := txn.NewIterator(store.IteratorOptions{Prefix: targetPrefix})
		defer it.Close()

		it.Seek(targetPrefix)
		for it.ValidForPrefix(targetPrefix) {
			keyRaw, valRaw, err := it.Item()
			if err != nil {
				return fmt.Errorf("scan merkle leaf rows failed: %w", err)
			}
			uuidBytes := keyRaw[len(rowPrefix):]
			if len(uuidBytes) != 16 {
				return fmt.Errorf("invalid merkle row key length: %d", len(uuidBytes))
			}
			u, err := uuid.FromBytes(uuidBytes)
			if err != nil {
				return fmt.Errorf("parse merkle row key failed: %w", err)
			}
			out[u.String()] = decodeMerkleHashHex(valRaw)
			it.Next()
		}
		return nil
	})
	return out, err
}

func (t *Table) MerkleNodeHash(level int, prefix string) (string, error) {
	if level < 0 || level > merkleMaxLevel {
		return "", fmt.Errorf("invalid merkle level: %d", level)
	}
	prefix = normalizeNibblePrefix(prefix)
	if !validNibblePrefix(prefix, level) {
		return "", fmt.Errorf("invalid merkle prefix %q for level %d", prefix, level)
	}

	var out string
	err := t.inTx(true, func(txn store.Tx) error {
		if err := t.ensureMerkleIndex(txn); err != nil {
			return err
		}
		raw, err := txn.Get(merkleNodeKey(t.schema.Name, level, prefix))
		if err == store.ErrKeyNotFound {
			out = ""
			return nil
		}
		if err != nil {
			return fmt.Errorf("read merkle node hash failed: %w", err)
		}
		out = decodeMerkleHashHex(raw)
		return nil
	})
	return out, err
}

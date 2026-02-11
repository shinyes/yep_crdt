package store

import (
	"os"
	"path/filepath"
	"testing"
)

func setupBadgerStore(t *testing.T) *BadgerStore {
	// Create a temporary directory for the test database
	tmpDir := filepath.Join(os.TempDir(), "badger-test-"+t.Name())
	
	// Clean up any existing directory
	os.RemoveAll(tmpDir)

	store, err := NewBadgerStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create BadgerStore: %v", err)
	}

	return store
}

func teardownBadgerStore(t *testing.T, store *BadgerStore) {
	if err := store.Close(); err != nil {
		t.Errorf("Failed to close BadgerStore: %v", err)
	}

	// Clean up the temporary directory
	tmpDir := filepath.Join(os.TempDir(), "badger-test-"+t.Name())
	os.RemoveAll(tmpDir)
}

func TestBadgerStore_Close(t *testing.T) {
	store := setupBadgerStore(t)
	defer teardownBadgerStore(t, store)

	err := store.Close()
	if err != nil {
		t.Fatalf("Close() failed: %v", err)
	}
}

func TestBadgerStore_UpdateAndGet(t *testing.T) {
	store := setupBadgerStore(t)
	defer teardownBadgerStore(t, store)

	err := store.Update(func(tx Tx) error {
		return tx.Set([]byte("key1"), []byte("value1"), 0)
	})
	if err != nil {
		t.Fatalf("Update() failed: %v", err)
	}

	err = store.View(func(tx Tx) error {
		val, err := tx.Get([]byte("key1"))
		if err != nil {
			return err
		}
		if string(val) != "value1" {
			t.Errorf("Got value %s, want value1", string(val))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() failed: %v", err)
	}
}

func TestBadgerStore_GetNotFound(t *testing.T) {
	store := setupBadgerStore(t)
	defer teardownBadgerStore(t, store)

	err := store.View(func(tx Tx) error {
		_, err := tx.Get([]byte("nonexistent"))
		if err != ErrKeyNotFound {
			t.Errorf("Expected ErrKeyNotFound, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() failed: %v", err)
	}
}

func TestBadgerStore_Delete(t *testing.T) {
	store := setupBadgerStore(t)
	defer teardownBadgerStore(t, store)

	// First, set a key
	err := store.Update(func(tx Tx) error {
		return tx.Set([]byte("key1"), []byte("value1"), 0)
	})
	if err != nil {
		t.Fatalf("Update() failed: %v", err)
	}

	// Verify key exists
	err = store.View(func(tx Tx) error {
		val, err := tx.Get([]byte("key1"))
		if err != nil {
			return err
		}
		if string(val) != "value1" {
			t.Errorf("Got value %s, want value1", string(val))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() failed: %v", err)
	}

	// Delete the key
	err = store.Update(func(tx Tx) error {
		return tx.Delete([]byte("key1"))
	})
	if err != nil {
		t.Fatalf("Update() failed: %v", err)
	}

	// Verify key is deleted
	err = store.View(func(tx Tx) error {
		_, err := tx.Get([]byte("key1"))
		if err != ErrKeyNotFound {
			t.Errorf("Expected ErrKeyNotFound, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() failed: %v", err)
	}
}

func TestBadgerStore_UpdateMultipleKeys(t *testing.T) {
	store := setupBadgerStore(t)
	defer teardownBadgerStore(t, store)

	keys := []string{"key1", "key2", "key3"}
	values := []string{"value1", "value2", "value3"}

	// Set multiple keys in one transaction
	err := store.Update(func(tx Tx) error {
		for i := range keys {
			if err := tx.Set([]byte(keys[i]), []byte(values[i]), 0); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update() failed: %v", err)
	}

	// Verify all keys exist
	err = store.View(func(tx Tx) error {
		for i := range keys {
			val, err := tx.Get([]byte(keys[i]))
			if err != nil {
				return err
			}
			if string(val) != values[i] {
				t.Errorf("Key %s: got value %s, want %s", keys[i], string(val), values[i])
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() failed: %v", err)
	}
}

func TestBadgerStore_Overwrite(t *testing.T) {
	store := setupBadgerStore(t)
	defer teardownBadgerStore(t, store)

	// Set a key
	err := store.Update(func(tx Tx) error {
		return tx.Set([]byte("key1"), []byte("value1"), 0)
	})
	if err != nil {
		t.Fatalf("Update() failed: %v", err)
	}

	// Overwrite the key
	err = store.Update(func(tx Tx) error {
		return tx.Set([]byte("key1"), []byte("value2"), 0)
	})
	if err != nil {
		t.Fatalf("Update() failed: %v", err)
	}

	// Verify the new value
	err = store.View(func(tx Tx) error {
		val, err := tx.Get([]byte("key1"))
		if err != nil {
			return err
		}
		if string(val) != "value2" {
			t.Errorf("Got value %s, want value2", string(val))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() failed: %v", err)
	}
}

func TestBadgerStore_EmptyKey(t *testing.T) {
	store := setupBadgerStore(t)
	defer teardownBadgerStore(t, store)

	// BadgerDB doesn't support empty keys, so we expect an error
	err := store.Update(func(tx Tx) error {
		return tx.Set([]byte(""), []byte("value1"), 0)
	})
	if err == nil {
		t.Error("Expected error for empty key, got nil")
	}
}

func TestBadgerStore_EmptyValue(t *testing.T) {
	store := setupBadgerStore(t)
	defer teardownBadgerStore(t, store)

	err := store.Update(func(tx Tx) error {
		return tx.Set([]byte("key1"), []byte(""), 0)
	})
	if err != nil {
		t.Fatalf("Update() failed: %v", err)
	}

	err = store.View(func(tx Tx) error {
		val, err := tx.Get([]byte("key1"))
		if err != nil {
			return err
		}
		if len(val) != 0 {
			t.Errorf("Got value %s, want empty", string(val))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() failed: %v", err)
	}
}

func TestBadgerStore_LargeKeyValue(t *testing.T) {
	store := setupBadgerStore(t)
	defer teardownBadgerStore(t, store)

	// Create large key and value (1KB)
	largeKey := make([]byte, 1024)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}

	largeValue := make([]byte, 1024)
	for i := range largeValue {
		largeValue[i] = byte((i + 128) % 256)
	}

	err := store.Update(func(tx Tx) error {
		return tx.Set(largeKey, largeValue, 0)
	})
	if err != nil {
		t.Fatalf("Update() failed: %v", err)
	}

	err = store.View(func(tx Tx) error {
		val, err := tx.Get(largeKey)
		if err != nil {
			return err
		}
		if len(val) != len(largeValue) {
			t.Errorf("Got value length %d, want %d", len(val), len(largeValue))
		}
		for i := range val {
			if val[i] != largeValue[i] {
				t.Errorf("Value mismatch at position %d", i)
				break
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() failed: %v", err)
	}
}

func TestBadgerStore_IteratorForward(t *testing.T) {
	store := setupBadgerStore(t)
	defer teardownBadgerStore(t, store)

	// Insert some keys
	keys := []string{"a", "b", "c", "d"}
	err := store.Update(func(tx Tx) error {
		for _, key := range keys {
			if err := tx.Set([]byte(key), []byte("value"), 0); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update() failed: %v", err)
	}

	// Iterate forward
	err = store.View(func(tx Tx) error {
		it := tx.NewIterator(IteratorOptions{})
		defer it.Close()
		
		it.Rewind()
		
		expectedKeys := keys
		for i, expectedKey := range expectedKeys {
			if !it.Valid() {
				t.Errorf("Iterator invalid at position %d", i)
				break
			}
			
			key, value, err := it.Item()
			if err != nil {
				t.Errorf("Item() failed at position %d: %v", i, err)
				break
			}
			
			if string(key) != expectedKey {
				t.Errorf("At position %d: got key %s, want %s", i, string(key), expectedKey)
			}
			
			if string(value) != "value" {
				t.Errorf("At position %d: got value %s, want 'value'", i, string(value))
			}
			
			it.Next()
		}
		
		return nil
	})
	if err != nil {
		t.Fatalf("View() failed: %v", err)
	}
}

func TestBadgerStore_IteratorReverse(t *testing.T) {
	store := setupBadgerStore(t)
	defer teardownBadgerStore(t, store)

	// Insert some keys
	keys := []string{"a", "b", "c", "d"}
	err := store.Update(func(tx Tx) error {
		for _, key := range keys {
			if err := tx.Set([]byte(key), []byte("value"), 0); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update() failed: %v", err)
	}

	// Iterate in reverse
	err = store.View(func(tx Tx) error {
		it := tx.NewIterator(IteratorOptions{Reverse: true})
		defer it.Close()
		
		it.Rewind()
		
		expectedKeys := []string{"d", "c", "b", "a"}
		for i, expectedKey := range expectedKeys {
			if !it.Valid() {
				t.Errorf("Iterator invalid at position %d", i)
				break
			}
			
			key, value, err := it.Item()
			if err != nil {
				t.Errorf("Item() failed at position %d: %v", i, err)
				break
			}
			
			if string(key) != expectedKey {
				t.Errorf("At position %d: got key %s, want %s", i, string(key), expectedKey)
			}
			
			if string(value) != "value" {
				t.Errorf("At position %d: got value %s, want 'value'", i, string(value))
			}
			
			it.Next()
		}
		
		return nil
	})
	if err != nil {
		t.Fatalf("View() failed: %v", err)
	}
}

func TestBadgerStore_IteratorSeek(t *testing.T) {
	store := setupBadgerStore(t)
	defer teardownBadgerStore(t, store)

	// Insert some keys
	keys := []string{"a", "b", "c", "d"}
	err := store.Update(func(tx Tx) error {
		for _, key := range keys {
			if err := tx.Set([]byte(key), []byte("value"), 0); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update() failed: %v", err)
	}

	// Seek to "c"
	err = store.View(func(tx Tx) error {
		it := tx.NewIterator(IteratorOptions{})
		defer it.Close()
		
		it.Seek([]byte("c"))
		
		if !it.Valid() {
			t.Error("Iterator invalid after Seek")
			return nil
		}
		
		key, value, err := it.Item()
		if err != nil {
			t.Fatalf("Item() failed: %v", err)
		}
		
		if string(key) != "c" {
			t.Errorf("Got key %s, want 'c'", string(key))
		}
		
		if string(value) != "value" {
			t.Errorf("Got value %s, want 'value'", string(value))
		}
		
		// Move to next
		it.Next()
		
		if !it.Valid() {
			t.Error("Iterator invalid after Next")
			return nil
		}
		
		key, value, err = it.Item()
		if err != nil {
			t.Fatalf("Item() failed: %v", err)
		}
		
		if string(key) != "d" {
			t.Errorf("Got key %s, want 'd'", string(key))
		}
		
		return nil
	})
	if err != nil {
		t.Fatalf("View() failed: %v", err)
	}
}

func TestBadgerStore_IteratorPrefix(t *testing.T) {
	store := setupBadgerStore(t)
	defer teardownBadgerStore(t, store)

	// Insert keys with different prefixes
	err := store.Update(func(tx Tx) error {
		keys := []string{"a1", "a2", "a3", "b1", "b2", "c1"}
		for _, key := range keys {
			if err := tx.Set([]byte(key), []byte("value"), 0); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update() failed: %v", err)
	}

	// Iterate with prefix "a"
	err = store.View(func(tx Tx) error {
		it := tx.NewIterator(IteratorOptions{Prefix: []byte("a")})
		defer it.Close()
		
		it.Rewind()
		
		count := 0
		expectedKeys := []string{"a1", "a2", "a3"}
		for i, expectedKey := range expectedKeys {
			if !it.ValidForPrefix([]byte("a")) {
				t.Errorf("Iterator invalid at position %d", i)
				break
			}
			
			key, _, err := it.Item()
			if err != nil {
				t.Errorf("Item() failed at position %d: %v", i, err)
				break
			}
			
			if string(key) != expectedKey {
				t.Errorf("At position %d: got key %s, want %s", i, string(key), expectedKey)
			}
			
			count++
			it.Next()
		}
		
		if count != 3 {
			t.Errorf("Iterated %d keys, want 3", count)
		}
		
		return nil
	})
	if err != nil {
		t.Fatalf("View() failed: %v", err)
	}
}

func TestBadgerStore_Persistence(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "badger-test-persist")
	
	// Clean up any existing directory
	os.RemoveAll(tmpDir)
	
	// Create store and add data
	store1, err := NewBadgerStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create BadgerStore: %v", err)
	}
	
	err = store1.Update(func(tx Tx) error {
		return tx.Set([]byte("key1"), []byte("value1"), 0)
	})
	if err != nil {
		t.Fatalf("Update() failed: %v", err)
	}
	
	err = store1.Close()
	if err != nil {
		t.Fatalf("Close() failed: %v", err)
	}
	
	// Open the store again and verify data
	store2, err := NewBadgerStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create BadgerStore: %v", err)
	}
	defer func() {
		store2.Close()
		os.RemoveAll(tmpDir)
	}()
	
	err = store2.View(func(tx Tx) error {
		val, err := tx.Get([]byte("key1"))
		if err != nil {
			return err
		}
		if string(val) != "value1" {
			t.Errorf("Got value %s, want value1", string(val))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View() failed: %v", err)
	}
}

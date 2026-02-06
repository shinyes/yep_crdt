package manager_test

import (
	"os"
	"testing"

	"github.com/shinyes/yep_crdt/crdt"
	"github.com/shinyes/yep_crdt/manager"
)

func setupManager(t *testing.T) *manager.Manager {
	dbPath := "./test_query_db"
	blobPath := "./test_query_blobs"
	os.RemoveAll(dbPath)
	os.RemoveAll(blobPath)

	m, err := manager.NewManager(dbPath, blobPath)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func cleanupManager(m *manager.Manager) {
	m.Close()
	os.RemoveAll("./test_query_db")
	os.RemoveAll("./test_query_blobs")
}

func TestQuery_SetAndGet(t *testing.T) {
	m := setupManager(t)
	defer cleanupManager(m)

	rootID := "user_profile"
	m.CreateRoot(rootID, crdt.TypeMap)

	// Test Set simple key
	err := m.From(rootID).Update().
		Set("name", "Alice").
		Set("age", 30).
		Commit()
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Test Get
	val, err := m.From(rootID).Select("name", "age").Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	resMap, ok := val.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected map result, got %T", val)
	}

	if resMap["name"] != "Alice" {
		t.Errorf("Expected name 'Alice', got %v", resMap["name"])
	}
	if resMap["age"] != 30 {
		t.Errorf("Expected age 30, got %v", resMap["age"])
	}
}

func TestQuery_NestedKeys(t *testing.T) {
	m := setupManager(t)
	defer cleanupManager(m)

	rootID := "config"
	m.CreateRoot(rootID, crdt.TypeMap)

	// Test Set nested key (auto-create intermediate maps)
	err := m.From(rootID).Update().
		Set("server.http.port", 8080).
		Set("server.http.host", "localhost").
		Commit()

	if err != nil {
		t.Fatalf("Nested update failed: %v", err)
	}

	// Test Select nested key
	val, err := m.From(rootID).Select("server.http.port").Get()
	if err != nil {
		t.Fatalf("Get nested failed: %v", err)
	}

	if val != 8080 {
		t.Errorf("Expected port 8080, got %v", val)
	}

	// Verify structure
	valRoot, _ := m.From(rootID).Get()
	t.Logf("Full structure: %v", valRoot)
}

func TestQuery_Inc(t *testing.T) {
	m := setupManager(t)
	defer cleanupManager(m)

	rootID := "stats"
	m.CreateRoot(rootID, crdt.TypeMap)

	// Test Inc (auto-init)
	err := m.From(rootID).Update().
		Inc("visitors", 1).
		Commit()
	if err != nil {
		t.Fatalf("Inc failed: %v", err)
	}

	val, err := m.From(rootID).Select("visitors").Get()
	if val != int64(1) {
		t.Errorf("Expected 1, got %v", val)
	}

	// Test Inc again
	m.From(rootID).Update().Inc("visitors", 5).Commit()
	val, _ = m.From(rootID).Select("visitors").Get()
	if val != int64(6) {
		t.Errorf("Expected 6, got %v", val)
	}
}

func TestQuery_Delete(t *testing.T) {
	m := setupManager(t)
	defer cleanupManager(m)

	rootID := "temp"
	m.CreateRoot(rootID, crdt.TypeMap)

	m.From(rootID).Update().Set("temp_key", "value").Commit()

	// Verify exists
	val, _ := m.From(rootID).Select("temp_key").Get()
	if val != "value" {
		t.Fatalf("Setup failed")
	}

	// Delete
	err := m.From(rootID).Update().Delete("temp_key").Commit()
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify gone
	val, _ = m.From(rootID).Select("temp_key").Get()
	if val != nil {
		t.Errorf("Expected nil after delete, got %v", val)
	}
}

func TestQuery_SetOperations(t *testing.T) {
	m := setupManager(t)
	defer cleanupManager(m)

	rootID := "sets_test"
	m.CreateRoot(rootID, crdt.TypeMap)

	// Test AddToSet (auto-init)
	err := m.From(rootID).Update().
		AddToSet("tags", "golang").
		AddToSet("tags", "crdt").
		AddToSet("tags", "distributed").
		Commit()
	if err != nil {
		t.Fatalf("AddToSet failed: %v", err)
	}

	// Get set value
	val, err := m.From(rootID).Select("tags").Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	tags, ok := val.([]interface{})
	if !ok {
		t.Fatalf("Expected slice, got %T", val)
	}

	if len(tags) != 3 {
		t.Errorf("Expected 3 tags, got %d: %v", len(tags), tags)
	}

	// Test RemoveFromSet
	err = m.From(rootID).Update().
		RemoveFromSet("tags", "crdt").
		Commit()
	if err != nil {
		t.Fatalf("RemoveFromSet failed: %v", err)
	}

	val, _ = m.From(rootID).Select("tags").Get()
	tags = val.([]interface{})
	if len(tags) != 2 {
		t.Errorf("Expected 2 tags after remove, got %d: %v", len(tags), tags)
	}

	// Verify "crdt" is removed
	found := false
	for _, tag := range tags {
		if tag == "crdt" {
			found = true
			break
		}
	}
	if found {
		t.Errorf("Expected 'crdt' to be removed, but it still exists")
	}
}

func TestQuery_SequenceOperations(t *testing.T) {
	m := setupManager(t)
	defer cleanupManager(m)

	rootID := "sequence_test"
	m.CreateRoot(rootID, crdt.TypeMap)

	// Test Append (auto-init)
	err := m.From(rootID).Update().
		Append("items", "first").
		Append("items", "second").
		Append("items", "third").
		Commit()
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Get sequence with IDs
	elems, err := m.From(rootID).GetSequence("items")
	if err != nil {
		t.Fatalf("GetSequence failed: %v", err)
	}

	if len(elems) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(elems))
	}

	// Verify order
	if elems[0].Value != "first" || elems[1].Value != "second" || elems[2].Value != "third" {
		t.Errorf("Unexpected order: %v", elems)
	}

	// Test InsertAt (insert at beginning)
	err = m.From(rootID).Update().
		InsertAt("items", "start", "zeroth").
		Commit()
	if err != nil {
		t.Fatalf("InsertAt failed: %v", err)
	}

	elems, _ = m.From(rootID).GetSequence("items")
	if len(elems) != 4 || elems[0].Value != "zeroth" {
		t.Errorf("InsertAt at start failed: %v", elems)
	}

	// Test RemoveAt
	secondID := elems[2].ID // "second"
	err = m.From(rootID).Update().
		RemoveAt("items", secondID).
		Commit()
	if err != nil {
		t.Fatalf("RemoveAt failed: %v", err)
	}

	elems, _ = m.From(rootID).GetSequence("items")
	if len(elems) != 3 {
		t.Errorf("Expected 3 elements after remove, got %d", len(elems))
	}

	// Verify "second" is removed
	for _, elem := range elems {
		if elem.Value == "second" {
			t.Errorf("'second' should have been removed")
		}
	}
}

func TestManager_RootManagement(t *testing.T) {
	m := setupManager(t)
	defer cleanupManager(m)

	// Create some roots
	m.CreateRoot("root1", crdt.TypeMap)
	m.CreateRoot("root2", crdt.TypeCounter)
	m.CreateRoot("root3", crdt.TypeSet)

	// Test Exists
	if !m.Exists("root1") {
		t.Error("root1 should exist")
	}
	if m.Exists("nonexistent") {
		t.Error("nonexistent should not exist")
	}

	// Test ListRoots
	roots, err := m.ListRoots()
	if err != nil {
		t.Fatalf("ListRoots failed: %v", err)
	}

	if len(roots) < 3 {
		t.Errorf("Expected at least 3 roots, got %d", len(roots))
	}

	// Test DeleteRoot
	err = m.DeleteRoot("root2")
	if err != nil {
		t.Fatalf("DeleteRoot failed: %v", err)
	}

	if m.Exists("root2") {
		t.Error("root2 should have been deleted")
	}
}

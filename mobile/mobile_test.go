package mobile_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/shinyes/yep_crdt/mobile"
)

func TestMobileWrapper(t *testing.T) {
	dbPath := "./test_mobile_db"
	blobPath := "./test_mobile_blobs"
	os.RemoveAll(dbPath)
	os.RemoveAll(blobPath)
	defer os.RemoveAll(dbPath)
	defer os.RemoveAll(blobPath)

	// Direct manager setup to create root type first (mobile wrapper assumes root exists typically or auto-creates?
	// Wait, mobile wrapper delegates to manager.Manager. The Manager.CreateRoot is not exposed in MobileManager yet!
	// I should add CreateRoot to MobileManager or checks if it is needed.
	// Let's assume we need to expose CreateMapRoot at least.

	mm, err := mobile.NewMobileManager(dbPath, blobPath)
	if err != nil {
		t.Fatal(err)
	}
	defer mm.Close()

	// Initialize root
	if err := mm.CreateMapRoot("user"); err != nil {
		t.Fatal(err)
	}

	// Test Set
	if err := mm.From("user").SetString("name", "Alice"); err != nil {
		t.Fatal(err)
	}
	if err := mm.From("user").SetInt("age", 30); err != nil {
		t.Fatal(err)
	}

	// Test GetAsJSON
	jsonStr, err := mm.From("user").Select("name,age").GetAsJSON()
	if err != nil {
		t.Fatal(err)
	}

	var res map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &res); err != nil {
		t.Fatal(err)
	}

	if res["name"] != "Alice" || res["age"] != float64(30) { // JSON numbers are floats
		t.Errorf("Unexpected JSON: %s", jsonStr)
	}
}

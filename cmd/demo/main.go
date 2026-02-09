package main

import (
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func main() {
	// 1. 初始化 Store
	dbPath := "./tmp/db_demo/tenant1"
	os.RemoveAll(dbPath) // 清理开始
	os.MkdirAll(dbPath, 0755)

	s, err := store.NewBadgerStore(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// 2. 打开 DB (租户)
	myDB := db.Open(s, "tenant-1")

	// 3. 定义 Schema
	// 3. Define Schema (With CRDT Types!)
	err = myDB.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "age", Type: meta.ColTypeInt, CrdtType: meta.CrdtLWW},
			{Name: "views", Type: meta.ColTypeInt, CrdtType: meta.CrdtCounter},
			{Name: "tags", Type: meta.ColTypeString, CrdtType: meta.CrdtORSet},
			{Name: "todos", Type: meta.ColTypeString, CrdtType: meta.CrdtRGA},
		},
		Indexes: []meta.IndexSchema{
			{Name: "idx_age", Columns: []string{"age"}, Unique: false},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	table := myDB.Table("users")

	// 4. Basic Insert (LWW)
	u1Key, _ := uuid.NewV7()
	u2Key, _ := uuid.NewV7()

	fmt.Println(">>> Inserting Data (LWW)...")
	table.Add(u1Key, "name", "Alice")
	table.Add(u1Key, "age", 30)

	// 5. Counter Inc/Dec
	fmt.Println(">>> Incrementing Views (Counter)...")
	table.Add(u1Key, "views", 10) // 10
	table.Add(u1Key, "views", 5)  // 15
	fmt.Println(">>> Decrementing Views (Counter)...")
	table.Remove(u1Key, "views", 3) // 12

	// 6. Set Add/Remove
	fmt.Println(">>> Modifying Tags (ORSet)...")
	table.Add(u1Key, "tags", "golang")
	table.Add(u1Key, "tags", "rust")
	table.Remove(u1Key, "tags", "rust") // Should remove rust

	// 7. RGA Operations
	fmt.Println(">>> Managing Todos (RGA)...")
	table.Add(u1Key, "todos", "Task 1")
	table.Add(u1Key, "todos", "Task 2") // [Task 1, Task 2]

	// Create another row to test InsertAfter
	table.Add(u2Key, "todos", "Start")
	table.InsertAfter(u2Key, "todos", "Start", "Middle") // [Start, Middle]
	table.Add(u2Key, "todos", "End")                     // [Start, Middle, End]

	// Test InsertAt
	fmt.Println(">>> RGA InsertAt...")
	table.InsertAt(u2Key, "todos", 0, "VeryFirst") // [VeryFirst, Start, Middle, End]
	table.RemoveAt(u2Key, "todos", 2)              // Remove "Middle" -> [VeryFirst, Start, End]

	// Query Check
	u1, _ := table.Get(u1Key)
	u2, _ := table.Get(u2Key)
	fmt.Printf("User U1: %v\n", u1)
	fmt.Printf("User U2: %v\n", u2)

	// 8. Advanced Queries
	fmt.Println("\n>>> Query: Find WHERE age > 20 LIMIT 5...")
	results, err := table.Where("age", db.OpGt, 20).Limit(5).Find()
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range results {
		fmt.Printf("Row: %v\n", row)
	}

	// 9. Transactions
	fmt.Println("\n>>> Transaction: Update two users atomically...")
	err = myDB.Update(func(tx *db.Tx) error {
		t := tx.Table("users")

		// Update Alice (u1)
		err := t.Add(u1Key, "views", 100)
		if err != nil {
			return err
		}

		// Update another user (u2)
		// If this fails, Alice's update should also be rolled back (if Badger supports it within same txn)
		// Here we just demo happy path
		err = t.Add(u2Key, "views", 100)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Verify
	u1New, _ := table.Get(u1Key)
	u2New, _ := table.Get(u2Key)
	fmt.Printf("User U1 Views: %v\n", u1New["views"])
	fmt.Printf("User U2 Views: %v\n", u2New["views"])

	// 10. Query Enhancements
	fmt.Println("\n>>> Query: In [100, 200]...")
	// Add name to U2
	err = table.Set(u2Key, map[string]any{
		"name": "Bob",
		"age":  25,
	})
	if err != nil {
		log.Fatal(err)
	}
	results, err = table.Where("views", db.OpIn, []any{int64(100), int64(200)}).Find()
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range results {
		fmt.Printf("In Result: %s\n", row["name"])
	}

	fmt.Println("\n>>> Query: Order By Age DESC...")
	results, err = table.Where("age", db.OpGt, 0).OrderBy("age", true).Find()
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range results {
		fmt.Printf("Ordered Row: %s, Age: %s\n", row["name"], row["age"])
	}
}

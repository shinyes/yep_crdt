package db

import (
	"errors"
	"fmt"
	"os"

	"github.com/shinyes/yep_crdt/pkg/store"
)

func ExampleOpen() {
	dir, err := os.MkdirTemp("", "yep-open-example-")
	if err != nil {
		fmt.Println("open store failed")
		return
	}
	defer os.RemoveAll(dir)

	s, err := store.NewBadgerStore(dir)
	if err != nil {
		fmt.Println("open store failed")
		return
	}
	defer s.Close()

	database, err := Open(s, "tenant-example")
	if err != nil {
		fmt.Println("open db failed")
		return
	}
	defer database.Close()

	fmt.Println(database.DatabaseID)
	// Output:
	// tenant-example
}

func ExampleOpen_databaseIDMismatch() {
	dir, err := os.MkdirTemp("", "yep-open-mismatch-example-")
	if err != nil {
		fmt.Println("open store failed")
		return
	}
	defer os.RemoveAll(dir)

	s, err := store.NewBadgerStore(dir)
	if err != nil {
		fmt.Println("open store failed")
		return
	}
	defer s.Close()

	db1, err := Open(s, "tenant-a")
	if err != nil {
		fmt.Println("first open failed")
		return
	}
	defer db1.Close()

	_, err = Open(s, "tenant-b")
	fmt.Println(errors.Is(err, ErrDatabaseIDMismatch))

	var mismatch *DatabaseIDMismatchError
	if errors.As(err, &mismatch) {
		fmt.Println(mismatch.StoredDatabaseID)
		fmt.Println(mismatch.ProvidedDatabaseID)
	}

	// Output:
	// true
	// tenant-a
	// tenant-b
}

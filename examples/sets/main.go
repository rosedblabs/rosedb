package main

import (
	"fmt"
	"github.com/flower-corp/rosedb"
	"path/filepath"
)

func main() {
	path := filepath.Join("/tmp", "rosedb")
	opts := rosedb.DefaultOptions(path)
	db, err := rosedb.Open(opts)
	if err != nil {
		fmt.Printf("open rosedb err: %v", err)
		return
	}

	err = db.SAdd([]byte("fruits"), []byte("watermelon"), []byte("grape"), []byte("orange"), []byte("apple"))
	if err != nil {
		fmt.Printf("SAdd error: %v", err)
	}

	err = db.SAdd([]byte("fav-fruits"), []byte("orange"), []byte("melon"), []byte("strawberry"))
	if err != nil {
		fmt.Printf("SAdd error: %v", err)
	}

	diffSet, err := db.SDiff([]byte("fruits"), []byte("fav-fruits"))
	if err != nil {
		fmt.Printf("SDiff error: %v", err)
	}
	fmt.Println("SDiff set:")
	for _, val := range diffSet {
		fmt.Printf("%v\n", string(val))
	}
}

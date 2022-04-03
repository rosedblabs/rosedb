package main

import (
	"fmt"
	"github.com/flower-corp/rosedb"
	"path/filepath"
	"time"
)

func main() {
	path := filepath.Join("/tmp", "rosedb")
	opts := rosedb.DefaultOptions(path)
	db, err := rosedb.Open(opts)
	if err != nil {
		fmt.Printf("open rosedb err: %v", err)
		return
	}

	err = db.Set([]byte("name"), []byte("RoseDB"))
	if err != nil {
		fmt.Printf("write data err: %v", err)
		return
	}

	v, err := db.Get([]byte("name"))
	if err != nil {
		fmt.Printf("read data err: %v", err)
		return
	}
	fmt.Println("val = ", string(v))

	err = db.SetEx([]byte("type"), []byte("RoseDB-Strs"), time.Second*5)
	if err != nil {
		fmt.Printf("write data err: %v", err)
		return
	}

	err = db.Delete([]byte("name"))
	if err != nil {
		fmt.Printf("delete data err: %v", err)
		return
	}
}

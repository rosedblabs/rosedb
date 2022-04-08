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

	err = db.SetEX([]byte("type"), []byte("RoseDB-Strs"), time.Second*5)
	if err != nil {
		fmt.Printf("write data err: %v", err)
		return
	}

	err = db.Delete([]byte("name"))
	if err != nil {
		fmt.Printf("delete data err: %v", err)
		return
	}

	err = db.SetNX([]byte("cmd"), []byte("SetNX"))
	if err != nil {
		fmt.Printf("write data err: %v", err)
		return
	}

	v, err = db.Get([]byte("cmd"))
	if err != nil {
		fmt.Printf("read data err: %v", err)
		return
	}
	fmt.Printf("cmd-type = %s\n", string(v))

	err = db.MSet([]byte("key-1"), []byte("value-1"), []byte("key-2"), []byte("value-2"))
	if err != nil {
		fmt.Printf("mset error: %v", err)
		return
	}
	fmt.Println("Multiple key-value pair added.")

	// Missing value.
	err = db.MSet([]byte("key-1"), []byte("value-1"), []byte("key-2"))
	if err != nil {
		fmt.Printf("mset error: %v", err)
		return
	}
}

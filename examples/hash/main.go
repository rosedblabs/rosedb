package main

import (
	"fmt"
	"path/filepath"

	"github.com/flower-corp/rosedb"
)

func main() {
	path := filepath.Join("/tmp", "rosedb")
	opts := rosedb.DefaultOptions(path)
	db, err := rosedb.Open(opts)
	if err != nil {
		fmt.Printf("open rosedb err: %v", err)
		return
	}

	err = db.HSet([]byte("watermelon"), []byte("hash"), []byte("In summer, I love watermelon."))
	if err != nil {
		fmt.Printf("HSet error: %v", err)
	}

	value, err := db.HGet([]byte("watermelon"), []byte("hash"))
	if err != nil {
		fmt.Printf("HGet error: %v", err)
	}
	fmt.Println(string(value))

	exist, err := db.HExists([]byte("watermelon"), []byte("hash"))
	if err != nil {
		fmt.Printf("HExists error: %v", err)
	}
	if exist {
		fmt.Println("Hash key watermelon exist.")
	}

	fields, err := db.HKeys([]byte("watermelon"))
	if err != nil {
		fmt.Printf("Hkeys error: %v", err)
	}
	fmt.Println("The fields in watermelon are:", fields)

	ok, err := db.HSetNX([]byte("key-1"), []byte("field-1"), []byte("value-1"))
	if err != nil {
		fmt.Printf("HSetNx error: %v", err)
	}
	fmt.Println(ok)

	value, err = db.HGet([]byte("key-1"), []byte("field-1"))
	if err != nil {
		fmt.Printf("Error when key-1/field-1 is trying to get: %v", err)
	}
	fmt.Printf("key-1/value-1: %s", string(value))
}

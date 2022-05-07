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

	// dataStruct: Ming, Jame, Tom
	err = db.LPush([]byte("students"), []byte("Tom"), []byte("Jame"), []byte("Ming"))
	if err != nil {
		fmt.Printf("write data err: %v", err)
		return
	}

	// dataStruct: Ming, Jame, Tom, Jack, Wei
	err = db.RPush([]byte("students"), []byte("Jack"), []byte("Wei"))
	if err != nil {
		fmt.Printf("write data err: %v", err)
		return
	}

	stuLens := db.LLen([]byte("students"))
	fmt.Println(stuLens)

	// out: Ming
	// dataStruct: Jame, Tom, Jack, Wei
	lPopStu, err := db.LPop([]byte("students"))
	if err != nil {
		fmt.Printf("lpop data err: %v", err)
		return
	}
	fmt.Println(string(lPopStu))

	// out: Wei
	// dataStruct: Jame, Tom, Jack
	rPopStu, err := db.RPop([]byte("students"))
	if err != nil {
		fmt.Printf("rpop data err: %v", err)
		return
	}
	fmt.Println(string(rPopStu))

}

package main

import (
	"fmt"
	"github.com/flower-corp/rosedb"
	"path/filepath"
	"strconv"
)

func main() {
	path := filepath.Join("/tmp", "rosedb")
	opts := rosedb.DefaultOptions(path)
	db, err := rosedb.Open(opts)
	if err != nil {
		fmt.Printf("open rosedb err: %v", err)
		return
	}

	for i := 1; i <= 10; i++ {
		err = db.ZAdd([]byte("zset-key"), float64(i*2), []byte("member-"+strconv.Itoa(i)))
		if err != nil {
			fmt.Printf("write data err: %v", err)
			return
		}
	}

	ok, score := db.ZScore([]byte("zset-key"), []byte("member-1"))
	if ok {
		fmt.Println("score is ", score)
	}

	err = db.ZRem([]byte("zset-key"), []byte("member-1"))
	if err != nil {
		fmt.Printf("delete data err: %v", err)
		return
	}

	card := db.ZCard([]byte("zset-key"))
	fmt.Println("card of zset-key : ", card)

	members, err := db.ZRange([]byte("zset-key"), 0, -1)
	if err != nil {
		fmt.Printf("get data err: %v", err)
		return
	}
	for _, v := range members {
		fmt.Println(string(v))
	}
}

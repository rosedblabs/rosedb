package main

import (
	"fmt"
	"github.com/roseduan/rosedb"
)

func main() {
	cfg := rosedb.DefaultConfig()
	cfg.DirPath = "/tmp/rosedb"
	db, err := rosedb.Open(cfg)
	if err != nil {
		panic(err)
	}

	//err = db.Set(11, 22)
	//if err != nil {
	//	panic(err)
	//}

	var v int
	err = db.Get(11, &v)
	if err != nil {
		panic(err)
	}

	fmt.Println("res = ", v)
}

package main

import (
	"github.com/roseduan/rosedb"
)

func main() {
	cfg := rosedb.DefaultConfig()
	cfg.DirPath = "/tmp/rosedb"
	db, err := rosedb.Open(cfg)
	if err != nil {
		panic(err)
	}
	if db == nil {
		//	...
	}
}

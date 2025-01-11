package main

import (
	"log"
	"runtime"
	"time"

	"github.com/rosedblabs/rosedb/v2"
)

// this file shows how to use the Expiry/TTL feature of rosedb.
func main() {
	// specify the options
	options := rosedb.DefaultOptions
	sysType := runtime.GOOS
	if sysType == "windows" {
		options.DirPath = "C:\\rosedb_ttl"
	} else {
		options.DirPath = "/tmp/rosedb_ttl"
	}

	// open a database
	db, err := rosedb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// when you put a key-value pair, you can specify the ttl.
	err = db.PutWithTTL([]byte("name"), []byte("rosedb"), time.Second*5)
	if err != nil {
		panic(err)
	}
	// now you can get the ttl of the key.
	ttl, err := db.TTL([]byte("name"))
	if err != nil {
		panic(err)
	}
	println(ttl.String())

	_ = db.Put([]byte("name2"), []byte("rosedb2"))
	//and you can also set the ttl of the key after you put it.
	err = db.Expire([]byte("name2"), time.Second*2)
	if err != nil {
		panic(err)
	}
	ttl, err = db.TTL([]byte("name2"))
	if err != nil {
		log.Println(err)
	}
	println(ttl.String())
}

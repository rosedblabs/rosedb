package main

import (
	"os"
	"runtime"

	"github.com/rosedblabs/rosedb/v2"
)

// this file shows how to use the basic operations of rosedb

func main() {

	// specify the options
	options := rosedb.DefaultOptions
	sysType := runtime.GOOS
	if sysType == "windows" {
		options.DirPath = "C:\\rosedb_basic"
	} else {
		options.DirPath = "/tmp/rosedb_basic"
	}

	//remove data dir, for test, there's no need to keep any file or directory on disk
	defer func() {
		_ = os.RemoveAll(options.DirPath)
	}()

	// open a database
	db, err := rosedb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// set a key
	err = db.Put([]byte("name"), []byte("rosedb"))
	if err != nil {
		panic(err)
	}

	// get a key
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	println(string(val))

	// delete a key
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}

package main

import (
	"os"
	"runtime"

	"github.com/rosedblabs/rosedb/v2"
)

// this file shows how to use the vector index of rosedb

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

	// insert a key that is a vector
	err = db.Put([]byte("(1,2)"), []byte("rose"))
	if err != nil {
		panic(err)
	}

	// update a key that is a vector
	err = db.Put([]byte("(1,2)"), []byte("db"))
	if err != nil {
		panic(err)
	}

	// get a key that is a vector
	val, err := db.Get([]byte("(1,2)"))
	if err != nil {
		panic(err)
	}
	println(string(val))

}

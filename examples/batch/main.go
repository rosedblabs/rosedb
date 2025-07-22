package main

import (
	"runtime"

	"github.com/rosedblabs/rosedb/v2"
)

// this file shows how to use the batch operations of rosedb

func main() {
	// specify the options
	options := rosedb.DefaultOptions
	sysType := runtime.GOOS
	if sysType == "windows" {
		options.DirPath = "C:\\rosedb_batch"
	} else {
		options.DirPath = "/tmp/rosedb_batch"
	}

	// open a database
	db, err := rosedb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// create a batch
	batch := db.NewBatch(rosedb.DefaultBatchOptions)

	// set a key
	_ = batch.Put([]byte("name"), []byte("rosedb"))

	// get a key
	val, _ := batch.Get([]byte("name"))
	println(string(val))

	// delete a key
	_ = batch.Delete([]byte("name"))

	// commit the batch
	_ = batch.Commit()

	// if you want to cancel batch, you must call rollback().
	// _= batch.Rollback()

	// once a batch is committed, it can't be used again
	// _ = batch.Put([]byte("name1"), []byte("rosedb1")) // don't do this!!!
}

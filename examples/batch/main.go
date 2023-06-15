package main

import (
	"github.com/rosedblabs/rosedb/v2"
)

// this file shows how to use the batch operations of rosedb

func main() {
	// specify the options
	options := rosedb.DefaultOptions
	options.DirPath = "/tmp/rosedb_batch"

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
	defer batch.Discard()

	// set a key
	_ = batch.Put([]byte("name"), []byte("rosedb"))

	// get a key
	val, _ := batch.Get([]byte("name"))
	println(string(val))

	// delete a key
	_ = batch.Delete([]byte("name"))

	// commit the batch
	_ = batch.Commit()

	// once a batch is committed, it can't be used again
	// _ = batch.Put([]byte("name1"), []byte("rosedb1")) // don't do this!!!
}

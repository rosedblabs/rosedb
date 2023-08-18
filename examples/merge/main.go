package main

import (
	"github.com/rosedblabs/rosedb/v2"
	"github.com/rosedblabs/rosedb/v2/utils"
)

// this file shows how to use the Merge feature of rosedb.
// Merge is used to merge the data files in the database.
// It is recommended to use it when the database is not busy.

func main() {
	// specify the options
	options := rosedb.DefaultOptions
	options.DirPath = "/tmp/rosedb_merge"

	// open a database
	db, err := rosedb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// write some data
	for i := 0; i < 100000; i++ {
		_ = db.Put([]byte(utils.GetTestKey(i)), utils.RandomValue(128))
	}
	// delete some data
	for i := 0; i < 100000/2; i++ {
		_ = db.Delete([]byte(utils.GetTestKey(i)))
	}

	// then merge the data files
	// all the invalid data will be removed, and the valid data will be merged into the new data files.
	_ = db.Merge(true)
}

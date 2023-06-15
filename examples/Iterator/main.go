package main

import (
	"fmt"

	"github.com/rosedblabs/rosedb/v2"
)

// this file shows how to use the iterator feature of rosedb.

func main() {
	// specify the options
	options := rosedb.DefaultOptions
	options.DirPath = "/tmp/rosedb_iterator"

	// open a database
	db, err := rosedb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// write some data
	_ = db.Put([]byte("bbna"), []byte("val-1"))
	_ = db.Put([]byte("cdba"), []byte("val-2"))
	_ = db.Put([]byte("eera"), []byte("val-3"))
	_ = db.Put([]byte("cdme"), []byte("val-4"))
	_ = db.Put([]byte("accg"), []byte("val-5"))
	_ = db.Put([]byte("ccgb"), []byte("val-6"))

	// create an iterator
	iterOptions := rosedb.DefaultIteratorOptions
	iter := db.NewIterator(iterOptions)
	defer iter.Close() // close the iterator after using it

	fmt.Println("----------Iterate all data in the database:")
	// iterate all data in the database
	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		val, _ := iter.Value()
		println(string(key), string(val))
	}

	// rewind the iterator, seek the first key in the iterator.
	iter.Rewind()

	fmt.Println("----------seek to a key:")
	// seek to a key
	for iter.Seek([]byte("cch")); iter.Valid(); iter.Next() {
		key := iter.Key()
		val, _ := iter.Value()
		println(string(key), string(val))
	}

	fmt.Println("----------reverse iterate:")
	// reverse iterate
	iterOptions.Reverse = true
	iter2 := db.NewIterator(iterOptions)
	defer iter2.Close()
	for ; iter2.Valid(); iter2.Next() {
		key := iter2.Key()
		val, _ := iter2.Value()
		println(string(key), string(val))
	}

	fmt.Println("----------iterate with prefix:")
	// iterate with prefix
	iterOptions.Prefix = []byte("c")
	iter3 := db.NewIterator(iterOptions)
	defer iter3.Close()
	for ; iter3.Valid(); iter3.Next() {
		key := iter3.Key()
		val, _ := iter3.Value()
		println(string(key), string(val))
	}
}

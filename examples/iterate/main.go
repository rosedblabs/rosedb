package main

import (
	"fmt"
	"runtime"

	"github.com/rosedblabs/rosedb/v2"
)

// this file shows how to use the iterate operations of rosedb
func main() {
	// specify the options
	options := rosedb.DefaultOptions
	sysType := runtime.GOOS
	if sysType == "windows" {
		options.DirPath = "C:\\rosedb_iterate"
	} else {
		options.DirPath = "/tmp/rosedb_iterate"
	}

	// open a database
	db, err := rosedb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// prepare sample data
	_ = db.Put([]byte("key13"), []byte("value13"))
	_ = db.Put([]byte("key11"), []byte("value11"))
	_ = db.Put([]byte("key35"), []byte("value35"))
	_ = db.Put([]byte("key27"), []byte("value27"))
	_ = db.Put([]byte("key41"), []byte("value41"))

	dbIteratorExample(db)
	customIteratorExample(db)
}

// dbIteratorExample demonstrates the built-in database iterator methods
func dbIteratorExample(db *rosedb.DB) {
	// iterate all keys in order
	db.AscendKeys(nil, true, func(k []byte) (bool, error) {
		fmt.Println("key = ", string(k))
		return true, nil
	})

	// iterate all keys and values in order
	db.Ascend(func(k, v []byte) (bool, error) {
		fmt.Printf("key = %s, value = %s\n", string(k), string(v))
		return true, nil
	})

	// iterate all keys in reverse order
	db.DescendKeys(nil, true, func(k []byte) (bool, error) {
		fmt.Println("key = ", string(k))
		return true, nil
	})

	// iterate all keys and values in reverse order
	db.Descend(func(k, v []byte) (bool, error) {
		fmt.Printf("key = %s, value = %s\n", string(k), string(v))
		return true, nil
	})
	// you can also use some other similar methods to iterate the data.
	// db.AscendRange()
	// db.AscendGreaterOrEqual()
	// db.DescendRange()
	// db.DescendLessOrEqual()
}

// customIteratorExample demonstrates how to use the low-level iterator API
func customIteratorExample(db *rosedb.DB) {
	// 1: Using iterator with ContinueOnError = true
	iterOpts := rosedb.DefaultIteratorOptions
	iterOpts.ContinueOnError = true
	iter1 := db.NewIterator(iterOpts)
	defer iter1.Close()

	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		item := iter1.Item()
		if item != nil {
			fmt.Printf("key = %s, value = %s\n", string(item.Key), string(item.Value))
		}
	}
	if err := iter1.Err(); err != nil {
		fmt.Printf("Iterator encountered errors but continued: %v\n", err)
	}

	// 2: Using iterator with ContinueOnError = false
	iterOpts.ContinueOnError = false
	iter2 := db.NewIterator(iterOpts)
	defer iter2.Close()

	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		item := iter2.Item()
		if item != nil {
			fmt.Printf("key = %s, value = %s\n", string(item.Key), string(item.Value))
		}
	}
	if err := iter2.Err(); err != nil {
		fmt.Printf("Iterator stopped due to error: %v\n", err)
	}
}

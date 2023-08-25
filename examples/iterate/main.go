package main

import (
	"fmt"

	"github.com/rosedblabs/rosedb/v2"
)

// this file shows how to use the iterate operations of rosedb
// you can use Ascend, Descend(and some other similar methods) to iterate all keys and values in order.
func main() {
	// specify the options
	options := rosedb.DefaultOptions
	options.DirPath = "/tmp/rosedb_iterate"

	// open a database
	db, err := rosedb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	_ = db.Put([]byte("key13"), []byte("value13"))
	_ = db.Put([]byte("key11"), []byte("value11"))
	_ = db.Put([]byte("key35"), []byte("value35"))
	_ = db.Put([]byte("key27"), []byte("value27"))
	_ = db.Put([]byte("key41"), []byte("value41"))

	// iterate all keys in order
	db.AscendKeys(nil, func(k []byte) (bool, error) {
		fmt.Println("key = ", string(k))
		return true, nil
	})

	// iterate all keys and values in order
	db.Ascend(func(k []byte, v []byte) (bool, error) {
		fmt.Printf("key = %s, value = %s\n", string(k), string(v))
		return true, nil
	})

	// iterate all keys in reverse order
	db.DescendKeys(nil, func(k []byte) (bool, error) {
		fmt.Println("key = ", string(k))
		return true, nil
	})

	// iterate all keys and values in reverse order
	db.Descend(func(k []byte, v []byte) (bool, error) {
		fmt.Printf("key = %s, value = %s\n", string(k), string(v))
		return true, nil
	})

	// you can also use some other similar methods to iterate the data.
	// db.AscendRange()
	// db.AscendGreaterOrEqual()
	// db.DescendRange()
	// db.DescendLessOrEqual()
}

package main

import (
	"encoding/gob"
	"os"
	"runtime"

	"github.com/drewlanenga/govector"
	"github.com/rosedblabs/rosedb/v2"

	"bytes"
	"fmt"
)

func EncodeVector(v govector.Vector) []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(v)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	return buffer.Bytes()
}

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
	var vectorArr = []govector.Vector{{8, -7, -10, -8, 3, -6, 6, -2, 5, 1},
		{-2, -2, -6, -10, 10, -3, 1, 3, -9, -10},
		{-4, 7, -6, -1, 3, -5, 5, -2, -10, -3},
		{1, 0, -7, 1, 3, -3, 1, 0, -2, 7},
		{-3, -7, -6, -3, 5, 3, 1, 1, -6, 6},
		{9, 0, 8, -3, -4, 1, -3, -9, -10, 4},
		{8, -5, -7, 4, -10, 0, -7, 4, 10, 0},
		{-2, -10, -7, -1, -10, -4, 1, 2, -3, 3},
		{-1, -7, 6, 2, -2, -2, -2, -1, -2, -10},
		{9, -2, -1, -1, -6, 9, 2, 3, -7, 5},
	}

	encoded := EncodeVector(vectorArr[0])

	err = db.Put(encoded, []byte("rose"))
	if err != nil {
		panic(err)
	}

	// update a key that is a vector
	err = db.Put(encoded, []byte("db"))
	if err != nil {
		panic(err)
	}

	// get a key that is a vector
	val, err := db.Get(encoded)
	if err != nil {
		panic(err)
	}
	println(string(val))

}

package main

import (
	"fmt"
	"github.com/flower-corp/rosedb"
	"path/filepath"
	"time"
)

func main() {
	path := filepath.Join("/tmp", "rosedb")
	opts := rosedb.DefaultOptions(path)
	db, err := rosedb.Open(opts)
	if err != nil {
		fmt.Printf("open rosedb err: %v", err)
		return
	}

	err = db.Set([]byte("name"), []byte("RoseDB"))
	if err != nil {
		fmt.Printf("write data err: %v", err)
		return
	}

	v, err := db.Get([]byte("name"))
	if err != nil {
		fmt.Printf("read data err: %v", err)
		return
	}
	fmt.Println("val = ", string(v))

	err = db.SetEX([]byte("type"), []byte("RoseDB-Strs"), time.Second*5)
	if err != nil {
		fmt.Printf("write data err: %v", err)
		return
	}

	err = db.Delete([]byte("name"))
	if err != nil {
		fmt.Printf("delete data err: %v", err)
		return
	}

	err = db.SetNX([]byte("cmd"), []byte("SetNX"))
	if err != nil {
		fmt.Printf("write data err: %v", err)
		return
	}

	v, err = db.Get([]byte("cmd"))
	if err != nil {
		fmt.Printf("read data err: %v", err)
		return
	}
	fmt.Printf("cmd-type = %s\n", string(v))

	err = db.MSet([]byte("key-1"), []byte("value-1"), []byte("key-2"), []byte("value-2"))
	if err != nil {
		fmt.Printf("mset error: %v\n", err)
		return
	}
	fmt.Println("Multiple key-value pair added.")

	// Missing value.
	err = db.MSet([]byte("key-1"), []byte("value-1"), []byte("key-2"))
	if err != nil {
		fmt.Printf("mset error: %v\n", err)
	}

	err = db.MSetNX([]byte("key-11"), []byte("value-11"), []byte("key-22"), []byte("value-22"))
	if err != nil {
		fmt.Printf("msetnx error: %v\n", err)
	}
	val, _ := db.Get([]byte("key-11"))
	fmt.Printf("key-11: %v\n", string(val))
	fmt.Printf("A example of missing value: %v\n", err)

	// mget
	keys := [][]byte{
		[]byte("key-1"),
		[]byte("not exist"),
		[]byte("key-11"),
	}
	vals, err := db.MGet(keys)
	if err != nil {
		fmt.Printf("mget err : %v\n", err)
	} else {
		fmt.Printf("mget values : %v\n", vals)
	}

	// example of append
	err = db.Delete([]byte("append"))
	if err != nil {
		fmt.Printf("delete data err: %v", err)
		return
	}

	_, err = db.GetDel([]byte("not-exist"))
	if err != nil {
		fmt.Printf("getdel data err: %v", err)
	}
	gdVal, err := db.GetDel([]byte("key-22"))
	if err != nil {
		fmt.Printf("getdel data err: %v", err)
	} else {
		fmt.Println("getdel val : ", string(gdVal))
	}

	err = db.Append([]byte("append"), []byte("Rose"))
	if err != nil {
		fmt.Printf("write data err: %v", err)
		return
	}

	err = db.Append([]byte("append"), []byte("DB"))
	if err != nil {
		fmt.Printf("write data err: %v", err)
		return
	}

	v, err = db.Get([]byte("append"))
	if err != nil {
		fmt.Printf("read data err: %v", err)
		return
	}
	fmt.Printf("append = %s\n", string(v))

	strLen := db.StrLen([]byte("key-1"))
	fmt.Printf("StrLen %v\n", strLen)

	_ = db.Set([]byte("int"), []byte("12"))
	valInt, err := db.Decr([]byte("int"))
	if err != nil {
		fmt.Printf(err.Error())
	}
	fmt.Printf("new value after Decr(): %v\n", valInt)

	valInt, err = db.DecrBy([]byte("int"), 5)
	if err != nil {
		fmt.Printf(err.Error())
	}
	fmt.Printf("new value after DecrBy(5): %v\n", valInt)
}

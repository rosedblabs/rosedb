package cmd

import (
	"fmt"
	"rosedb"
)

func set(db *rosedb.RoseDB, args []string) (res string) {
	key, value := args[0], args[1]
	err := db.Set([]byte(key), []byte(value))
	if err != nil {
		res = fmt.Sprintf("%+v", err)
	} else {
		res = "OK"
	}
	return
}

func get(db *rosedb.RoseDB, args []string) (res string) {
	key := args[0]
	val, err := db.Get([]byte(key))
	if err != nil {
		res = fmt.Sprintf("%+v", err)
	} else {
		res = string(val)
	}
	return
}

// todo

func init() {
	register("set", set)
	register("get", get)
}

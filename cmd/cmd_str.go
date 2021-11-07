package cmd

import (
	"errors"
	"fmt"
	"github.com/roseduan/rosedb"
	"github.com/tidwall/redcon"
	"strconv"
)

// ErrSyntaxIncorrect incorrect err
var ErrSyntaxIncorrect = errors.New("syntax err")
var okResult = redcon.SimpleString("OK")

func newWrongNumOfArgsError(cmd string) error {
	return fmt.Errorf("wrong number of arguments for '%s' command", cmd)
}

func set(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("set")
		return
	}

	key, value := args[0], args[1]
	if err = db.Set([]byte(key), []byte(value)); err == nil {
		res = okResult
	}
	return
}

func get(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("get")
		return
	}

	key := args[0]
	var val string
	err = db.Get([]byte(key), &val)
	res = val

	if err == rosedb.ErrKeyNotExist {
		err = nil
		res = "(nil)"
	}

	return
}

func setNx(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("setnx")
		return
	}

	key, value := args[0], args[1]
	result, err := db.SetNx([]byte(key), []byte(value))

	if err == nil {
		res = result
	}
	return
}

func setEx(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("setex")
		return
	}

	var dur int64
	dur, err = strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}

	key, value := args[0], args[2]
	if err = db.SetEx([]byte(key), []byte(value), dur); err == nil {
		res = "OK"
	}

	return
}

func getSet(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("getset")
		return
	}

	var val string
	key, value := args[0], args[1]
	err = db.GetSet([]byte(key), []byte(value), &val)
	res = val
	return
}

func mSet(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args)%2 != 0 {
		err = newWrongNumOfArgsError("mset")
		return
	}

	var values []interface{}
	for i := 0; i < len(args); i++ {
		values = append(values, []byte(args[i]))
	}

	if err = db.MSet(values...); err == nil {
		res = "OK"
	}
	return
}

func mGet(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) <= 0 {
		err = newWrongNumOfArgsError("mget")
		return
	}

	var values []interface{}
	for i := 0; i < len(args); i++ {
		values = append(values, []byte(args[i]))
	}

	res, err = db.MGet(values...)

	return
}

func appendStr(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("append")
		return
	}

	key, value := args[0], args[1]
	if err = db.Append([]byte(key), value); err == nil {
		res = okResult
	}
	return
}

func strExists(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("strexists")
		return
	}
	if exists := db.StrExists([]byte(args[0])); exists {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}
	return
}

func remove(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("remove")
		return
	}
	if err = db.Remove([]byte(args[0])); err == nil {
		res = okResult
	}
	return
}

func expire(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = ErrSyntaxIncorrect
		return
	}
	seconds, err := strconv.Atoi(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	if err = db.Expire([]byte(args[0]), int64(seconds)); err == nil {
		res = okResult
	}
	return
}

func persist(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("persist")
		return
	}
	db.Persist([]byte(args[0]))
	res = okResult
	return
}

func ttl(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("ttl")
	}
	ttlVal := db.TTL([]byte(args[0]))
	res = redcon.SimpleInt(ttlVal)
	return
}

func init() {
	addExecCommand("set", set)
	addExecCommand("get", get)
	addExecCommand("setnx", setNx)
	addExecCommand("setex", setEx)
	addExecCommand("getset", getSet)
	addExecCommand("mset", mSet)
	addExecCommand("mget", mGet)
	addExecCommand("append", appendStr)
	addExecCommand("strexists", strExists)
	addExecCommand("remove", remove)
	addExecCommand("expire", expire)
	addExecCommand("persist", persist)
	addExecCommand("ttl", ttl)
}

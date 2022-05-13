package main

import (
	"errors"
	"fmt"
	"github.com/flower-corp/rosedb"
	"github.com/tidwall/redcon"
	"strconv"
	"strings"
	"time"
)

const (
	resultOK = "OK"
)

var (
	errSyntax         = errors.New("ERR syntax error ")
	errValueIsInvalid = errors.New("ERR value is not an integer or out of range")
)

func newWrongNumOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------- Server managment commands ---------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func info(db *rosedb.RoseDB, args [][]byte) (interface{}, error) {
	// todo
	return "info", nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |-------------------------- generic commands --------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func del(db *rosedb.RoseDB, args [][]byte) (interface{}, error) {
	if len(args) < 1 {
		return nil, newWrongNumOfArgsError("del")
	}
	for _, key := range args {
		if err := db.Delete(key); err != nil {
			return 0, err
		}
		// delete other ds. todo
	}
	return redcon.SimpleInt(1), nil
}

func keyType(db *rosedb.RoseDB, args [][]byte) (interface{}, error) {
	// todo
	return "string", nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |-------------------------- String commands --------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func set(db *rosedb.RoseDB, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("set")
	}
	key, value := args[0], args[1]

	var setErr error
	if len(args) > 2 {
		ex := strings.ToLower(string(args[2]))
		if ex != "ex" || len(args) != 4 {
			return nil, errSyntax
		}
		second, err := strconv.Atoi(string(args[3]))
		if err != nil {
			return nil, errSyntax
		}
		setErr = db.SetEX(key, value, time.Second*time.Duration(second))
	} else {
		setErr = db.Set(key, value)
	}
	if setErr != nil {
		return nil, setErr
	}
	return redcon.SimpleString(resultOK), nil
}

func setex(db *rosedb.RoseDB, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumOfArgsError("get")
	}
	key, seconds, value := args[0], args[1], args[2]
	sec, err := strconv.Atoi(string(seconds))
	if err != nil {
		return nil, errSyntax
	}
	err = db.SetEX(key, value, time.Second*time.Duration(sec))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(resultOK), nil
}

func get(db *rosedb.RoseDB, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("get")
	}
	value, err := db.Get(args[0])
	if err != nil {
		return nil, err
	}
	return value, nil
}

func mget(db *rosedb.RoseDB, args [][]byte) (interface{}, error) {
	if len(args) < 1 {
		return nil, newWrongNumOfArgsError("mget")
	}
	var values [][]byte
	for _, key := range args {
		val, err := db.Get(key)
		if err != nil && err != rosedb.ErrKeyNotFound {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

func appendStr(db *rosedb.RoseDB, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("append")
	}
	key, value := args[0], args[1]
	err := db.Append(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(db.StrLen(key)), nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------------- List commands ---------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func lpush(db *rosedb.RoseDB, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("lpush")
	}
	key, value := args[0], args[1:]
	err := db.LPush(key, value...)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(db.LLen(key)), nil
}

func rpush(db *rosedb.RoseDB, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("rpush")
	}
	key, value := args[0], args[1:]
	err := db.RPush(key, value...)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(db.LLen(key)), nil
}

func lpop(db *rosedb.RoseDB, args [][]byte) (interface{}, error) {
	return popInternal(db, args, true)
}

func rpop(db *rosedb.RoseDB, args [][]byte) (interface{}, error) {
	return popInternal(db, args, false)
}

func popInternal(db *rosedb.RoseDB, args [][]byte, isLeft bool) (interface{}, error) {
	if len(args) < 1 {
		return nil, newWrongNumOfArgsError("lpop")
	}
	key := args[0]
	var count = 1
	if len(args) == 2 {
		c, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return nil, errValueIsInvalid
		}
		count = c
	}
	total := db.LLen(key)
	var values [][]byte
	for i := 0; i < count && i < total; i++ {
		var (
			val []byte
			err error
		)
		if isLeft {
			val, err = db.LPop(key)
		} else {
			val, err = db.RPop(key)
		}
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

func llen(db *rosedb.RoseDB, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("llen")
	}
	key := args[0]
	return redcon.SimpleInt(db.LLen(key)), nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |--------------------------- Hash commands ----------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------------- Set commands ----------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+

// +-------+--------+----------+------------+-----------+-------+---------+
// |------------------------- Sorted Set commands ------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+

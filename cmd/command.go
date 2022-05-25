package main

import (
	"errors"
	"fmt"
	"github.com/flower-corp/rosedb"
	"github.com/tidwall/redcon"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	resultOK   = "OK"
	resultPong = "PONG"
)

var (
	errSyntax            = errors.New("ERR syntax error ")
	errValueIsInvalid    = errors.New("ERR value is not an integer or out of range")
	errDBIndexOutOfRange = errors.New("ERR DB index is out of range")
)

func newWrongNumOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------- server management commands --------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func info(cli *Client, args [][]byte) (interface{}, error) {
	// todo
	return "info", nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |-------------------- connection management commands ------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func selectDB(cli *Client, args [][]byte) (interface{}, error) {
	cli.svr.mu.Lock()
	defer cli.svr.mu.Unlock()

	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("select")
	}
	n, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return nil, errValueIsInvalid
	}

	if n < 0 || uint(n) >= cli.svr.opts.databases {
		return nil, errDBIndexOutOfRange
	}

	db := cli.svr.dbs[n]
	if db == nil {
		path := filepath.Join(cli.svr.opts.dbPath, fmt.Sprintf(dbName, n))
		opts := rosedb.DefaultOptions(path)
		newdb, err := rosedb.Open(opts)
		if err != nil {
			return nil, err
		}
		db = newdb
		cli.svr.dbs[n] = db
	}
	cli.db = db
	return resultOK, nil
}

func ping(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) > 1 {
		return nil, newWrongNumOfArgsError("ping")
	}
	var res = resultPong
	if len(args) == 1 {
		res = string(args[0])
	}
	return res, nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |-------------------------- generic commands --------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func del(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) < 1 {
		return nil, newWrongNumOfArgsError("del")
	}
	for _, key := range args {
		if err := cli.db.Delete(key); err != nil {
			return 0, err
		}
		// delete other ds. todo
	}
	return redcon.SimpleInt(1), nil
}

func keyType(cli *Client, args [][]byte) (interface{}, error) {
	// todo
	return "string", nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |-------------------------- String commands --------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func set(cli *Client, args [][]byte) (interface{}, error) {
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
		setErr = cli.db.SetEX(key, value, time.Second*time.Duration(second))
	} else {
		setErr = cli.db.Set(key, value)
	}
	if setErr != nil {
		return nil, setErr
	}
	return redcon.SimpleString(resultOK), nil
}

func setex(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumOfArgsError("get")
	}
	key, seconds, value := args[0], args[1], args[2]
	sec, err := strconv.Atoi(string(seconds))
	if err != nil {
		return nil, errSyntax
	}
	err = cli.db.SetEX(key, value, time.Second*time.Duration(sec))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(resultOK), nil
}

func get(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("get")
	}
	value, err := cli.db.Get(args[0])
	if err != nil {
		return nil, err
	}
	return value, nil
}

func mget(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) < 1 {
		return nil, newWrongNumOfArgsError("mget")
	}
	var keys [][]byte
	for _, key := range args {
		keys = append(keys, key)
	}
	values, err := cli.db.MGet(keys)
	return values, err
}

func appendStr(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("append")
	}
	key, value := args[0], args[1]
	err := cli.db.Append(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(cli.db.StrLen(key)), nil
}

func getDel(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("getdel")
	}
	val, err := cli.db.GetDel(args[0])
	return val, err
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------------- List commands ---------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func lpush(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("lpush")
	}
	key, value := args[0], args[1:]
	err := cli.db.LPush(key, value...)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(cli.db.LLen(key)), nil
}

func rpush(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("rpush")
	}
	key, value := args[0], args[1:]
	err := cli.db.RPush(key, value...)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(cli.db.LLen(key)), nil
}

func lpop(cli *Client, args [][]byte) (interface{}, error) {
	return popInternal(cli.db, args, true)
}

func rpop(cli *Client, args [][]byte) (interface{}, error) {
	return popInternal(cli.db, args, false)
}

func lMove(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 4 {
		return nil, newWrongNumOfArgsError("lmove")
	}

	srcKey, dstKey := args[0], args[1]
	from, to := strings.ToLower(string(args[2])), strings.ToLower(string(args[3]))
	var srcIsLeft, dstIsLeft bool

	if from == "left" {
		srcIsLeft = true
	} else if from == "right" {
		srcIsLeft = false
	} else {
		return nil, errSyntax
	}

	if to == "left" {
		dstIsLeft = true
	} else if to == "right" {
		dstIsLeft = false
	} else {
		return nil, errSyntax
	}

	return cli.db.LMove(srcKey, dstKey, srcIsLeft, dstIsLeft)
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

func llen(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("llen")
	}
	key := args[0]
	return redcon.SimpleInt(cli.db.LLen(key)), nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |--------------------------- Hash commands ----------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func hset(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) < 2 || len(args)%2 == 0 {
		return nil, newWrongNumOfArgsError("hset")
	}
	key := args[0]
	var count int
	for i := 1; i < len(args); i += 2 {
		err := cli.db.HSet(key, args[i], args[i+1])
		if err != nil {
			return nil, err
		}
		count++
	}
	return redcon.SimpleInt(count), nil
}

func hget(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("hget")
	}
	val, err := cli.db.HGet(args[0], args[1])
	return val, err
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------------- Set commands ----------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func sadd(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("sadd")
	}
	key := args[0]
	var count int
	for _, val := range args[1:] {
		isMember := cli.db.SIsMember(key, val)
		if !isMember {
			err := cli.db.SAdd(key, val)
			if err != nil {
				return nil, err
			}
			count++
		}
	}
	return redcon.SimpleInt(count), nil
}

func srem(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("srem")
	}
	key := args[0]
	var count int
	for _, val := range args[1:] {
		isMember := cli.db.SIsMember(key, val)
		if isMember {
			err := cli.db.SRem(key, val)
			if err != nil {
				return nil, err
			}
			count++
		}
	}
	return redcon.SimpleInt(count), nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |------------------------- Sorted Set commands ------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+

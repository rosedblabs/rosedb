package main

import (
	"errors"
	"fmt"
	"github.com/flower-corp/rosedb"
	"github.com/flower-corp/rosedb/util"
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

func setEX(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumOfArgsError("get")
	}
	key, seconds, value := args[0], args[1], args[2]
	sec, err := strconv.Atoi(string(seconds))
	if err != nil {
		return nil, errValueIsInvalid
	}
	err = cli.db.SetEX(key, value, time.Second*time.Duration(sec))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(resultOK), nil
}

func setNX(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("setnx")
	}
	key, value := args[0], args[1]
	err := cli.db.SetNX(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(resultOK), nil
}

func mSet(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return nil, newWrongNumOfArgsError("mset")
	}
	err := cli.db.MSet(args...)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(resultOK), nil
}

func mSetNX(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return nil, newWrongNumOfArgsError("msetnx")
	}
	err := cli.db.MSetNX(args...)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(resultOK), nil
}

func decr(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("decr")
	}
	key := args[0]
	return cli.db.Decr(key)
}

func decrBy(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("decrby")
	}
	key, decrVal := args[0], args[1]
	decrInt64Val, err := util.StrToInt64(string(decrVal))
	if err != nil {
		return nil, errValueIsInvalid
	}
	return cli.db.DecrBy(key, decrInt64Val)
}

func incr(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("incr")
	}
	key := args[0]
	return cli.db.Incr(key)
}

func incrBy(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("incrby")
	}
	key, incrVal := args[0], args[1]
	incrInt64Val, err := util.StrToInt64(string(incrVal))
	if err != nil {
		return nil, errValueIsInvalid
	}
	return cli.db.IncrBy(key, incrInt64Val)
}

func strLen(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("strlen")
	}
	return cli.db.StrLen(args[0]), nil
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

func mGet(cli *Client, args [][]byte) (interface{}, error) {
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
func lPush(cli *Client, args [][]byte) (interface{}, error) {
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

func rPush(cli *Client, args [][]byte) (interface{}, error) {
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

func lPop(cli *Client, args [][]byte) (interface{}, error) {
	return popInternal(cli.db, args, true)
}

func rPop(cli *Client, args [][]byte) (interface{}, error) {
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

func lLen(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("llen")
	}
	key := args[0]
	return redcon.SimpleInt(cli.db.LLen(key)), nil
}

func lIndex(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("lindex")
	}
	key, index := args[0], args[1]
	intIndex, err := strconv.Atoi(string(index))
	if err != nil {
		return nil, errValueIsInvalid
	}
	return cli.db.LIndex(key, intIndex)
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |--------------------------- Hash commands ----------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func hSet(cli *Client, args [][]byte) (interface{}, error) {
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

func hSetNX(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumOfArgsError("hsetnx")
	}

	key, field, value := args[0], args[1], args[2]
	ok, err := cli.db.HSetNX(key, field, value)
	if err != nil {
		return nil, err
	}
	if ok {
		return redcon.SimpleInt(1), nil
	}
	return redcon.SimpleInt(0), nil
}

func hGet(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("hget")
	}
	val, err := cli.db.HGet(args[0], args[1])
	return val, err
}

func hmGet(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("hmget")
	}
	return cli.db.HMGet(args[0], args[1:]...)
}

func hDel(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("hdel")
	}
	count, err := cli.db.HDel(args[0], args[1:]...)
	return redcon.SimpleInt(count), err
}

func hExists(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("hexists")
	}
	ok, err := cli.db.HExists(args[0], args[1])
	if err != nil {
		return nil, err
	}
	if ok {
		return redcon.SimpleInt(1), nil
	}
	return redcon.SimpleInt(0), nil
}

func hLen(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("hlen")
	}
	return redcon.SimpleInt(cli.db.HLen(args[0])), nil
}

func hKeys(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("hkeys")
	}
	return cli.db.HKeys(args[0])
}

func hVals(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("hvals")
	}
	return cli.db.HVals(args[0])
}

func hGetAll(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("hgetall")
	}
	return cli.db.HGetAll(args[0])
}

func hStrLen(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("hstrlen")
	}
	return redcon.SimpleInt(cli.db.HStrLen(args[0], args[1])), nil
}

func hScan(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 4 {
		return nil, newWrongNumOfArgsError("hscan")
	}
	pattern := string(args[2])
	count, err := strconv.Atoi(string(args[3]))
	if err != nil {
		return nil, err
	}
	return cli.db.HScan(args[0], args[1], pattern, count)
}

func hIncrBy(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumOfArgsError("hincrby")
	}
	key, field, incrVal := args[0], args[1], args[2]
	incrInt64Val, err := util.StrToInt64(string(incrVal))
	if err != nil {
		return nil, errValueIsInvalid
	}
	return cli.db.HIncrby(key, field, incrInt64Val)
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------------- Set commands ----------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func sAdd(cli *Client, args [][]byte) (interface{}, error) {
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

func sRem(cli *Client, args [][]byte) (interface{}, error) {
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

func sPop(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("spop")
	}
	count, err := util.StrToUint(string(args[1]))
	if err != nil {
		return nil, errValueIsInvalid
	}
	return cli.db.SPop(args[0], uint(count))
}

func sIsMember(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("sismember")
	}
	res := make([]redcon.SimpleInt, len(args[1:]))
	key := args[0]
	for _, mem := range args[1:] {
		if ok := cli.db.SIsMember(key, mem); ok {
			res = append(res, redcon.SimpleInt(1))
		} else {
			res = append(res, redcon.SimpleInt(0))
		}
	}
	return res, nil
}

func sMembers(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("smembers")
	}
	return cli.db.SMembers(args[0])
}

func sCard(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("scard")
	}
	return redcon.SimpleInt(cli.db.SCard(args[0])), nil
}

func sDiff(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) == 0 {
		return nil, newWrongNumOfArgsError("sdiff")
	}
	return cli.db.SDiff(args...)
}

func sUnion(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) == 0 {
		return nil, newWrongNumOfArgsError("sunion")
	}
	return cli.db.SUnion(args...)
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |------------------------- Sorted Set commands ------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+

func zAdd(cli *Client, args [][]byte) (interface{}, error) {
	if (len(args)-1)%2 != 0 {
		return nil, newWrongNumOfArgsError("zadd")
	}
	key := args[0]
	for i := 1; i < len(args); i += 2 {
		score, err := util.StrToFloat64(string(args[i]))
		if err != nil {
			return nil, errValueIsInvalid
		}
		err = cli.db.ZAdd(key, score, args[i+1])
		if err != nil {
			return nil, err
		}
	}
	return redcon.SimpleInt(len(args[1:]) / 2), nil
}

func zScore(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("zscore")
	}
	_, score := cli.db.ZScore(args[0], args[1])
	return score, nil
}

func zRem(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("zrem")
	}
	key := args[0]
	for _, member := range args[1:] {
		err := cli.db.ZRem(key, member)
		if err != nil {
			return nil, err
		}
	}
	return redcon.SimpleInt(len(args[1:]) / 2), nil
}

func zCard(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("zcard")
	}
	return redcon.SimpleInt(cli.db.ZCard(args[0])), nil
}

func zRange(cli *Client, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumOfArgsError("zrange")
	}
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, err
	}
	stop, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return nil, err
	}
	return cli.db.ZRange(args[0], start, stop)
}

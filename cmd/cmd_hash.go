package cmd

import (
	"github.com/roseduan/rosedb"
	"github.com/tidwall/redcon"
	"strconv"
)

func hSet(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("hset")
		return
	}
	var count int
	if count, err = db.HSet([]byte(args[0]), []byte(args[1]), []byte(args[2])); err == nil {
		res = redcon.SimpleInt(count)
	}
	return
}

func hSetNx(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("hsetnx")
		return
	}
	var ok int
	if ok, err = db.HSetNx([]byte(args[0]), []byte(args[1]), []byte(args[2])); err == nil {
		if ok == 1 {
			res = redcon.SimpleInt(1)
		} else {
			res = redcon.SimpleInt(0)
		}
	}
	return
}

func hGet(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = ErrSyntaxIncorrect
		return
	}
	val := db.HGet([]byte(args[0]), []byte(args[1]))
	if len(val) == 0 {
		res = nil
	} else {
		res = string(val)
	}
	return
}

func hGetAll(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("hgetall")
		return
	}
	res = db.HGetAll([]byte(args[0]))
	return
}

func hMSet(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args)%2 != 1 {
		err = newWrongNumOfArgsError("hmset")
		return
	}

	var values [][]byte
	for i := 1; i < len(args); i++ {
		values = append(values, []byte(args[i]))
	}

	res = db.HMSet([]byte(args[0]), values...)
	return
}

func hMGet(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) <= 1 {
		err = newWrongNumOfArgsError("hmget")
		return
	}

	var values [][]byte
	for i := 1; i < len(args); i++ {
		values = append(values, []byte(args[i]))
	}

	res = db.HMGet([]byte(args[0]), values...)
	return
}

func hDel(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) <= 1 {
		err = newWrongNumOfArgsError("hdel")
		return
	}

	var fields [][]byte
	for _, f := range args[1:] {
		fields = append(fields, []byte(f))
	}
	var count int
	if count, err = db.HDel([]byte(args[0]), fields...); err == nil {
		res = redcon.SimpleInt(count)
	}
	return
}

func hKeyExists(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("hkeyexists")
		return
	}
	exists := db.HKeyExists([]byte(args[0]))
	if exists {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}
	return
}

func hExists(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("hexists")
		return
	}
	exists := db.HExists([]byte(args[0]), []byte(args[1]))
	if exists {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}
	return
}

func hLen(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("hlen")
		return
	}
	count := db.HLen([]byte(args[0]))
	res = redcon.SimpleInt(count)
	return
}

func hKeys(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = ErrSyntaxIncorrect
		return
	}
	res = db.HKeys([]byte(args[0]))
	return
}

func hVals(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("hvals")
		return
	}
	res = db.HVals([]byte(args[0]))
	return
}

func hClear(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("hclear")
		return
	}

	if err = db.HClear([]byte(args[0])); err == nil {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}

	return
}

func hExpire(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("hexpire")
		return
	}

	var dur int64
	dur, err = strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}

	if err = db.HExpire([]byte(args[0]), dur); err == nil {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}

	return
}

func hTTL(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("httl")
		return
	}

	var ttl int64
	ttl = db.HTTL([]byte(args[0]))

	res = redcon.SimpleInt(ttl)

	return
}

func init() {
	addExecCommand("hset", hSet)
	addExecCommand("hsetnx", hSetNx)
	addExecCommand("hget", hGet)
	addExecCommand("hgetall", hGetAll)
	addExecCommand("hmset", hMSet)
	addExecCommand("hmget", hMGet)
	addExecCommand("hdel", hDel)
	addExecCommand("hkeyexists", hKeyExists)
	addExecCommand("hexists", hExists)
	addExecCommand("hlen", hLen)
	addExecCommand("hkeys", hKeys)
	addExecCommand("hvals", hVals)
	addExecCommand("hclear", hClear)
	addExecCommand("hexpire", hExpire)
	addExecCommand("httl", hTTL)
}

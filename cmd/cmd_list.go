package cmd

import (
	"github.com/roseduan/rosedb"
	"github.com/roseduan/rosedb/ds/list"
	"github.com/tidwall/redcon"
	"strconv"
)

func lPush(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) < 2 {
		err = newWrongNumOfArgsError("lpush")
		return
	}

	var values [][]byte
	for i := 1; i < len(args); i++ {
		values = append(values, []byte(args[i]))
	}

	var val int
	if val, err = db.LPush([]byte(args[0]), values...); err == nil {
		res = redcon.SimpleInt(val)
	}
	return
}

func rPush(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) < 2 {
		err = newWrongNumOfArgsError("rpush")
		return
	}

	var values [][]byte
	for i := 1; i < len(args); i++ {
		values = append(values, []byte(args[i]))
	}

	var val int
	if val, err = db.RPush([]byte(args[0]), values...); err == nil {
		res = redcon.SimpleInt(val)
	}
	return
}

func lPop(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("lpop")
		return
	}

	var val []byte
	if val, err = db.LPop([]byte(args[0])); err == nil {
		res = string(val)
	}
	return
}

func rPop(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("rpop")
		return
	}

	var val []byte
	if val, err = db.RPop([]byte(args[0])); err == nil {
		res = string(val)
	}
	return
}

func lIndex(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 0 {
		err = newWrongNumOfArgsError("lindex")
		return
	}
	index, err := strconv.Atoi(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}

	val := db.LIndex([]byte(args[0]), index)
	res = string(val)
	return
}

func lRem(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("lrem")
		return
	}
	count, err := strconv.Atoi(args[2])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}

	var val int
	if val, err = db.LRem([]byte(args[0]), []byte(args[1]), count); err == nil {
		res = redcon.SimpleInt(val)
	}
	return
}

func lInsert(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 4 {
		err = newWrongNumOfArgsError("linsert")
		return
	}
	var flag int
	if args[1] == "BEFORE" {
		flag = 0
	}
	if args[1] == "AFTER" {
		flag = 1
	}
	var val int
	if val, err = db.LInsert(args[0], list.InsertOption(flag), []byte(args[2]), []byte(args[3])); err == nil {
		res = redcon.SimpleInt(val)
	}
	return
}

func lSet(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("lset")
		return
	}
	index, err := strconv.Atoi(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}

	var ok bool
	ok, err = db.LSet([]byte(args[0]), index, []byte(args[2]))
	if ok {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}
	return
}

func lTrim(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("ltrim")
		return
	}
	start, err := strconv.Atoi(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}

	if err = db.LTrim([]byte(args[0]), start, end); err == nil {
		res = okResult
	}
	return
}

func lRange(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("lrange")
		return
	}
	start, err := strconv.Atoi(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}

	res, err = db.LRange([]byte(args[0]), start, end)
	return
}

func lLen(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("llen")
		return
	}

	length := db.LLen([]byte(args[0]))
	res = redcon.SimpleInt(length)
	return
}

func LKeyExists(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("lkeyexists")
		return
	}

	if ok := db.LKeyExists([]byte(args[0])); ok {
		res = redcon.SimpleInt(1)

	} else {
		res = redcon.SimpleInt(0)
	}
	return
}

func LValExists(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("lvalexists")
		return
	}

	if ok := db.LValExists([]byte(args[0]), []byte(args[1])); ok {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}
	return
}

func init() {
	addExecCommand("lpush", lPush)
	addExecCommand("rpush", rPush)
	addExecCommand("lpop", lPop)
	addExecCommand("rpop", rPop)
	addExecCommand("lindex", lIndex)
	addExecCommand("lrem", lRem)
	addExecCommand("linsert", lInsert)
	addExecCommand("lset", lSet)
	addExecCommand("ltrim", lTrim)
	addExecCommand("lrange", lRange)
	addExecCommand("llen", lLen)
	addExecCommand("lkeyexists", LKeyExists)
	addExecCommand("lvalexists", LValExists)
}

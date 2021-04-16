package cmd

import (
	"rosedb"
	"rosedb/ds/list"
	"strconv"
)

func lPush(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) < 2 {
		err = ErrSyntaxIncorrect
		return
	}

	var values [][]byte
	for i := 1; i < len(args); i++ {
		values = append(values, []byte(args[i]))
	}

	var val int
	if val, err = db.LPush([]byte(args[0]), values...); err == nil {
		res = strconv.Itoa(val)
	}
	return
}

func rPush(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) < 2 {
		err = ErrSyntaxIncorrect
		return
	}

	var values [][]byte
	for i := 1; i < len(args); i++ {
		values = append(values, []byte(args[i]))
	}

	var val int
	if val, err = db.RPush([]byte(args[0]), values...); err == nil {
		res = strconv.Itoa(val)
	}
	return
}

func lPop(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 1 {
		err = ErrSyntaxIncorrect
		return
	}

	var val []byte
	if val, err = db.LPop([]byte(args[0])); err == nil {
		res = string(val)
	}
	return
}

func rPop(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 1 {
		err = ErrSyntaxIncorrect
		return
	}

	var val []byte
	if val, err = db.RPop([]byte(args[0])); err == nil {
		res = string(val)
	}
	return
}

func lIndex(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 0 {
		err = ErrSyntaxIncorrect
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

func lRem(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 3 {
		err = ErrSyntaxIncorrect
		return
	}
	count, err := strconv.Atoi(args[2])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}

	var val int
	if val, err = db.LRem([]byte(args[0]), []byte(args[1]), count); err == nil {
		res = strconv.Itoa(val)
	}
	return
}

func lInsert(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 4 {
		err = ErrSyntaxIncorrect
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
		res = strconv.Itoa(val)
	}
	return
}

func lSet(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 3 {
		err = ErrSyntaxIncorrect
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
		res = "1"
	} else {
		res = "0"
	}
	return
}

func lTrim(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 3 {
		err = ErrSyntaxIncorrect
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
		res = "OK"
	}
	return
}

func lRange(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 3 {
		err = ErrSyntaxIncorrect
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

	var val [][]byte
	if val, err = db.LRange([]byte(args[0]), start, end); err == nil {
		for i, v := range val {
			res += string(v)
			if i != len(val)-1 {
				res += "\n"
			}
		}
	}
	return
}

func lLen(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 1 {
		err = ErrSyntaxIncorrect
		return
	}

	length := db.LLen([]byte(args[0]))
	res = strconv.Itoa(length)
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
}

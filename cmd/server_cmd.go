package cmd

import (
	"errors"
	"rosedb"
	"strconv"
	"strings"
)

var SyntaxErr = errors.New("syntax err")

type ExecCmdFunc func(*rosedb.RoseDB, []string) (string, error)

var ExecCmd = make(map[string]ExecCmdFunc)

func addExecCommand(cmd string, cmdFunc ExecCmdFunc) {
	ExecCmd[strings.ToLower(cmd)] = cmdFunc
}

func set(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 2 {
		err = SyntaxErr
		return
	}

	key, value := args[0], args[1]
	if err = db.Set([]byte(key), []byte(value)); err == nil {
		res = "OK"
	}
	return
}

func get(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 1 {
		err = SyntaxErr
		return
	}
	key := args[0]
	var val []byte
	if val, err = db.Get([]byte(key)); err == nil {
		res = string(val)
	}
	return
}

func setNx(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 2 {
		err = SyntaxErr
		return
	}

	key, value := args[0], args[1]
	if err = db.SetNx([]byte(key), []byte(value)); err == nil {
		res = "OK"
	}
	return
}

func getSet(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 2 {
		err = SyntaxErr
		return
	}
	key, value := args[0], args[1]
	var val []byte
	if val, err = db.GetSet([]byte(key), []byte(value)); err == nil {
		res = string(val)
	}
	return
}

func appendStr(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 2 {
		err = SyntaxErr
		return
	}
	key, value := args[0], args[1]
	if err = db.Append([]byte(key), []byte(value)); err == nil {
		res = "OK"
	}
	return
}

func strLen(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 1 {
		err = SyntaxErr
		return
	}
	length := db.StrLen([]byte(args[0]))
	res = strconv.Itoa(length)
	return
}

func strExists(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 1 {
		err = SyntaxErr
		return
	}
	if exists := db.StrExists([]byte(args[0])); exists {
		res = "1"
	} else {
		res = "0"
	}
	return
}

func strRem(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 1 {
		err = SyntaxErr
		return
	}
	if err = db.StrRem([]byte(args[0])); err == nil {
		res = "OK"
	}
	return
}

func prefixScan(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 3 {
		err = SyntaxErr
		return
	}
	limit, err := strconv.Atoi(args[1])
	if err != nil {
		err = SyntaxErr
		return
	}
	offset, err := strconv.Atoi(args[2])
	if err != nil {
		err = SyntaxErr
		return
	}

	var val [][]byte
	if val, err = db.PrefixScan(args[0], limit, offset); err == nil {
		for i, v := range val {
			res += string(v)
			if i != len(val)-1 {
				res += "\n"
			}
		}
	}
	return
}

func rangeScan(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 2 {
		err = SyntaxErr
		return
	}

	var val [][]byte
	if val, err = db.RangeScan([]byte(args[0]), []byte(args[1])); err == nil {
		for i, v := range val {
			res += string(v)
			if i != len(val)-1 {
				res += "\n"
			}
		}
	}
	return
}

func expire(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 2 {
		err = SyntaxErr
		return
	}
	seconds, err := strconv.Atoi(args[1])
	if err != nil {
		err = SyntaxErr
		return
	}
	if err = db.Expire([]byte(args[0]), uint32(seconds)); err == nil {
		res = "OK"
	}
	return
}

func persist(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 1 {
		err = SyntaxErr
		return
	}
	db.Persist([]byte(args[0]))
	res = "OK"
	return
}

func ttl(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 1 {
		err = SyntaxErr
	}

	ttl := db.TTL([]byte(args[0]))
	res = strconv.FormatInt(int64(ttl), 10)
	return
}

func init() {
	addExecCommand("set", set)
	addExecCommand("get", get)
	addExecCommand("setnx", setNx)
	addExecCommand("getset", getSet)
	addExecCommand("append", appendStr)
	addExecCommand("strlen", strLen)
	addExecCommand("strexists", strExists)
	addExecCommand("strrem", strRem)
	addExecCommand("prefixscan", prefixScan)
	addExecCommand("rangescan", rangeScan)
	addExecCommand("expire", expire)
	addExecCommand("persist", persist)
	addExecCommand("ttl", ttl)
}

package cmd

import (
	"errors"
	"fmt"
	"rosedb"
	"strings"
)

var SyntaxErr = errors.New("syntax err")

type ExecCmdFunc func(*rosedb.RoseDB, []string) string

var ExecCmd = make(map[string]ExecCmdFunc)

func addExecCommand(cmd string, cmdFunc ExecCmdFunc) {
	ExecCmd[strings.ToLower(cmd)] = cmdFunc
}

func set(db *rosedb.RoseDB, args []string) (res string) {
	if len(args) != 2 {
		return SyntaxErr.Error()
	}
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
	if len(args) != 1 {
		return SyntaxErr.Error()
	}
	key := args[0]
	val, err := db.Get([]byte(key))
	if err != nil {
		res = fmt.Sprintf("%+v", err)
	} else {
		res = string(val)
	}
	return
}

// other commands todo

func init() {
	addExecCommand("set", set)
	addExecCommand("get", get)
}

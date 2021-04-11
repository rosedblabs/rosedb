package cmd

import (
	"rosedb"
	"strings"
)

type ExecCmdFunc func(*rosedb.RoseDB, []string) string

var ExecCmd = make(map[string]ExecCmdFunc)

func register(cmd string, cmdFunc ExecCmdFunc) {
	ExecCmd[strings.ToLower(cmd)] = cmdFunc
}

package cmd

import (
	"strconv"

	"github.com/roseduan/rosedb"
)

func scan(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	lenArgs := len(args)
	mactch := ""
	count := 10
	cursor := 0
	if lenArgs > 2 {
		count, _ = strconv.Atoi(args[2])
	}
	if lenArgs > 1 {
		mactch = args[1]
	}
	if lenArgs > 0 {
		cursor, _ = strconv.Atoi(args[0])
	}
	val := db.Keys(mactch, count, cursor)

	return val, nil
}

func init() {
	addExecCommand("scan", scan)
}

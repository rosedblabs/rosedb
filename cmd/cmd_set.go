package cmd

import (
	"strconv"

	"github.com/roseduan/rosedb"
	"github.com/tidwall/redcon"
)

func sAdd(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) <= 1 {
		err = newWrongNumOfArgsError("sadd")
		return
	}

	var members []interface{}
	for _, m := range args[1:] {
		members = append(members, m)
	}
	var count int
	if count, err = db.SAdd([]byte(args[0]), members...); err == nil {
		res = redcon.SimpleInt(count)
	}
	return
}

func sPop(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("spop")
		return
	}
	count, err := strconv.Atoi(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	res, err = db.SPop([]byte(args[0]), count)
	return
}

func sIsMember(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("sismember")
		return
	}
	if ok := db.SIsMember([]byte(args[0]), []byte(args[1])); ok {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}
	return
}

func sRandMember(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("srandmember")
		return
	}
	count, err := strconv.Atoi(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	res = db.SRandMember([]byte(args[0]), count)
	return
}

func sRem(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) <= 1 {
		err = ErrSyntaxIncorrect
		return
	}
	var members []interface{}
	for _, m := range args[1:] {
		members = append(members, m)
	}
	var count int
	if count, err = db.SRem([]byte(args[0]), members...); err == nil {
		res = redcon.SimpleInt(count)
	}
	return
}

func sMove(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("smove")
		return
	}
	if err = db.SMove([]byte(args[0]), []byte(args[1]), []byte(args[2])); err == nil {
		res = okResult
	}
	return
}

func sCard(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("scard")
		return
	}
	card := db.SCard([]byte(args[0]))
	res = redcon.SimpleInt(card)
	return
}

func sMembers(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("smembers")
		return
	}
	res = db.SMembers([]byte(args[0]))
	return
}

func sUnion(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) <= 0 {
		err = newWrongNumOfArgsError("sunion")
		return
	}
	var keys []interface{}
	for _, v := range args {
		keys = append(keys, v)
	}
	res = db.SUnion(keys...)
	return
}

func sDiff(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) <= 0 {
		err = newWrongNumOfArgsError("sdiff")
		return
	}
	var keys []interface{}
	for _, v := range args {
		keys = append(keys, v)
	}
	res = db.SDiff(keys...)
	return
}

func sKeyExists(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) <= 0 {
		err = newWrongNumOfArgsError("skeyexists")
		return
	}

	if ok := db.SKeyExists([]byte(args[0])); ok {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}

	return
}

func sClear(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("sclear")
		return
	}

	if err = db.SClear([]byte(args[0])); err == nil {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}

	return
}

func sExpire(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("sexpire")
		return
	}

	var dur int64
	dur, err = strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}

	if err = db.SExpire([]byte(args[0]), dur); err == nil {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}

	return
}

func sTTL(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("sttl")
		return
	}

	var ttl int64
	ttl = db.STTL([]byte(args[0]))

	res = redcon.SimpleInt(ttl)

	return
}

func init() {
	addExecCommand("sadd", sAdd)
	addExecCommand("spop", sPop)
	addExecCommand("sismember", sIsMember)
	addExecCommand("srandmember", sRandMember)
	addExecCommand("srem", sRem)
	addExecCommand("smove", sMove)
	addExecCommand("scard", sCard)
	addExecCommand("smembers", sMembers)
	addExecCommand("sunion", sUnion)
	addExecCommand("sdiff", sDiff)
	addExecCommand("skeyexists", sKeyExists)
	addExecCommand("sclear", sClear)
	addExecCommand("sexpire", sExpire)
	addExecCommand("sttl", sTTL)
}

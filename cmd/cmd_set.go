package cmd

import (
	"rosedb"
	"strconv"
)

func sAdd(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) <= 1 {
		err = ErrSyntaxIncorrect
		return
	}

	var members [][]byte
	for _, m := range args[1:] {
		members = append(members, []byte(m))
	}
	var count int
	if count, err = db.SAdd([]byte(args[0]), members...); err == nil {
		res = strconv.Itoa(count)
	}
	return
}

func sPop(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 2 {
		err = ErrSyntaxIncorrect
		return
	}
	count, err := strconv.Atoi(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	var val [][]byte
	if val, err = db.SPop([]byte(args[0]), count); err == nil {
		for i, v := range val {
			res += string(v)
			if i != len(val)-1 {
				res += "\n"
			}
		}
	}
	return
}

func sIsMember(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 2 {
		err = ErrSyntaxIncorrect
		return
	}
	if ok := db.SIsMember([]byte(args[0]), []byte(args[1])); ok {
		res = "1"
	} else {
		res = "0"
	}
	return
}

func sRandMember(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 2 {
		err = ErrSyntaxIncorrect
		return
	}
	count, err := strconv.Atoi(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	val := db.SRandMember([]byte(args[0]), count)
	for i, v := range val {
		res += string(v)
		if i != len(val)-1 {
			res += "\n"
		}
	}
	return
}

func sRem(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) <= 1 {
		err = ErrSyntaxIncorrect
		return
	}
	var members [][]byte
	for _, m := range args[1:] {
		members = append(members, []byte(m))
	}
	var count int
	if count, err = db.SRem([]byte(args[0]), members...); err == nil {
		res = strconv.Itoa(count)
	}
	return
}

func sMove(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 3 {
		err = ErrSyntaxIncorrect
		return
	}
	if err = db.SMove([]byte(args[0]), []byte(args[1]), []byte(args[2])); err == nil {
		res = "OK"
	}
	return
}

func sCard(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 1 {
		err = ErrSyntaxIncorrect
		return
	}
	card := db.SCard([]byte(args[0]))
	res = strconv.Itoa(card)
	return
}

func sMembers(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 1 {
		err = ErrSyntaxIncorrect
		return
	}
	members := db.SMembers([]byte(args[0]))
	for i, v := range members {
		res += string(v)
		if i != len(members)-1 {
			res += "\n"
		}
	}
	return
}

func sUnion(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) <= 0 {
		err = ErrSyntaxIncorrect
		return
	}
	var keys [][]byte
	for _, v := range args {
		keys = append(keys, []byte(v))
	}
	val := db.SUnion(keys...)
	for i, v := range val {
		res += string(v)
		if i != len(val)-1 {
			res += "\n"
		}
	}
	return
}

func sDiff(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) <= 0 {
		err = ErrSyntaxIncorrect
		return
	}
	var keys [][]byte
	for _, v := range args {
		keys = append(keys, []byte(v))
	}
	val := db.SDiff(keys...)
	for i, v := range val {
		res += string(v)
		if i != len(val)-1 {
			res += "\n"
		}
	}
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
}

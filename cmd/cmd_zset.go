package cmd

import (
	"fmt"
	"rosedb"
	"rosedb/utils"
	"strconv"
)

func zAdd(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 3 {
		err = SyntaxErr
		return
	}
	score, err := utils.StrToFloat64(args[1])
	if err != nil {
		err = SyntaxErr
		return
	}
	if err = db.ZAdd([]byte(args[0]), score, []byte(args[2])); err == nil {
		res = "OK"
	}
	return
}

func zScore(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 2 {
		err = SyntaxErr
		return
	}
	score := db.ZScore([]byte(args[0]), []byte(args[1]))
	res = utils.Float64ToStr(score)
	return
}

func zCard(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 1 {
		err = SyntaxErr
		return
	}
	card := db.ZCard([]byte(args[0]))
	res = strconv.Itoa(card)
	return
}

func zRank(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 2 {
		err = SyntaxErr
		return
	}
	rank := db.ZRank([]byte(args[0]), []byte(args[1]))
	res = strconv.FormatInt(rank, 10)
	return
}

func zRevRank(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 2 {
		err = SyntaxErr
		return
	}
	rank := db.ZRevRank([]byte(args[0]), []byte(args[1]))
	res = strconv.FormatInt(rank, 10)
	return
}

func zIncrBy(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 3 {
		err = SyntaxErr
		return
	}
	incr, err := utils.StrToFloat64(args[1])
	if err != nil {
		err = SyntaxErr
		return
	}
	var val float64
	if val, err = db.ZIncrBy([]byte(args[0]), incr, []byte(args[2])); err == nil {
		res = utils.Float64ToStr(val)
	}
	return
}

func zRange(db *rosedb.RoseDB, args []string) (res string, err error) {
	return zRawRange(db, args, false)
}

func zRevRange(db *rosedb.RoseDB, args []string) (res string, err error) {
	return zRawRange(db, args, true)
}

// for zRange and zRevRange
func zRawRange(db *rosedb.RoseDB, args []string, rev bool) (res string, err error) {
	if len(args) != 3 {
		err = SyntaxErr
		return
	}
	start, err := strconv.Atoi(args[1])
	if err != nil {
		err = SyntaxErr
		return
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		err = SyntaxErr
		return
	}

	var val []interface{}
	if rev {
		val = db.ZRevRange([]byte(args[0]), start, end)
	} else {
		val = db.ZRange([]byte(args[0]), start, end)
	}

	for i, v := range val {
		res += fmt.Sprintf("%v", v)
		if i != len(val)-1 {
			res += "\n"
		}
	}
	return
}

func zRem(db *rosedb.RoseDB, args []string) (res string, err error) {
	if len(args) != 2 {
		err = SyntaxErr
		return
	}
	var ok bool
	if ok, err = db.ZRem([]byte(args[0]), []byte(args[1])); err == nil {
		if ok {
			res = "1"
		} else {
			res = "0"
		}
	}
	return
}

func zGetByRank(db *rosedb.RoseDB, args []string) (res string, err error) {
	return zRawGetByRank(db, args, false)
}

func zRevGetByRank(db *rosedb.RoseDB, args []string) (res string, err error) {
	return zRawGetByRank(db, args, true)
}

// for zGetByRank and zRevGetByRank
func zRawGetByRank(db *rosedb.RoseDB, args []string, rev bool) (res string, err error) {
	if len(args) != 2 {
		err = SyntaxErr
		return
	}
	rank, err := strconv.Atoi(args[1])
	if err != nil {
		err = SyntaxErr
		return
	}

	var val []interface{}
	if rev {
		val = db.ZRevGetByRank([]byte(args[0]), rank)
	} else {
		val = db.ZGetByRank([]byte(args[0]), rank)
	}
	for i, v := range val {
		res += fmt.Sprintf("%v", v)
		if i != len(val)-1 {
			res += "\n"
		}
	}
	return
}

func zScoreRange(db *rosedb.RoseDB, args []string) (res string, err error) {
	return zRawScoreRange(db, args, false)
}

func zSRevScoreRange(db *rosedb.RoseDB, args []string) (res string, err error) {
	return zRawScoreRange(db, args, true)
}

// for zScoreRange and zSRevScoreRange
func zRawScoreRange(db *rosedb.RoseDB, args []string, rev bool) (res string, err error) {
	if len(args) != 3 {
		err = SyntaxErr
		return
	}
	param1, err := utils.StrToFloat64(args[1])
	if err != nil {
		err = SyntaxErr
		return
	}
	param2, err := utils.StrToFloat64(args[2])
	if err != nil {
		err = SyntaxErr
		return
	}
	var val []interface{}
	if rev {
		val = db.ZRevScoreRange([]byte(args[0]), param1, param2)
	} else {
		val = db.ZScoreRange([]byte(args[0]), param1, param2)
	}
	for i, v := range val {
		res += fmt.Sprintf("%v", v)
		if i != len(val)-1 {
			res += "\n"
		}
	}
	return
}

func init() {
	addExecCommand("zadd", zAdd)
	addExecCommand("zscore", zScore)
	addExecCommand("zcard", zCard)
	addExecCommand("zrank", zRank)
	addExecCommand("zrevrank", zRevRank)
	addExecCommand("zincrby", zIncrBy)
	addExecCommand("zrange", zRange)
	addExecCommand("zrevrange", zRevRange)
	addExecCommand("zrem", zRem)
	addExecCommand("zgetbyrank", zGetByRank)
	addExecCommand("zrevgetbyrank", zRevGetByRank)
	addExecCommand("zscorerange", zScoreRange)
	addExecCommand("zrevscorerange", zSRevScoreRange)
}

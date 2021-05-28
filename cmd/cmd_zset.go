package cmd

import (
	"fmt"
	"github.com/roseduan/rosedb"
	"github.com/roseduan/rosedb/utils"
	"github.com/tidwall/redcon"
	"strconv"
	"strings"
)

func zAdd(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("zadd")
		return
	}
	score, err := utils.StrToFloat64(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	if err = db.ZAdd([]byte(args[0]), score, []byte(args[2])); err == nil {
		res = okResult
	}
	return
}

func zScore(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("zscore")
		return
	}
	score := db.ZScore([]byte(args[0]), []byte(args[1]))
	res = utils.Float64ToStr(score)
	return
}

func zCard(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("zcard")
		return
	}
	card := db.ZCard([]byte(args[0]))
	res = redcon.SimpleInt(card)
	return
}

func zRank(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = ErrSyntaxIncorrect
		return
	}
	rank := db.ZRank([]byte(args[0]), []byte(args[1]))
	res = redcon.SimpleInt(rank)
	return
}

func zRevRank(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("zrevrank")
		return
	}
	rank := db.ZRevRank([]byte(args[0]), []byte(args[1]))
	res = redcon.SimpleInt(rank)
	return
}

func zIncrBy(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("zincrby")
		return
	}
	incr, err := utils.StrToFloat64(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	var val float64
	if val, err = db.ZIncrBy([]byte(args[0]), incr, []byte(args[2])); err == nil {
		res = utils.Float64ToStr(val)
	}
	return
}

func zRange(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 && len(args) != 4 {
		err = newWrongNumOfArgsError("zrange")
		return
	}
	return zRawRange(db, args, false)
}

func zRevRange(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 && len(args) != 4 {
		err = newWrongNumOfArgsError("zrevrange")
		return
	}
	return zRawRange(db, args, true)
}

// for zRange and zRevRange
func zRawRange(db *rosedb.RoseDB, args []string, rev bool) (res interface{}, err error) {
	withScores := false
	if len(args) == 4 {
		if strings.ToLower(args[3]) == "withscores" {
			withScores = true
			args = args[:3]
		} else {
			err = ErrSyntaxIncorrect
			return
		}
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

	var val []interface{}
	if rev {
		if withScores {
			val = db.ZRevRangeWithScores([]byte(args[0]), start, end)
		} else {
			val = db.ZRevRange([]byte(args[0]), start, end)
		}
	} else {
		if withScores {
			val = db.ZRangeWithScores([]byte(args[0]), start, end)
		} else {
			val = db.ZRange([]byte(args[0]), start, end)
		}
	}

	results := make([]string, len(val))
	for i, v := range val {
		results[i] = fmt.Sprintf("%v", v)
	}
	res = results
	return
}

func zRem(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = ErrSyntaxIncorrect
		return
	}
	var ok bool
	if ok, err = db.ZRem([]byte(args[0]), []byte(args[1])); err == nil {
		if ok {
			res = redcon.SimpleInt(1)
		} else {
			res = redcon.SimpleInt(0)
		}
	}
	return
}

func zGetByRank(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("zgetbyrank")
		return
	}
	return zRawGetByRank(db, args, false)
}

func zRevGetByRank(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("zrevgetbyrank")
		return
	}
	return zRawGetByRank(db, args, true)
}

// for zGetByRank and zRevGetByRank
func zRawGetByRank(db *rosedb.RoseDB, args []string, rev bool) (res interface{}, err error) {
	rank, err := strconv.Atoi(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}

	var val []interface{}
	if rev {
		val = db.ZRevGetByRank([]byte(args[0]), rank)
	} else {
		val = db.ZGetByRank([]byte(args[0]), rank)
	}
	results := make([]string, len(val))
	for i, v := range val {
		results[i] = fmt.Sprintf("%v", v)
	}
	res = results
	return
}

func zScoreRange(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("zscorerange")
		return
	}
	return zRawScoreRange(db, args, false)
}

func zSRevScoreRange(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("zsrevscorerange")
		return
	}
	return zRawScoreRange(db, args, true)
}

// for zScoreRange and zSRevScoreRange
func zRawScoreRange(db *rosedb.RoseDB, args []string, rev bool) (res interface{}, err error) {
	param1, err := utils.StrToFloat64(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	param2, err := utils.StrToFloat64(args[2])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	var val []interface{}
	if rev {
		val = db.ZRevScoreRange([]byte(args[0]), param1, param2)
	} else {
		val = db.ZScoreRange([]byte(args[0]), param1, param2)
	}
	results := make([]string, len(val))
	for i, v := range val {
		results[i] = fmt.Sprintf("%v", v)
	}
	res = results
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

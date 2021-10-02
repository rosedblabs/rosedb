package cmd

import (
	"github.com/roseduan/rosedb"
	"strconv"
	"strings"
)

func txn(db *rosedb.RoseDB, args []string) (res interface{}, err error) {

	var result []interface{}

	txnCommand := parseTxnArgs(args)

	var transaction = db.NewTransaction()
	for _, txnCmd := range txnCommand {
		if len(txnCmd) == 0 {
			continue
		}

		switch txnCmd[0].(string) {
		case "set":
			if len(txnCmd) != 3 {
				err = newWrongNumOfArgsError("set")
				return
			}
			err = transaction.Set(txnCmd[1], txnCmd[2])
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, "OK")

		case "setnx":
			if len(txnCmd) != 3 {
				err = newWrongNumOfArgsError("setnx")
				return
			}

			_, err = transaction.SetNx(txnCmd[1], txnCmd[2])
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, "OK")

		case "setex":
			if len(txnCmd) != 4 {
				err = newWrongNumOfArgsError("setex")
				return
			}

			dur, err1 := strconv.ParseInt(txnCmd[2].(string), 10, 64)
			if err1 != nil {
				err = ErrSyntaxIncorrect
				return
			}

			err = transaction.SetEx(txnCmd[1], txnCmd[3], dur)
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, "OK")

		case "get":
			if len(txnCmd) != 2 {
				err = newWrongNumOfArgsError("get")
				return
			}

			var val string
			err = transaction.Get(txnCmd[1], &val)
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, val)

		case "getset":
			if len(txnCmd) != 3 {
				err = newWrongNumOfArgsError("getset")
				return
			}

			var val string
			err = transaction.GetSet(txnCmd[1], txnCmd[2], &val)
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, val)

		case "append":
			if len(txnCmd) != 3 {
				err = newWrongNumOfArgsError("append")
				return
			}

			err = transaction.Append(txnCmd[1], txnCmd[2].(string))
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, "OK")

		case "strexists":
			if len(txnCmd) != 2 {
				err = newWrongNumOfArgsError("strexists")
				return
			}

			var exist bool
			exist = transaction.StrExists(txnCmd[1])
			if exist {
				result = append(result, "OK")
			}
			result = append(result, "nil")

		case "remove":
			if len(txnCmd) != 2 {
				err = newWrongNumOfArgsError("remove")
				return
			}

			err = transaction.Remove(txnCmd[1])
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, "OK")

		case "lpush":
			if len(txnCmd) <= 2 {
				err = newWrongNumOfArgsError("lpush")
				return
			}

			err = transaction.LPush(txnCmd[1], txnCmd[1:]...)
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, "OK")

		case "rpush":
			if len(txnCmd) <= 2 {
				err = newWrongNumOfArgsError("rpush")
				return
			}

			err = transaction.RPush(txnCmd[1], txnCmd[1:]...)
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, "OK")

		case "hset":
			if len(txnCmd) != 4 {
				err = newWrongNumOfArgsError("hset")
				return
			}

			err = transaction.HSet(txnCmd[1], txnCmd[2], txnCmd[3])
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, "OK")

		case "hsetnx":
			if len(txnCmd) != 4 {
				err = newWrongNumOfArgsError("hsetnx")
				return
			}

			err = transaction.HSetNx(txnCmd[1], txnCmd[2], txnCmd[3])
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, "OK")

		case "hget":
			if len(txnCmd) != 3 {
				err = newWrongNumOfArgsError("hget")
				return
			}

			var dest string
			err = transaction.HGet(txnCmd[1], txnCmd[2], &dest)
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, dest)

		case "hdel":
			if len(txnCmd) <= 2 {
				err = newWrongNumOfArgsError("hdel")
				return
			}

			err = transaction.HDel(txnCmd[1], txnCmd[1:]...)
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, "OK")

		case "hexists":
			if len(txnCmd) != 3 {
				err = newWrongNumOfArgsError("hexists")
				return
			}

			ok := transaction.HExists(txnCmd[1], txnCmd[2])
			if ok {
				result = append(result, "OK")
			}

			result = append(result, "nil")

		case "sadd":
			if len(txnCmd) <= 2 {
				err = newWrongNumOfArgsError("sadd")
				return
			}

			err = transaction.SAdd(txnCmd[1], txnCmd[1:]...)
			if err != nil {
				return
			}

			result = append(result, "OK")

		case "sismember":
			if len(txnCmd) != 3 {
				err = newWrongNumOfArgsError("sismember")
				return
			}

			ok := transaction.SIsMember(txnCmd[1], txnCmd[2])
			if ok {
				result = append(result, "OK")
			}

			result = append(result, "nil")

		case "srem":
			if len(txnCmd) <= 2 {
				err = newWrongNumOfArgsError("srem")
				return
			}

			err = transaction.SRem(txnCmd[1], txnCmd[1:]...)
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, "OK")

		case "zadd":
			if len(txnCmd) != 4 {
				err = newWrongNumOfArgsError("zadd")
				return
			}

			score, err1 := strconv.ParseFloat(txnCmd[2].(string), 64)
			if err1 != nil {
				err = ErrSyntaxIncorrect
				return
			}

			err = transaction.ZAdd(txnCmd[1], score, txnCmd[3])
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, "OK")

		case "zscore":
			if len(txnCmd) != 3 {
				err = newWrongNumOfArgsError("zscore")
				return
			}

			_, score, err1 := transaction.ZScore(txnCmd[1], txnCmd[2])
			if err1 != nil {
				transaction.Rollback()
				return
			}
			result = append(result, strconv.FormatFloat(score, 'f', -1, 64))

		case "zrem":
			if len(txnCmd) != 3 {
				err = newWrongNumOfArgsError("zrem")
				return
			}

			err = transaction.ZRem(txnCmd[1], txnCmd[2])
			if err != nil {
				transaction.Rollback()
				return
			}
			result = append(result, "OK")

		}
	}

	err = transaction.Commit()

	res = result

	return
}

func parseTxnArgs(args []string) [][]interface{} {
	var txnCommands [][]interface{}

	for _, arg := range args {
		arg = strings.Trim(arg, "[")
		arg = strings.Trim(arg, "]")

		var txnCmd []interface{}
		for _, sli := range strings.Split(arg, " ") {
			txnCmd = append(txnCmd, sli)
		}

		txnCommands = append(txnCommands, txnCmd)
	}

	return txnCommands
}

func init() {
	addExecCommand("txn", txn)
}

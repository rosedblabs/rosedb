package main

import (
	"strings"

	"github.com/flower-corp/rosedb"
	"github.com/tidwall/redcon"
)

type cmdHandler func(cli *Client, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	// string commands
	"set":      set,
	"get":      get,
	"mget":     mGet,
	"getrange": getRange,
	"getdel":   getDel,
	"setex":    setEX,
	"setnx":    setNX,
	"mset":     mSet,
	"msetnx":   mSetNX,
	"append":   appendStr,
	"decr":     decr,
	"decrby":   decrBy,
	"incr":     incr,
	"incrby":   incrBy,
	"strlen":   strLen,

	// list
	"lpush":  lPush,
	"lpushx": lPushX,
	"rpush":  rPush,
	"rpushx": rPushX,
	"lpop":   lPop,
	"rpop":   rPop,
	"lmove":  lMove,
	"llen":   lLen,
	"lindex": lIndex,
	"lset":   lSet,
	"lrange": lRange,
	"lrem":   lRem,

	// hash commands
	"hset":       hSet,
	"hsetnx":     hSetNX,
	"hget":       hGet,
	"hmget":      hmGet,
	"hdel":       hDel,
	"hexists":    hExists,
	"hlen":       hLen,
	"hkeys":      hKeys,
	"hvals":      hVals,
	"hgetall":    hGetAll,
	"hstrlen":    hStrLen,
	"hscan":      hScan,
	"hincrby":    hIncrBy,
	"hrandfield": hRandField,

	// set commands
	"sadd":       sAdd,
	"spop":       sPop,
	"srem":       sRem,
	"sismember":  sIsMember,
	"smismember": sMisMember,
	"smembers":   sMembers,
	"scard":      sCard,
	"sdiff":      sDiff,
	"sdiffstore": sDiffStore,
	"sunion":     sUnion,

	// zset commands
	"zadd":      zAdd,
	"zscore":    zScore,
	"zrem":      zRem,
	"zcard":     zCard,
	"zrange":    zRange,
	"zrevrange": zRevRange,
	"zrank":     zRank,
	"zrevrank":  zRevRank,

	// generic commands
	"type": keyType,
	"del":  del,

	// connection management commands
	"select": selectDB,
	"ping":   ping,
	"quit":   nil,

	// server management commands
	"info": info,
}

type Client struct {
	svr *Server
	db  *rosedb.RoseDB
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("ERR unsupported command '" + string(cmd.Args[0]) + "'")
		return
	}

	cli, _ := conn.Context().(*Client)
	if cli == nil {
		conn.WriteError(errClientIsNil.Error())
		return
	}
	switch command {
	case "quit":
		_ = conn.Close()
	default:
		if res, err := cmdFunc(cli, cmd.Args[1:]); err != nil {
			if err == rosedb.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
		} else {
			conn.WriteAny(res)
		}
	}
}

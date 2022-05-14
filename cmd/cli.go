package main

import (
	"github.com/flower-corp/rosedb"
	"github.com/tidwall/redcon"
	"strings"
)

type cmdHandler func(cli *Client, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	// string
	"set":    set,
	"setex":  setex,
	"get":    get,
	"mget":   mget,
	"append": appendStr,

	// list
	"lpush": lpush,
	"rpush": rpush,
	"lpop":  lpop,
	"rpop":  rpop,
	"llen":  llen,

	// generic
	"type": keyType,
	"del":  del,

	// connection management
	"select": selectDB,
	"ping":   ping,
	"quit":   nil,

	// Server management
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

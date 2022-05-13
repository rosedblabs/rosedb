package main

import (
	"flag"
	"fmt"
	"github.com/flower-corp/rosedb"
	"github.com/flower-corp/rosedb/logger"
	"github.com/tidwall/redcon"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
)

type cmdHandler func(db *rosedb.RoseDB, args [][]byte) (interface{}, error)

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

	"info": info,

	// other
	"ping": nil,
	"quit": nil,
}

var (
	defaultDBPath = filepath.Join("/tmp", "rosedb")
	defaultHost   = "127.0.0.1"
	defaultPort   = "5200"

	dbPath string
	host   string
	port   string
)

func init() {
	// print banner
	path, _ := filepath.Abs("resource/banner.txt")
	banner, _ := ioutil.ReadFile(path)
	fmt.Println(string(banner))

	// options
	flag.StringVar(&dbPath, "dbpath", defaultDBPath, "db path")
	flag.StringVar(&host, "host", defaultHost, "server host")
	flag.StringVar(&port, "port", defaultPort, "server port")
}

type Server struct {
	db     *rosedb.RoseDB
	ser    *redcon.Server
	signal chan os.Signal
}

func main() {
	flag.Parse()
	opts := rosedb.DefaultOptions(dbPath)
	db, err := rosedb.Open(opts)
	if err != nil {
		logger.Errorf("open rosedb err, fail to start server. %v", err)
		return
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	svr := &Server{db: db, signal: sig}
	addr := host + ":" + port
	redServer := redcon.NewServerNetwork("tcp", addr, svr.handler,
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {
		},
	)
	svr.ser = redServer
	go svr.listen()
	<-svr.signal
	svr.stop()
}

func (svr *Server) listen() {
	logger.Info("rosedb server is running, ready to accept connections")
	if err := svr.ser.ListenAndServe(); err != nil {
		logger.Fatalf("listen and serve err, fail to start. %v", err)
		return
	}
}

func (svr *Server) stop() {
	if err := svr.db.Close(); err != nil {
		logger.Errorf("close db err: %v", err)
	}
	if err := svr.ser.Close(); err != nil {
		logger.Errorf("close server err: %v", err)
	}
	logger.Info("rosedb is ready to exit, bye bye...")
}

func (svr *Server) handler(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("ERR unsupported command '" + string(cmd.Args[0]) + "'")
		return
	}
	switch command {
	case "ping":
		conn.WriteString("PONG")
	case "quit":
		_ = conn.Close()
	default:
		if res, err := cmdFunc(svr.db, cmd.Args[1:]); err != nil {
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

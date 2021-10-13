package cmd

import (
	"errors"
	"fmt"
	"github.com/roseduan/rosedb"
	"github.com/tidwall/redcon"
	"log"
	"strings"
	"sync"
)

// ExecCmdFunc func for cmd execute.
type ExecCmdFunc func(*rosedb.RoseDB, []string) (interface{}, error)

// ExecCmd exec cmd map, saving all the functions corresponding to a specified command.
var ExecCmd = make(map[string]ExecCmdFunc)

var (
	nestedMultiErr  = errors.New("ERR MULTI calls can not be nested")
	withoutMultiErr = errors.New("ERR EXEC without MULTI")
	execAbortErr    = errors.New("EXECABORT Transaction discarded because of previous errors.")
)

func addExecCommand(cmd string, cmdFunc ExecCmdFunc) {
	ExecCmd[strings.ToLower(cmd)] = cmdFunc
}

// Server a rosedb server.
type Server struct {
	server   *redcon.Server
	db       *rosedb.RoseDB
	closed   bool
	mu       sync.Mutex
	TxnLists sync.Map
}

type TxnList struct {
	cmdArgs [][]string
	err     error
}

// NewServer create a new rosedb server.
func NewServer(config rosedb.Config) (*Server, error) {
	db, err := rosedb.Open(config)
	if err != nil {
		return nil, err
	}
	return &Server{db: db}, nil
}

// Listen listen the server.
func (s *Server) Listen(addr string) {
	svr := redcon.NewServerNetwork("tcp", addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			s.handleCmd(conn, cmd)
		},
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {
			s.TxnLists.Delete(conn.RemoteAddr())
		},
	)

	s.server = svr
	log.Println("rosedb is running, ready to accept connections.")
	if err := svr.ListenAndServe(); err != nil {
		log.Printf("listen and serve ocuurs error: %+v", err)
	}
}

// Stop stops the server.
func (s *Server) Stop() {
	if s.closed {
		return
	}
	s.mu.Lock()
	s.closed = true
	if err := s.server.Close(); err != nil {
		log.Printf("close redcon err: %+v\n", err)
	}
	if err := s.db.Close(); err != nil {
		log.Printf("close rosedb err: %+v\n", err)
	}
	s.mu.Unlock()
}

func (s *Server) handleCmd(conn redcon.Conn, cmd redcon.Command) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic when handle the cmd: %+v", r)
		}
	}()

	var reply interface{}
	var err error

	command := strings.ToLower(string(cmd.Args[0]))
	if command == "multi" {
		if _, ok := s.TxnLists.Load(conn.RemoteAddr()); !ok {

			var txnList TxnList
			s.TxnLists.Store(conn.RemoteAddr(), &txnList)
			reply = "OK"

		} else {

			err = nestedMultiErr

		}
	} else if command == "exec" {
		if value, ok := s.TxnLists.Load(conn.RemoteAddr()); ok {
			s.TxnLists.Delete(conn.RemoteAddr())

			txnList := value.(*TxnList)
			if txnList.err != nil {
				err = execAbortErr
			} else {
				if len(txnList.cmdArgs) == 0 {
					reply = "(empty list or set)"
				} else {
					reply, err = txn(s.db, txnList.cmdArgs)
				}

			}

		} else {
			err = withoutMultiErr
		}

	} else {

		if value, ok := s.TxnLists.Load(conn.RemoteAddr()); ok {

			txnList := value.(*TxnList)
			_, exist := ExecCmd[command]

			if !exist {
				txnList.err = fmt.Errorf("ERR unknown command '%s'", command)
				conn.WriteError(txnList.err.Error())
				return
			}

			txnList.cmdArgs = append(txnList.cmdArgs, parseTxnArgs(cmd.Args))

			reply = "QUEUED"

		} else {

			exec, exist := ExecCmd[command]
			if !exist {
				conn.WriteError(fmt.Sprintf("ERR unknown command '%s'", command))
				return
			}

			args := parseArgs(cmd.Args)
			reply, err = exec(s.db, args)

		}

	}

	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteAny(reply)

}

func parseArgs(cmdArgs [][]byte) []string {
	args := make([]string, 0, len(cmdArgs)-1)
	for i, bytes := range cmdArgs {
		if i == 0 {
			continue
		}
		args = append(args, string(bytes))
	}
	return args
}

func parseTxnArgs(cmdArgs [][]byte) []string {
	args := make([]string, 0, len(cmdArgs))
	for _, bytes := range cmdArgs {
		args = append(args, string(bytes))
	}
	return args
}

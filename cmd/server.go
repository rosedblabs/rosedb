package cmd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"regexp"
	"rosedb"
	"sync"
	"time"
)

var reg, _ = regexp.Compile(`'.*?'|".*?"|\S+`)

const connInterval = 8

type Server struct {
	db       *rosedb.RoseDB
	closed   bool
	mu       sync.Mutex
	done     chan struct{}
	listener net.Listener
}

func NewServer(config rosedb.Config) (*Server, error) {
	db, err := rosedb.Open(config)
	if err != nil {
		return nil, err
	}
	return &Server{db: db, done: make(chan struct{})}, nil
}

func (s *Server) Listen(addr string) {
	var err error
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		log.Printf("tcp listen err: %+v\n", err)
		return
	}

	log.Println("rosedb is running, ready to accept connections.")
	for {
		select {
		case <-s.done:
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				continue
			}
			go s.handleConn(conn)
		}
	}
}

func (s *Server) Stop() {
	if s.closed {
		return
	}
	s.mu.Lock()
	close(s.done)
	s.closed = true
	s.listener.Close()
	if err := s.db.Close(); err != nil {
		fmt.Printf("close rosedb err: %+v\n", err)
	}
	s.mu.Unlock()
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(time.Hour * connInterval))

		bufReader := bufio.NewReader(conn)
		b := make([]byte, 4)
		_, err := bufReader.Read(b)
		if err != nil {
			log.Printf("read cmd size err: %+v\n", err)
			break
		}

		size := binary.BigEndian.Uint32(b[:4])
		if size > 0 {
			data := make([]byte, size)
			_, err := bufReader.Read(data)
			if err != nil {
				log.Printf("read cmd data err: %+v\n", err)
				break
			}

			cmdAndArgs := reg.FindAllString(string(data), -1)
			reply := s.handleCmd(cmdAndArgs[0], cmdAndArgs[1:])
			info := wrapReplyInfo(reply)
			_, err = conn.Write(info)
			if err != nil {
				log.Printf("write reply err: %+v\n", err)
			}
		}
	}
}

func (s *Server) handleCmd(cmd string, args []string) string {
	if cmd == "quit" {
		s.Stop()
		return ""
	}

	exec, exist := ExecCmd[cmd]
	if !exist {
		return "command not found"
	}

	return exec(s.db, args)
}

func wrapReplyInfo(reply string) []byte {
	b := make([]byte, len(reply)+4)
	binary.BigEndian.PutUint32(b[:4], uint32(len(reply)))
	copy(b[4:], reply)
	return b
}

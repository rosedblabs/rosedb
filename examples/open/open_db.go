package main

import (
	"fmt"
	"github.com/flower-corp/rosedb"
	"path/filepath"
)

func main() {
	path := filepath.Join("/tmp", "rosedb")
	// specify other options
	// opts.XXX
	opts := rosedb.DefaultOptions(path)
	db, err := rosedb.Open(opts)
	if err != nil {
		fmt.Printf("open rosedb err: %v", err)
		return
	}
	defer func() {
		_ = db.Close()
	}()
}

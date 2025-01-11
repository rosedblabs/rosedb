package main

import (
	"fmt"
	"runtime"
	"time"

	"github.com/rosedblabs/rosedb/v2"
	"github.com/rosedblabs/rosedb/v2/utils"
)

// this file shows how to use the Watch feature of rosedb.

func main() {
	// specify the options
	options := rosedb.DefaultOptions
	sysType := runtime.GOOS
	if sysType == "windows" {
		options.DirPath = "C:\\rosedb_watch"
	} else {
		options.DirPath = "/tmp/rosedb_watch"
	}
	options.WatchQueueSize = 1000

	// open a database
	db, err := rosedb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// run a new goroutine to handle db event.
	go func() {
		eventCh, err := db.Watch()
		if err != nil {
			return
		}
		for {
			event := <-eventCh
			// when db closed, the event will receive nil.
			if event == nil {
				fmt.Println("The db is closed, so the watch channel is closed.")
				return
			}
			// events can be captured here for processing
			fmt.Printf("Get a new event: key%s \n", event.Key)
		}
	}()

	// write some data
	for i := 0; i < 10; i++ {
		_ = db.Put(utils.GetTestKey(i), utils.RandomValue(64))
	}
	// delete some data
	for i := 0; i < 10/2; i++ {
		_ = db.Delete(utils.GetTestKey(i))
	}

	// wait for watch goroutine to finish.
	time.Sleep(1 * time.Second)
}

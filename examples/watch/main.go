package main

import (
	"fmt"
	"time"

	"github.com/rosedblabs/rosedb/v2"
	"github.com/rosedblabs/rosedb/v2/utils"
)

// this file shows how to use the Watch feature of rosedb.

func main() {
	// specify the options
	options := rosedb.DefaultOptions
	options.DirPath = "/tmp/rosedb_merge"
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
		wc, err := db.WatchChan()
		if err != nil {
			return
		}
		for {
			event := <-wc
			// when db closed, the event will receive nil.
			if event == nil {
				fmt.Println("The db is closed, so the watch channel is closed.")
				return
			}
			// events can be captured here for processing
			fmt.Printf("==== Get a new event ==== %s \n", event.String())
		}
	}()

	// write some data
	for i := 0; i < 10; i++ {
		_ = db.Put([]byte(utils.GetTestKey(i)), utils.RandomValue(64))
	}
	// delete some data
	for i := 0; i < 10/2; i++ {
		_ = db.Delete([]byte(utils.GetTestKey(i)))
	}

	// wait for watch goroutine to finish.
	time.Sleep(1 * time.Second)
}

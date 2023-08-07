package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/rosedblabs/rosedb/v2"
	"github.com/rosedblabs/rosedb/v2/utils"
)

// this file shows how to use the Watch feature of rosedb.

func main() {
	// specify the options
	options := rosedb.DefaultOptions
	options.DirPath = "/tmp/rosedb_watch"
	options.WatchQueueSize = 1000

	var wg sync.WaitGroup
	wg.Add(1)

	// open a database
	db, err := rosedb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	watcher, err := db.Watcher()
	if err != nil {
		return
	}

	// run a new goroutine to handle db event.
	go func() {
		defer wg.Done()

		eventCh := watcher.Watch()
		for {
			select {
			case event, ok := <-eventCh:
				if !ok {
					return
				}
				// events can be captured here for processing
				fmt.Printf("Get a new event: key%s \n", event.Key)
			}
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

	// do some work
	time.Sleep(1 * time.Second)

	watcher.Close()

	// wait for watch goroutine to finish.
	wg.Wait()
}

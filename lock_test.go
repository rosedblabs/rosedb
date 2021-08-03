package rosedb

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLockMgr_Lock(t *testing.T) {

	cfg := DefaultConfig()
	cfg.DirPath = "/tmp/rosedb"
	db, _ := Open(cfg)
	defer db.Close()

	t.Run("multi", func(t *testing.T) {
		lockMgr := newLockMgr(db)

		wg := new(sync.WaitGroup)
		wg.Add(3)

		go func() {
			unlockFunc := lockMgr.Lock(0, 1)
			defer func() {
				unlockFunc()
				fmt.Println("release lock 1")
				wg.Done()
			}()

			fmt.Println("lock-1")
			time.Sleep(time.Second * 3)
		}()

		go func() {
			unlockFunc := lockMgr.Lock(1, 2)
			defer func() {
				unlockFunc()
				fmt.Println("release lock 2")
				wg.Done()
			}()

			fmt.Println("lock-2")
			time.Sleep(time.Second * 2)
		}()

		go func() {
			unlockFunc := lockMgr.RLock(2, 3)
			defer func() {
				unlockFunc()
				fmt.Println("release lock 3")
				wg.Done()
			}()

			fmt.Println("lock-3")
			time.Sleep(time.Second * 5)
		}()

		wg.Wait()
	})
}

func TestLockMgr_RLock(t *testing.T) {

	cfg := DefaultConfig()
	cfg.DirPath = "/tmp/rosedb"
	db, _ := Open(cfg)
	defer db.Close()

	t.Run("multi", func(t *testing.T) {
		lockMgr := newLockMgr(db)

		wg := new(sync.WaitGroup)
		wg.Add(4)

		go func() {
			unlockFunc := lockMgr.RLock(0, 1, 2, 3, 4)
			defer func() {
				unlockFunc()
				fmt.Println("release lock 1")
				wg.Done()
			}()

			fmt.Println("lock-1")
			time.Sleep(time.Second * 3)
		}()

		go func() {
			unlockFunc := lockMgr.RLock(0, 1, 2, 3, 4)
			defer func() {
				unlockFunc()
				fmt.Println("release lock 2")
				wg.Done()
			}()

			fmt.Println("lock-2")
			time.Sleep(time.Second * 2)
		}()

		go func() {
			unlockFunc := lockMgr.RLock(0, 1, 2, 3, 4)
			defer func() {
				unlockFunc()
				fmt.Println("release lock 3")
				wg.Done()
			}()

			fmt.Println("lock-3")
			time.Sleep(time.Second * 5)
		}()

		go func() {
			db.strIndex.mu.RLock()
			defer func() {
				db.strIndex.mu.RUnlock()
				wg.Done()
				fmt.Println("release lock 4")
			}()

			fmt.Println("lock-4")
			time.Sleep(time.Second * 10)
		}()

		wg.Wait()
	})
}

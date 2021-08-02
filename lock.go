package rosedb

import "sync"

// LockMgr is a lock manager that manages read and write operations of different data structures.
// It is also be used to manage transaction.
type LockMgr struct {
	locks map[DataType]*sync.RWMutex
}

func newLockMgr(db *RoseDB) *LockMgr {
	locks := make(map[DataType]*sync.RWMutex)
	// store the lock of different data types.
	locks[String] = db.strIndex.mu
	locks[List] = db.listIndex.mu
	locks[Hash] = db.hashIndex.mu
	locks[Set] = db.setIndex.mu
	locks[ZSet] = db.zsetIndex.mu

	return &LockMgr{locks: locks}
}

// Lock locks the rw of dTypes for writing.
func (lm *LockMgr) Lock(dTypes ...DataType) func() {
	for _, t := range dTypes {
		lm.locks[t].Lock()
	}

	unLockFunc := func() {
		for _, t := range dTypes {
			lm.locks[t].Unlock()
		}
	}
	return unLockFunc
}

// RLock locks the rw of dTypes for reading.
func (lm *LockMgr) RLock(dTypes ...DataType) func() {
	for _, t := range dTypes {
		lm.locks[t].RLock()
	}

	unLockFunc := func() {
		for _, t := range dTypes {
			lm.locks[t].RUnlock()
		}
	}
	return unLockFunc
}

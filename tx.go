package rosedb

import (
	"github.com/roseduan/rosedb/storage"
	"sync"
)

type Txn struct {
	db            *RoseDB
	readSeq       uint64
	commitSeq     uint64
	pendingWrites map[string]*storage.Entry
	readKeys      map[uint64]struct{}
	update        bool
}

type writeBuffer struct {
	entries []*storage.Entry
	wg      *sync.WaitGroup
	Err     error
}

// NewTxn create a new transaction.
func (db *RoseDB) NewTxn(update bool) (*Txn, error) {
	if db.isClosed() {
		return nil, ErrDBIsClosed
	}

	txn := &Txn{
		db:            db,
		update:        update,
		pendingWrites: make(map[string]*storage.Entry),
	}

	// get read seq todo

	return txn, nil
}

// TxnUpdate read-write transaction.
func (db *RoseDB) TxnUpdate(fn func(tx *Txn) error) (err error) {
	tx, err := db.NewTxn(true)
	if err != nil {
		return err
	}
	defer tx.finish()

	if err = fn(tx); err != nil {
		return
	}

	return tx.Commit()
}

// TxnView read only transaction
func (db *RoseDB) TxnView(fn func(tx *Txn) error) (err error) {
	tx, err := db.NewTxn(false)
	if err != nil {
		return err
	}
	defer tx.finish()

	if err = fn(tx); err != nil {
		return
	}

	return
}

// Commit ...
func (tx *Txn) Commit() (err error) {
	if len(tx.pendingWrites) == 0 {
		return
	}

	// send
	waitFn, err := tx.send()
	if err != nil {
		return err
	}
	// wait for write done. todo
	return waitFn()
}

func (tx *Txn) send() (func() error, error) {
	txnMgr := tx.db.txnMgr
	txnMgr.mu.Lock()
	defer txnMgr.mu.Unlock()

	// check conflict. todo

	// clean transaction if necessary. todo

	// get commit seq. todo

	var entries []*storage.Entry
	// put all entries into a slice.(set every entry`s version, add a special entry as end.) todo

	// start commit, send entries to txnCh(defined in RoseDB). todo
	buf := &writeBuffer{
		entries: entries,
		wg:      new(sync.WaitGroup),
	}
	buf.wg.Add(1)

	tx.db.sendTxnChn(buf)

	waitFn := func() error {
		buf.wg.Wait()
		return buf.Err
	}
	return waitFn, nil
}

// Rollback ...
func (tx *Txn) Rollback() {
	tx.finish()
}

func (tx *Txn) finish() {
	tx.pendingWrites = nil
	tx.db = nil
}

// Set ...
func (tx *Txn) Set(key, value interface{}) error {
	return nil
}

// Get ...
func (tx *Txn) Get(key, dest interface{}) error {
	return nil
}

// Remove ...
func (tx *Txn) Remove(key interface{}) error {
	return nil
}

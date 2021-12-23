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

	update   bool
	doneRead bool
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

	// Is there an update or write operation
	if update {
		txn.pendingWrites = make(map[string]*storage.Entry)
	}

	// get read seq todo
	txn.readSeq = db.txnMgr.getReadSeq()

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

// Commit ... 对应bedger 中的commit
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
	txnMgr.writeChLock.Lock()
	defer txnMgr.writeChLock.Unlock()

	// check conflict. todo
	conflict := txnMgr.checkConflict(tx)
	if conflict {
		return nil, nil
	}

	// clean transaction if necessary. todo
	var ts uint64

	txnMgr.doneRead(tx)
	txnMgr.cleanCommittedTxns()

	// This is the general case, when user doesn't specify the read and commit ts.
	ts = txnMgr.nextSeq
	txnMgr.nextSeq++
	txnMgr.txnMark.Begin(ts)

	if ts < txnMgr.lastCleanupTs {
		panic("ts < lastCleanupTs in cleanCommittedTxns()")
	}

	// get commit seq. todo
	// Add the current transaction to the list of future detections
	txnMgr.committedTxs = append(txnMgr.committedTxs, committedTxn{
		seq:      ts,
		readKeys: tx.readKeys,
	})

	var entries []*storage.Entry
	// put all entries into a slice.(set every entry`s version, add a special entry as end.) todo

	setVersion := func(e *storage.Entry) {
		if e.Version == 0 {
			e.Version = ts
		}
	}
	for _, e := range tx.pendingWrites {
		setVersion(e)
	}

	for _, e := range tx.pendingWrites {
		entries = append(entries, e)
	}

	// 是这个方法生成最后的事务提交信息嘛 如果是应该填些什么呢？
	e := storage.NewEntryWithTxn([]byte("Txn_key"), []byte("Txn_val"), []byte("Txn_end"), 101, String, ts)
	entries = append(entries, e)

	// start commit, send entries to txnCh(defined in RoseDB). todo
	buf := &writeBuffer{
		entries: entries,
		wg:      new(sync.WaitGroup),
	}
	buf.wg.Add(1)

	tx.db.sendTxnChn(buf)

	waitFn := func() error {
		buf.wg.Wait()
		txnMgr.doneCommit(ts)
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

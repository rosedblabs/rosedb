package rosedb

import "github.com/roseduan/rosedb/storage"

type Txn struct {
	db            *RoseDB
	readSeq       uint64
	commitSeq     uint64
	pendingWrites map[string]*storage.Entry
	readKeys      map[uint64]struct{}
	update        bool
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

	// check conflict. todo

	// get commit seq. todo

	// handle all entries(set version, add a special )

	// start commit, write entries into db file. todo

	return
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

// Get
func (tx *Txn) Get(key, dest interface{}) error {
	return nil
}

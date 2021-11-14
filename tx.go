package rosedb

import "github.com/roseduan/rosedb/storage"

type Txn struct {
	db            *RoseDB
	readSeq       uint64
	commitSeq     uint64
	pendingWrites map[string]*storage.Entry
}

type TxnManager struct {
	nextSeq      uint64
	committedTxs []committedTxn
}

type committedTxn struct {
	commitSeq uint64
	readKeys  map[uint64]struct{}
}

func (db *RoseDB) NewTxn() (*Txn, error) {
	if db.isClosed() {
		return nil, ErrDBIsClosed
	}
	return nil, nil
}

// TxnUpdate read-write transaction.
func (db *RoseDB) TxnUpdate(func(tx *Txn) error) (err error) {

	return
}

// TxnView read only transaction
func (db *RoseDB) TxnView(func(tx *Txn) error) (err error) {

	return
}

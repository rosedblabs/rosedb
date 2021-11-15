package rosedb

import "sync"

type TxnManager struct {
	mu           sync.Mutex
	nextSeq      uint64
	committedTxs []committedTxn
}

type committedTxn struct {
	seq      uint64
	readKeys map[uint64]struct{}
}

type TxnMark struct {
}

func (mgr *TxnManager) checkConflict(tx *Txn) bool {
	// todo
	return false
}

func (mgr *TxnManager) getReadSeq() uint64 {
	// todo
	return 0
}

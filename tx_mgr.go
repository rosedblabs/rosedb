package rosedb

import "sync"

type TxnManager struct {
	mu           sync.Mutex
	nextSeq      uint64
	committedTxs []committedTxn
}

type committedTxn struct {
	commitSeq uint64
	readKeys  map[uint64]struct{}
}

type TxnMark struct {
}

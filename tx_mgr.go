package rosedb

import (
	"container/heap"
	"github.com/roseduan/rosedb/utils"
	"sync"
)

func (q priorityQueue) Len() int           { return len(q) }
func (q priorityQueue) Less(i, j int) bool { return q[i] < q[j] }
func (q priorityQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }

func (q *priorityQueue) Push(x interface{}) {
	*q = append(*q, x.(uint64))
}

func (q *priorityQueue) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[0 : n-1]
	return x
}

type (
	priorityQueue []uint64

	mark struct {
		seq    uint64
		done   bool
		waiter chan struct{}
	}

	TxnMark struct {
		latestDone utils.AtomicUint64
		latestSeq  utils.AtomicUint64
		markChn    chan mark

		seqNumbers priorityQueue
		count      map[uint64]int
		waiters    map[uint64]chan struct{}
	}
)

func (ma *TxnMark) Init() {
	ma.markChn = make(chan mark)
	go ma.startWatch()
}

func (ma *TxnMark) Begin(seq uint64) {
	ma.latestSeq.Set(seq)
	ma.markChn <- mark{seq: seq, done: false}
}

func (ma *TxnMark) Done(seq uint64) {
	ma.markChn <- mark{seq: seq, done: true}
}

func (ma *TxnMark) startWatch() {
	heap.Init(&ma.seqNumbers)
	ma.count = make(map[uint64]int)
	ma.waiters = make(map[uint64]chan struct{})

	for {
		select {
		case mark := <-ma.markChn:
			if mark.waiter != nil {
				// todo
			} else {
				ma.handle(mark)
			}
		}
	}
}

func (ma *TxnMark) handle(m mark) {
	seq, done := m.seq, m.done
	prev, ok := ma.count[seq]
	if !ok {
		heap.Push(&ma.seqNumbers, seq)
	}

	flag := 1
	if done {
		flag = -1
	}
	ma.count[seq] = prev + flag
	if ma.latestDone.Get() > seq {

	}
}

type TxnManager struct {
	mu           *sync.Mutex
	nextSeq      uint64
	committedTxs []committedTxn
}

type committedTxn struct {
	seq      uint64
	readKeys map[uint64]struct{}
}

func (mgr *TxnManager) checkConflict(tx *Txn) bool {
	// todo
	return false
}

func (mgr *TxnManager) getReadSeq() uint64 {
	// todo
	return 0
}

// clean useless committed txns.
func (mgr *TxnManager) cleanCommittedTxns() {
	// todo
}

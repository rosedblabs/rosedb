package rosedb

import (
	"container/heap"
	"context"
	"fmt"
	"github.com/roseduan/rosedb/utils"
	"os"
	"os/signal"
	"sync"
	"syscall"
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
		waiters    map[uint64][]chan struct{}
	}
)

func (ma *TxnMark) Init(closed *closeSignal) {
	ma.markChn = make(chan mark, 100)
	go ma.startWatch(closed)
}

func (ma *TxnMark) Begin(seq uint64) {
	ma.latestSeq.Set(seq)
	ma.markChn <- mark{seq: seq, done: false}
}

func (ma *TxnMark) Done(seq uint64) {
	ma.markChn <- mark{seq: seq, done: true}
}

func (ma *TxnMark) WaitDone(ctx context.Context, seq uint64) error {
	if ma.latestDone.Get() >= seq {
		return nil
	}
	waitChn := make(chan struct{})
	ma.markChn <- mark{seq: seq, waiter: waitChn}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitChn:
		return nil
	}
}

func (ma *TxnMark) startWatch(closed *closeSignal) {
	heap.Init(&ma.seqNumbers)
	ma.count = make(map[uint64]int)
	ma.waiters = make(map[uint64][]chan struct{})

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for {
		select {
		case <-closed.chn:
			return
		case <-sig:
			return
		case mark := <-ma.markChn:
			if mark.waiter != nil {
				if ma.latestDone.Get() >= mark.seq {
					close(mark.waiter)
				} else {
					ma.waiters[mark.seq] = append(ma.waiters[mark.seq], mark.waiter)
				}
			} else {
				ma.handle(&mark)
			}
		}
	}
}

func (ma *TxnMark) handle(m *mark) {
	if m == nil {
		return
	}

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

	lastDone := ma.latestDone.Get()
	if lastDone > seq {
		panic(fmt.Sprintf("latest:%d can`t be bigger than seq:%d", lastDone, seq))
	}

	latest := lastDone
	for len(ma.seqNumbers) > 0 {
		min := ma.seqNumbers[0]
		// if min txn is not done, break immediately.
		if done := ma.count[min]; done > 0 {
			break
		}

		// min txn is done.
		heap.Pop(&ma.seqNumbers)
		delete(ma.count, min)
		latest = min
	}

	if lastDone != latest {
		swap := ma.latestDone.CompareAndSwap(lastDone, latest)
		if !swap {
			panic(fmt.Sprintf("compare and swap last done fail, old:%d, new:%d", lastDone, latest))
		}
	}

	notifyWaiter := func(seq uint64, wts []chan struct{}) {
		for _, ch := range wts {
			close(ch)
		}
		delete(ma.waiters, seq)
	}
	if latest-lastDone <= uint64(len(ma.waiters)) {
		for seq := lastDone + 1; seq <= latest; seq++ {
			if wts, ok := ma.waiters[seq]; ok {
				notifyWaiter(seq, wts)
			}
		}
	} else {
		for seq, wts := range ma.waiters {
			if seq <= latest {
				notifyWaiter(seq, wts)
			}
		}
	}
}

type TxnManager struct {
	mu           sync.Mutex
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

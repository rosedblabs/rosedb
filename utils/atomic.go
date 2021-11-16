package utils

import "sync/atomic"

// some wrappers of sync/atomic.

type AtomicUint64 struct {
	uint64
}

func (i *AtomicUint64) Get() uint64 {
	return atomic.LoadUint64(&i.uint64)
}

func (i *AtomicUint64) Set(val uint64) {
	atomic.StoreUint64(&i.uint64, val)
}

func (i *AtomicUint64) CompareAndSwap(old, new uint64) bool {
	return atomic.CompareAndSwapUint64(&i.uint64, old, new)
}

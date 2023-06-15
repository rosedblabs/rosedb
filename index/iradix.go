package index

import (
	"sync"

	iradix "github.com/rosedblabs/go-immutable-radix/v2"
	"github.com/rosedblabs/wal"
)

// IRadixTree is a wrapper of immutable radix tree of hashicorp.
// See more details: github.com/hashicorp/go-immutable-radix.
type IRadixTree struct {
	tree *iradix.Tree[*wal.ChunkPosition]
	lock sync.Mutex
}

func newRadixTree() *IRadixTree {
	return &IRadixTree{
		tree: iradix.New[*wal.ChunkPosition](),
	}
}

func (irx *IRadixTree) Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition {
	irx.lock.Lock()
	defer irx.lock.Unlock()
	var oldPos *wal.ChunkPosition
	irx.tree, oldPos, _ = irx.tree.Insert(key, position)
	return oldPos
}

func (irx *IRadixTree) Get(key []byte) *wal.ChunkPosition {
	pos, _ := irx.tree.Get(key)
	return pos
}

func (irx *IRadixTree) Delete(key []byte) (*wal.ChunkPosition, bool) {
	irx.lock.Lock()
	defer irx.lock.Unlock()
	var oldPos *wal.ChunkPosition
	var ok bool
	irx.tree, oldPos, ok = irx.tree.Delete(key)
	return oldPos, ok
}

func (irx *IRadixTree) Size() int {
	return irx.tree.Len()
}

func (irx *IRadixTree) Iterator(options IteratorOptions) Iterator {
	minKey, _, _ := irx.tree.Root().Minimum()
	maxKey, _, _ := irx.tree.Root().Maximum()
	radixIter := &IRadixTreeIterator{
		options: options,
		tree:    irx.tree,
		min:     minKey,
		max:     maxKey,
	}
	radixIter.Rewind()
	return radixIter
}

// IRadixTreeIterator is a wrapper of immutable radix tree iterator of hashicorp.
type IRadixTreeIterator struct {
	options      IteratorOptions
	min          []byte
	max          []byte
	currentKey   []byte
	currentValue *wal.ChunkPosition

	tree    *iradix.Tree[*wal.ChunkPosition]
	iter    *iradix.Iterator[*wal.ChunkPosition]
	revIter *iradix.ReverseIterator[*wal.ChunkPosition]
}

func (it *IRadixTreeIterator) Rewind() {
	it.seekInner(it.min, it.max)
}

func (it *IRadixTreeIterator) Seek(key []byte) {
	it.seekInner(key, key)
}

func (it *IRadixTreeIterator) Next() {
	if it.options.Reverse {
		it.currentKey, it.currentValue, _ = it.revIter.Previous()
	} else {
		it.currentKey, it.currentValue, _ = it.iter.Next()
	}
}

func (it *IRadixTreeIterator) Key() []byte {
	return it.currentKey
}

func (it *IRadixTreeIterator) Value() *wal.ChunkPosition {
	return it.currentValue
}

func (it *IRadixTreeIterator) Valid() bool {
	return it.currentKey != nil
}

func (it *IRadixTreeIterator) Close() {
	it.iter = nil
	it.revIter = nil
}

func (it *IRadixTreeIterator) seekInner(min, max []byte) {
	if it.options.Reverse {
		it.revIter = it.tree.Root().ReverseIterator()
		if len(it.options.Prefix) > 0 {
			it.revIter.SeekPrefix(it.options.Prefix)
		}
		it.revIter.SeekReverseLowerBound(max)
		it.currentKey, it.currentValue, _ = it.revIter.Previous()
	} else {
		it.iter = it.tree.Root().Iterator()
		if len(it.options.Prefix) > 0 {
			it.iter.SeekPrefix(it.options.Prefix)
		}
		it.iter.SeekLowerBound(min)
		it.currentKey, it.currentValue, _ = it.iter.Next()
	}
}

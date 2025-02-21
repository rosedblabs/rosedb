package index

import (
	"bytes"
	"sync"

	"github.com/google/btree"
	"github.com/rosedblabs/wal"
)

// MemoryBTree is a memory based btree implementation of the Index interface
// It is a wrapper around the google/btree package: github.com/google/btree
type MemoryBTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

type item struct {
	key []byte
	pos *wal.ChunkPosition
}

func newBTree() *MemoryBTree {
	return &MemoryBTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (it *item) Less(bi btree.Item) bool {
	if bi == nil {
		return false
	}
	return bytes.Compare(it.key, bi.(*item).key) < 0
}

func (mt *MemoryBTree) Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	oldValue := mt.tree.ReplaceOrInsert(&item{key: key, pos: position})
	if oldValue != nil {
		return oldValue.(*item).pos
	}
	return nil
}

func (mt *MemoryBTree) Get(key []byte) *wal.ChunkPosition {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	value := mt.tree.Get(&item{key: key})
	if value != nil {
		return value.(*item).pos
	}
	return nil
}

func (mt *MemoryBTree) Delete(key []byte) (*wal.ChunkPosition, bool) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	value := mt.tree.Delete(&item{key: key})
	if value != nil {
		return value.(*item).pos, true
	}
	return nil, false
}

func (mt *MemoryBTree) Size() int {
	return mt.tree.Len()
}

func (mt *MemoryBTree) Ascend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.Ascend(func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (mt *MemoryBTree) Descend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.Descend(func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (mt *MemoryBTree) AscendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.AscendRange(&item{key: startKey}, &item{key: endKey}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (mt *MemoryBTree) DescendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.DescendRange(&item{key: startKey}, &item{key: endKey}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (mt *MemoryBTree) AscendGreaterOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.AscendGreaterOrEqual(&item{key: key}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (mt *MemoryBTree) DescendLessOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.DescendLessOrEqual(&item{key: key}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (mt *MemoryBTree) Iterator(reverse bool) IndexIterator {
	if mt.tree == nil {
		return nil
	}
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	return newMemoryBTreeIterator(mt.tree, reverse)
}

// memoryBTreeIterator represents a B-tree index iterator implementation
type memoryBTreeIterator struct {
	tree    *btree.BTree // underlying B-tree implementation
	reverse bool         // indicates whether to traverse in descending order
	current *item        // current element being traversed
	valid   bool         // indicates if the iterator is valid
}

func newMemoryBTreeIterator(tree *btree.BTree, reverse bool) *memoryBTreeIterator {
	var current *item
	var valid bool
	if tree.Len() > 0 {
		if reverse {
			current = tree.Max().(*item)
		} else {
			current = tree.Min().(*item)
		}
		valid = true
	}

	return &memoryBTreeIterator{
		tree:    tree.Clone(),
		reverse: reverse,
		current: current,
		valid:   valid,
	}
}

func (it *memoryBTreeIterator) Rewind() {
	if it.tree == nil || it.tree.Len() == 0 {
		return
	}

	if it.reverse {
		it.current = it.tree.Max().(*item)
	} else {
		it.current = it.tree.Min().(*item)
	}
	it.valid = true
}

func (it *memoryBTreeIterator) Seek(key []byte) {
	if it.tree == nil || !it.valid {
		return
	}

	seekItem := &item{key: key}
	it.valid = false
	if it.reverse {
		it.tree.DescendLessOrEqual(seekItem, func(i btree.Item) bool {
			it.current = i.(*item)
			it.valid = true
			return false
		})
	} else {
		it.tree.AscendGreaterOrEqual(seekItem, func(i btree.Item) bool {
			it.current = i.(*item)
			it.valid = true
			return false
		})
	}
}

func (it *memoryBTreeIterator) Next() {
	if it.tree == nil || !it.valid {
		return
	}

	it.valid = false
	if it.reverse {
		it.tree.DescendLessOrEqual(it.current, func(i btree.Item) bool {
			if !i.(*item).Less(it.current) {
				return true
			}
			it.current = i.(*item)
			it.valid = true
			return false
		})
	} else {
		it.tree.AscendGreaterOrEqual(it.current, func(i btree.Item) bool {
			if !it.current.Less(i.(*item)) {
				return true
			}
			it.current = i.(*item)
			it.valid = true
			return false
		})
	}

	if !it.valid {
		it.current = nil
	}
}

func (it *memoryBTreeIterator) Valid() bool {
	return it.valid
}

func (it *memoryBTreeIterator) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.current.key
}

func (it *memoryBTreeIterator) Value() *wal.ChunkPosition {
	if !it.valid {
		return nil
	}
	return it.current.pos
}

func (it *memoryBTreeIterator) Close() {
	it.tree.Clear(true)
	it.tree = nil
	it.current = nil
	it.valid = false
}

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

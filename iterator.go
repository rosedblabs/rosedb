package rosedb

import (
	goart "github.com/plar/go-adaptive-radix-tree"
)

const defaultLimitNum = 1

type IteratorOptions struct {
	PrefetchSize int
	Limit        int
}

type Iterator struct {
	db       *RoseDB
	opts     IteratorOptions
	treeIter goart.Iterator
	curKey   []byte
}

func (db *RoseDB) NewIterator(opts IteratorOptions) *Iterator {
	if opts.Limit <= 0 {
		opts.Limit = defaultLimitNum
	}
	it := &Iterator{
		db:       db,
		opts:     opts,
		treeIter: db.strIndex.idxTree.Iterator(),
	}
	return it
}

func (it *Iterator) HasNext() bool {
	if it.opts.Limit <= 0 {
		return false
	}

	hasNext := it.treeIter.HasNext()
	if hasNext {
		node, _ := it.treeIter.Next()
		it.curKey = node.Key()
		it.opts.Limit--
	}
	return hasNext
}

func (it *Iterator) Key() []byte {
	return it.curKey
}

func (it *Iterator) Value() []byte {
	val, _ := it.db.getVal(it.curKey)
	return val
}

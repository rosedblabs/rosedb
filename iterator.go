package rosedb

import (
	goart "github.com/plar/go-adaptive-radix-tree"
)

type IteratorOptions struct {
	PrefetchSize int
}

type Iterator struct {
	db   *RoseDB
	iter goart.Iterator
	opts IteratorOptions
}

func (db *RoseDB) NewIterator(opts IteratorOptions) *Iterator {
	return &Iterator{
		db:   db,
		opts: opts,
		iter: db.strIndex.idxTree.Iterator(),
	}
}

func (it *Iterator) Rewind() {
	it.iter = it.db.strIndex.idxTree.Iterator()
}

func (it *Iterator) Valid() bool {
	return it.iter.HasNext()
}

func (it *Iterator) Next() {
	it.db.strIndex.mu.RLock()
	defer it.db.strIndex.mu.RUnlock()
	//node, err := it.iter.Next()
	//if err != nil {
	//	logger.Errorf("err in iter: %v", err)
	//	return
	//}
	//
}

func (it *Iterator) Key() []byte {
	return nil
}

func (it *Iterator) Value() []byte {
	return nil
}

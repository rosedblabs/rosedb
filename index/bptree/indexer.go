package bptree

import (
	"bytes"
	"path/filepath"

	"github.com/rosedblabs/rosedb/v2/index"
	"github.com/rosedblabs/wal"
)

const indexFileName = "bptree.index"

// BPTreeIndexer is an adapter that implements the Indexer interface using B+Tree.
type BPTreeIndexer struct {
	tree *BPlusTree
}

// NewBPTreeIndexer creates a new B+Tree indexer.
func NewBPTreeIndexer(dirPath string, options Options) (*BPTreeIndexer, error) {
	indexPath := filepath.Join(dirPath, indexFileName)
	tree, err := Open(indexPath, options)
	if err != nil {
		return nil, err
	}
	return &BPTreeIndexer{tree: tree}, nil
}

// Put inserts or updates a key-value pair.
func (idx *BPTreeIndexer) Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition {
	return idx.tree.Put(key, position)
}

// Get retrieves the value for a key.
func (idx *BPTreeIndexer) Get(key []byte) *wal.ChunkPosition {
	return idx.tree.Get(key)
}

// Delete removes a key from the index.
func (idx *BPTreeIndexer) Delete(key []byte) (*wal.ChunkPosition, bool) {
	return idx.tree.Delete(key)
}

// Size returns the number of keys in the index.
func (idx *BPTreeIndexer) Size() int {
	return idx.tree.Size()
}

// Close closes the B+Tree.
func (idx *BPTreeIndexer) Close() error {
	return idx.tree.Close()
}

// Sync flushes all dirty pages to disk.
func (idx *BPTreeIndexer) Sync() error {
	return idx.tree.Sync()
}

// Ascend iterates over all key-value pairs in ascending order.
func (idx *BPTreeIndexer) Ascend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	it := idx.tree.Iterator(false)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		cont, err := handleFn(it.Key(), it.Value())
		if err != nil || !cont {
			break
		}
	}
}

// AscendRange iterates over key-value pairs in [startKey, endKey) in ascending order.
func (idx *BPTreeIndexer) AscendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	it := idx.tree.Iterator(false)
	defer it.Close()

	it.Seek(startKey)
	for it.Valid() {
		key := it.Key()
		if bytes.Compare(key, endKey) >= 0 {
			break
		}
		cont, err := handleFn(key, it.Value())
		if err != nil || !cont {
			break
		}
		it.Next()
	}
}

// AscendGreaterOrEqual iterates over key-value pairs where key >= given key in ascending order.
func (idx *BPTreeIndexer) AscendGreaterOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	it := idx.tree.Iterator(false)
	defer it.Close()

	it.Seek(key)
	for it.Valid() {
		cont, err := handleFn(it.Key(), it.Value())
		if err != nil || !cont {
			break
		}
		it.Next()
	}
}

// Descend iterates over all key-value pairs in descending order.
func (idx *BPTreeIndexer) Descend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	it := idx.tree.Iterator(true)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		cont, err := handleFn(it.Key(), it.Value())
		if err != nil || !cont {
			break
		}
	}
}

// DescendRange iterates over key-value pairs in (endKey, startKey] in descending order.
// startKey is inclusive (upper bound), endKey is exclusive (lower bound).
func (idx *BPTreeIndexer) DescendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	it := idx.tree.Iterator(true)
	defer it.Close()

	it.Seek(startKey)
	for it.Valid() {
		key := it.Key()
		if bytes.Compare(key, endKey) <= 0 {
			break
		}
		cont, err := handleFn(key, it.Value())
		if err != nil || !cont {
			break
		}
		it.Next()
	}
}

// DescendLessOrEqual iterates over key-value pairs where key <= given key in descending order.
func (idx *BPTreeIndexer) DescendLessOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	it := idx.tree.Iterator(true)
	defer it.Close()

	it.Seek(key)
	for it.Valid() {
		cont, err := handleFn(it.Key(), it.Value())
		if err != nil || !cont {
			break
		}
		it.Next()
	}
}

// Iterator returns an iterator for the index.
func (idx *BPTreeIndexer) Iterator(reverse bool) index.IndexIterator {
	return idx.tree.Iterator(reverse)
}

package index

import "github.com/rosedblabs/wal"

// Indexer is an interface for indexing key and position.
// It is used to store the key and the position of the data in the WAL.
// The index will be rebuilt when the database is opened.
// You can implement your own indexer by implementing this interface.
type Indexer interface {
	// Put key and position into the index.
	Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition

	// Get the position of the key in the index.
	Get(key []byte) *wal.ChunkPosition

	// Delete the index of the key.
	Delete(key []byte) (*wal.ChunkPosition, bool)

	// Size represents the number of keys in the index.
	Size() int

	// Iterator returns an iterator for the index.
	Iterator(options IteratorOptions) Iterator
}

// Iterator is an interface for iterating the index.
type Iterator interface {
	// Rewind seek the first key in the index iterator.
	Rewind()

	// Seek move the iterator to the key which is
	// greater(less when reverse is true) than or equal to the specified key.
	Seek(key []byte)

	// Next moves the iterator to the next key.
	Next()

	// Key get the current key.
	Key() []byte

	// Value get the current value.
	Value() *wal.ChunkPosition

	// Valid returns whether the iterator is exhausted.
	Valid() bool

	// Close the iterator.
	Close()
}

// IteratorOptions is the options for the iterator.
type IteratorOptions struct {
	// Prefix filters the keys by prefix.
	Prefix []byte

	// Reverse indicates whether the iterator is reversed.
	// false is forward, true is backward.
	Reverse bool
}

type IndexerType = byte

const (
	RadixTree IndexerType = iota
)

// Change the index type as you implement.
var indexType = RadixTree

func NewIndexer() Indexer {
	switch indexType {
	case RadixTree:
		return newRadixTree()
	default:
		panic("unexpected index type")
	}
}

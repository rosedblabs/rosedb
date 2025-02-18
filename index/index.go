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

	// Ascend iterates over items in ascending order and invokes the handler function for each item.
	// If the handler function returns false, iteration stops.
	Ascend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	// AscendRange iterates in ascending order within [startKey, endKey], invoking handleFn.
	// Stops if handleFn returns false.
	AscendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	// AscendGreaterOrEqual iterates in ascending order, starting from key >= given key,
	// invoking handleFn. Stops if handleFn returns false.
	AscendGreaterOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	// Descend iterates over items in descending order and invokes the handler function for each item.
	// If the handler function returns false, iteration stops.
	Descend(handleFn func(key []byte, pos *wal.ChunkPosition) (bool, error))

	// DescendRange iterates in descending order within [startKey, endKey], invoking handleFn.
	// Stops if handleFn returns false.
	DescendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	// DescendLessOrEqual iterates in descending order, starting from key <= given key,
	// invoking handleFn. Stops if handleFn returns false.
	DescendLessOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	// Iterator returns an index iterator.
	Iterator(reverse bool) Iterator
}

type IndexerType = byte

const (
	BTree IndexerType = iota
)

// Change the index type as you implement.
var indexType = BTree

func NewIndexer() Indexer {
	switch indexType {
	case BTree:
		return newBTree()
	default:
		panic("unexpected index type")
	}
}

// Iterator represents a generic index iterator interface.
type Iterator interface {
	// Rewind resets the iterator to its initial position.
	Rewind()

	// Seek positions the cursor to the element with the specified key.
	Seek(key []byte)

	// Next moves the cursor to the next element.
	Next()

	// Valid checks if the iterator is still valid for reading.
	Valid() bool

	// Key returns the key of the current element.
	Key() []byte

	// Value returns the value (chunk position) of the current element.
	Value() *wal.ChunkPosition

	// Close releases the resources associated with the iterator.
	Close()
}

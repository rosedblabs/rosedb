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

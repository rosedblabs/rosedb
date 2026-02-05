package rosedb

import (
	"bytes"
	"log"
	"time"

	"github.com/rosedblabs/rosedb/v2/index"
)

// Item represents a key-value pair in the database.
type Item struct {
	Key   []byte
	Value []byte
}

// Iterator represents a database-level iterator that
// provides methods to traverse over the key/value pairs in the database.
// It wraps the index iterator and adds functionality to
// retrieve the actual values from the database.
type Iterator struct {
	indexIter   index.IndexIterator // index iterator for traversing keys
	db          *DB                 // database instance for retrieving values
	options     IteratorOptions     // user-defined configuration options
	lastError   error               // stores the last error encountered during iteration
	currentItem *Item               // cached current item to avoid side effects in Item()
}

// NewIterator initializes and returns a new database iterator with the specified options.
// The iterator is automatically positioned at the first valid entry.
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	iterator := &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   opts,
	}
	iterator.skipToNext()
	return iterator
}

// Rewind repositions the iterator to its initial state based on the iteration order.
// After repositioning, it automatically skips any invalid or expired entries.
func (it *Iterator) Rewind() {
	if it.db == nil || it.indexIter == nil {
		return
	}
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek positions the iterator at a specific key in the database.
// After seeking, it automatically skips any invalid or expired entries.
func (it *Iterator) Seek(key []byte) {
	if it.db == nil || it.indexIter == nil {
		return
	}
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next advances the iterator to the next valid entry in the database.
func (it *Iterator) Next() {
	if it.db == nil || it.indexIter == nil {
		return
	}
	it.indexIter.Next()
	it.skipToNext()
}

// Valid checks if the iterator is currently positioned at a valid entry.
func (it *Iterator) Valid() bool {
	if it.db == nil || it.indexIter == nil {
		return false
	}
	return it.indexIter.Valid()
}

// Item retrieves the current key-value pair as an Item.
// This method is idempotent and can be called multiple times
// without advancing the iterator.
func (it *Iterator) Item() *Item {
	return it.currentItem
}

// Close releases all resources associated with the iterator.
func (it *Iterator) Close() {
	if it.db == nil || it.indexIter == nil {
		return
	}

	it.indexIter.Close()
	it.indexIter = nil
	it.db = nil
}

// Err returns the last error encountered during iteration.
func (it *Iterator) Err() error {
	return it.lastError
}

// skipToNext advances the iterator to the next valid entry that satisfies all conditions:
// - Matches the prefix filter if one is specified
// - Has not expired
// - Has not been marked for deletion
// It updates the currentItem cache with the valid entry found.
func (it *Iterator) skipToNext() {
	it.currentItem = nil
	prefixLen := len(it.options.Prefix)

	for it.indexIter.Valid() {
		key := it.indexIter.Key()
		// Check prefix condition if prefix is specified
		if prefixLen > 0 {
			if prefixLen > len(key) || !bytes.Equal(it.options.Prefix, key[:prefixLen]) {
				it.indexIter.Next()
				continue
			}
		}

		position := it.indexIter.Value()
		if position == nil {
			it.indexIter.Next()
			continue
		}

		// read the record from data file
		chunk, err := it.db.dataFiles.Read(position)
		if err != nil {
			it.lastError = err
			if !it.options.ContinueOnError {
				it.Close()
				return
			}
			log.Printf("Error reading data file at key %q: %v", key, err)
			it.indexIter.Next()
			continue
		}

		// Skip if record is deleted or expired
		record := decodeLogRecord(chunk)
		now := time.Now().UnixNano()
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			it.indexIter.Next()
			continue
		}

		it.currentItem = &Item{
			Key:   record.Key,
			Value: record.Value,
		}
		return
	}
}

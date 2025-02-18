package rosedb

import (
	"bytes"
	"time"

	"github.com/rosedblabs/rosedb/v2/index"
)

// Iterator represents a database-level iterator that provides methods to traverse over the key/value pairs in the database.
// It wraps the index iterator and adds functionality to retrieve the actual values from the database.
type Iterator struct {
	indexIter index.IndexIterator // index iterator for traversing keys
	db        *DB                 // database instance for retrieving values
	options   IteratorOptions     // user-defined configuration options
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
	_ = iterator.skipToNext()
	return iterator
}

// Rewind repositions the iterator to its initial state based on the iteration order.
// After repositioning, it automatically skips any invalid or expired entries.
func (it *Iterator) Rewind() {
	if it.db == nil || it.indexIter == nil {
		return
	}
	it.indexIter.Rewind()
	_ = it.skipToNext()
}

// Seek positions the iterator at a specific key in the database.
// After seeking, it automatically skips any invalid or expired entries.
func (it *Iterator) Seek(key []byte) {
	if it.db == nil || it.indexIter == nil {
		return
	}
	it.indexIter.Seek(key)
	_ = it.skipToNext()
}

// Next advances the iterator to the next valid entry in the database.
func (it *Iterator) Next() {
	if it.db == nil || it.indexIter == nil {
		return
	}
	it.indexIter.Next()
	_ = it.skipToNext()
}

// Valid checks if the iterator is currently positioned at a valid entry.
func (it *Iterator) Valid() bool {
	if it.db == nil || it.indexIter == nil {
		return false
	}
	return it.indexIter.Valid()
}

// Key retrieves the key at the current iterator position.
func (it *Iterator) Key() []byte {
	if it.db == nil || it.indexIter == nil || !it.Valid() {
		return nil
	}
	record := it.skipToNext()
	return record.Key
}

// Value retrieves the value associated with the current key in the iterator.
func (it *Iterator) Value() []byte {
	if it.db == nil || it.indexIter == nil || !it.Valid() {
		return nil
	}
	record := it.skipToNext()
	return record.Value
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

// skipToNext advances the iterator to the next valid entry that satisfies all conditions:
// - Matches the prefix filter if one is specified
// - Has not expired
// - Has not been marked for deletion
// Returns the LogRecord of the valid entry or an error if no valid entry is found.
func (it *Iterator) skipToNext() *LogRecord {
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

		// Check if the key is expired
		position := it.indexIter.Value()
		if position == nil {
			it.indexIter.Next()
			continue
		}

		chunk, err := it.db.dataFiles.Read(position)
		if err != nil {
			it.indexIter.Next()
			continue
		}

		// Skip if record is deleted or expired
		record := decodeLogRecord(chunk)
		if record.Type == LogRecordDeleted || record.IsExpired(time.Now().UnixNano()) {
			it.indexIter.Next()
			continue
		}

		return record
	}
	return nil
}

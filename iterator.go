package rosedb

import (
	"github.com/rosedblabs/rosedb/v2/index"
)

// Iterator is the iterator of database.
// It can be used to iterate all the data in the db.
// You must close the iterator after using it by calling the Close() method.
//
// Since we have the index iterator, we can get the position of the data.
// Then we can get the data from the WAL by the position.
//
// The common usage is as follows:
//
// iter := db.NewIterator(DefaultIteratorOptions)
// defer iter.Close()
//
//	for ; iter.Valid(); iter.Next() {
//	    key := iter.Key()
//	    value, err := iter.Value()
//	    // do something with key/value
//	}
type Iterator struct {
	db        *DB
	indexIter index.Iterator
}

// NewIterator returns a new iterator.
func (db *DB) NewIterator(options IteratorOptions) *Iterator {
	return &Iterator{
		db: db,
		indexIter: db.index.Iterator(index.IteratorOptions{
			Prefix:  options.Prefix,
			Reverse: options.Reverse,
		}),
	}
}

// Rewind seek the first key in the iterator.
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
}

// Seek move the iterator to the key which is
// greater(less when reverse is true) than or equal to the specified key.
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
}

// Next moves the iterator to the next key.
func (it *Iterator) Next() {
	it.indexIter.Next()
}

// Key get the current key.
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value get the current value.
func (it *Iterator) Value() ([]byte, error) {
	// we can get the value from the WAL by the position.
	position := it.indexIter.Value()
	chunk, err := it.db.dataFiles.Read(position)
	if err != nil {
		return nil, err
	}

	record := decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return record.Value, nil
}

// Valid returns whether the iterator is exhausted.
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Close the iterator.
func (it *Iterator) Close() {
	it.indexIter.Close()
}

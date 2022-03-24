package rosedb

import (
	"github.com/flower-corp/rosedb/ds/list"
	"github.com/flower-corp/rosedb/logfile"
)

// LPush insert all the specified values at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
func (db *RoseDB) LPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	for _, val := range values {
		db.listIndex.indexes.LPush(key, val)
		listKey := list.EncodeCommandKey(key, list.LPush)
		entry := &logfile.LogEntry{Key: listKey, Value: val}
		if _, err := db.writeLogEntry(entry, List); err != nil {
			return err
		}
	}
	return nil
}

// RPush insert all the specified values at the tail of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operation.
func (db *RoseDB) RPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	for _, val := range values {
		db.listIndex.indexes.RPush(key, val)
		listKey := list.EncodeCommandKey(key, list.RPush)
		entry := &logfile.LogEntry{Key: listKey, Value: val}
		if _, err := db.writeLogEntry(entry, List); err != nil {
			return err
		}
	}
	return nil
}

// LPop removes and returns the first elements of the list stored at key.
func (db *RoseDB) LPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	val := db.listIndex.indexes.LPop(key)
	if val == nil {
		return nil, nil
	}

	listKey := list.EncodeCommandKey(key, list.LPop)
	entry := &logfile.LogEntry{Key: listKey, Type: logfile.TypeDelete}
	if _, err := db.writeLogEntry(entry, List); err != nil {
		return nil, err
	}
	return val, nil
}

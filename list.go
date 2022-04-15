package rosedb

import (
	"github.com/flower-corp/rosedb/ds/list"
	"github.com/flower-corp/rosedb/logfile"
	"strconv"
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

// RPop Removes and returns the last elements of the list stored at key.
func (db *RoseDB) RPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	val := db.listIndex.indexes.RPop(key)
	if val == nil {
		return nil, nil
	}

	listKey := list.EncodeCommandKey(key, list.RPop)
	entry := &logfile.LogEntry{Key: listKey, Type: logfile.TypeDelete}
	if _, err := db.writeLogEntry(entry, List); err != nil {
		return nil, err
	}
	return val, nil
}

// LIndex returns the element at index index in the list stored at key.
// The index is zero-based, so 0 means the first element, 1 the second element and so on.
// Negative indices can be used to designate elements starting at the tail of the list.
// For example: -1 means the last element, -2 means the penultimate and so forth.
func (db *RoseDB) LIndex(key []byte, index int) []byte {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()
	return db.listIndex.indexes.LIndex(key, index)
}

// LSet sets the list element at index to element.
// returns whether is successful.
func (db *RoseDB) LSet(key []byte, index int, value []byte) (bool, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	i := strconv.Itoa(index)
	encKey := db.encodeKey(key, []byte(i))
	commandKey := list.EncodeCommandKey(encKey, list.LSet)
	entry := &logfile.LogEntry{Key: commandKey, Value: value}
	if _, err := db.writeLogEntry(entry, List); err != nil {
		return false, err
	}
	ok := db.listIndex.indexes.LSet(key, index, value)
	return ok, nil
}

// LLen returns the length of the list stored at key.
// If key does not exist, it is interpreted as an empty list and 0 is returned.
func (db *RoseDB) LLen(key []byte) uint32 {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()
	return db.listIndex.indexes.LLen(key)
}

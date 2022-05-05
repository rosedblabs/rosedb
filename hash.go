package rosedb

import (
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
)

// HSet sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
// If field already exists in the hash, it is overwritten.
// Return num of elements in hash of the specified key.
func (db *RoseDB) HSet(key, field, value []byte) error {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	hashKey := db.encodeKey(key, field)
	ent := &logfile.LogEntry{Key: hashKey, Value: value}
	valuePos, err := db.writeLogEntry(ent, Hash)
	if err != nil {
		return err
	}

	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}
	db.hashIndex.idxTree = db.hashIndex.trees[string(key)]
	entry := &logfile.LogEntry{Key: field, Value: value}
	_, size := logfile.EncodeEntry(ent)
	valuePos.entrySize = size
	return db.updateIndexTree(entry, valuePos, true, Hash)
}

// HGet returns the value associated with field in the hash stored at key.
func (db *RoseDB) HGet(key, field []byte) ([]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.hashIndex.trees[string(key)] == nil {
		return nil, nil
	}
	db.hashIndex.idxTree = db.hashIndex.trees[string(key)]
	val, err := db.getVal(field, Hash)
	if err == ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

// HDel removes the specified fields from the hash stored at key.
// Specified fields that do not exist within this hash are ignored.
// If key does not exist, it is treated as an empty hash and this command returns false.
func (db *RoseDB) HDel(key []byte, fields ...[]byte) (int, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if db.hashIndex.trees[string(key)] == nil {
		return 0, nil
	}
	db.hashIndex.idxTree = db.hashIndex.trees[string(key)]

	var count int
	for _, field := range fields {
		hashKey := db.encodeKey(key, field)
		entry := &logfile.LogEntry{Key: hashKey, Type: logfile.TypeDelete}
		valuePos, err := db.writeLogEntry(entry, Hash)
		if err != nil {
			return 0, err
		}

		val, updated := db.hashIndex.idxTree.Delete(field)
		if updated {
			count++
		}
		db.sendDiscard(val, updated, Hash)
		// The deleted entry itself is also invalid.
		_, size := logfile.EncodeEntry(entry)
		node := &indexNode{fid: valuePos.fid, entrySize: size}
		select {
		case db.discards[Hash].valChan <- node:
		default:
			logger.Warn("send to discard chan fail")
		}
	}
	return count, nil
}

// HExists returns whether the field exists in the hash stored at key.
// If the hash contains field, it returns true.
// If the hash does not contain field, or key does not exist, it returns false.
func (db *RoseDB) HExists(key, field []byte) (bool, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.hashIndex.trees[string(key)] == nil {
		return false, nil
	}
	db.hashIndex.idxTree = db.hashIndex.trees[string(key)]
	val, err := db.getVal(field, Hash)
	if err != nil && err != ErrKeyNotFound {
		return false, err
	}
	return val != nil, nil
}

// HLen returns the number of fields contained in the hash stored at key.
func (db *RoseDB) HLen(key []byte) int {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.hashIndex.trees[string(key)] == nil {
		return 0
	}
	db.hashIndex.idxTree = db.hashIndex.trees[string(key)]
	return db.hashIndex.idxTree.Size()
}

// HKeys returns all field names in the hash stored at key.
func (db *RoseDB) HKeys(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	var keys [][]byte
	tree, ok := db.hashIndex.trees[string(key)]
	if !ok {
		return keys, nil
	}
	iter := tree.Iterator()
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		keys = append(keys, node.Key())
	}
	return keys, nil
}

// HVals return all values in the hash stored at key.
func (db *RoseDB) HVals(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	var values [][]byte
	tree, ok := db.hashIndex.trees[string(key)]
	if !ok {
		return values, nil
	}
	db.hashIndex.idxTree = tree

	iter := tree.Iterator()
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		val, err := db.getVal(node.Key(), Hash)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

// HGetAll return all fields and values of the hash stored at key.
func (db *RoseDB) HGetAll(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	tree, ok := db.hashIndex.trees[string(key)]
	if !ok {
		return [][]byte{}, nil
	}
	db.hashIndex.idxTree = tree

	var index int
	pairs := make([][]byte, tree.Size()*2)
	iter := tree.Iterator()
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		field := node.Key()
		val, err := db.getVal(field, Hash)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		pairs[index] = field
		pairs[index+1] = val
		index += 2
	}
	return pairs[:index], nil
}

// HStrLen returns the string length of the value associated with field in the hash stored at key.
// If the key or the field do not exist, 0 is returned.
func (db *RoseDB) HStrLen(key, field []byte) int {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.hashIndex.trees[string(key)] == nil {
		return 0
	}
	db.hashIndex.idxTree = db.hashIndex.trees[string(key)]
	val, err := db.getVal(field, Hash)
	if err == ErrKeyNotFound {
		return 0
	}
	return len(val)
}

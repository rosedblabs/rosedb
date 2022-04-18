package rosedb

import (
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

	err = db.updateIndexTree(ent, valuePos, true, Hash)
	return nil
}

// HGet returns the value associated with field in the hash stored at key.
func (db *RoseDB) HGet(key, field []byte) ([]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	hashKey := db.encodeKey(key, field)
	val, err := db.getVal(hashKey, Hash)
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

	var count int
	for _, field := range fields {
		hashKey := db.encodeKey(key, field)
		entry := &logfile.LogEntry{Key: hashKey, Type: logfile.TypeDelete}
		valuePos, err := db.writeLogEntry(entry, Hash)
		if err != nil {
			return 0, err
		}

		val, updated := db.hashIndex.idxTree.Delete(hashKey)
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

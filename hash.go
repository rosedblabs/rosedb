package rosedb

import (
	"bytes"
	"errors"
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"github.com/flower-corp/rosedb/util"
	"math"
	"regexp"
	"strconv"
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

// HSetNX sets the given value only if the field doesn't exist.
// If the key doesn't exist, new hash is created.
// If field already exist, HSetNX doesn't have side effect.
func (db *RoseDB) HSetNX(key, field, value []byte) (bool, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}
	db.hashIndex.idxTree = db.hashIndex.trees[string(key)]
	val, err := db.getVal(field, Hash)
	// field exists in db
	if val != nil {
		return false, nil
	}
	hashKey := db.encodeKey(key, field)
	ent := &logfile.LogEntry{Key: hashKey, Value: value}
	valuePos, err := db.writeLogEntry(ent, Hash)
	if err != nil {
		return false, err
	}

	db.hashIndex.idxTree = db.hashIndex.trees[string(key)]
	entry := &logfile.LogEntry{Key: field, Value: value}
	_, size := logfile.EncodeEntry(ent)
	valuePos.entrySize = size
	err = db.updateIndexTree(entry, valuePos, true, Hash)
	if err != nil {
		return false, err
	}
	return true, nil
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

// HMGet returns the values associated with the specified fields in the hash stored at the key.
// For every field that does not exist in the hash, a nil value is returned.
// Because non-existing keys are treated as empty hashes,
// running HMGET against a non-existing key will return a list of nil values.
func (db *RoseDB) HMGet(key []byte, field ...[]byte) (vals [][]byte, err error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	length := len(field)
	// key not exist
	if db.hashIndex.trees[string(key)] == nil {
		for i := 0; i < length; i++ {
			vals = append(vals, nil)
		}
		return vals, nil
	}
	// key exist
	db.hashIndex.idxTree = db.hashIndex.trees[string(key)]

	for _, v := range field {
		val, err := db.getVal(v, Hash)
		if err == ErrKeyNotFound {
			vals = append(vals, nil)
		} else {
			vals = append(vals, val)
		}
	}
	return
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

func (db *RoseDB) HScan(key []byte, prefix []byte, pattern string, count int) ([][]byte, error) {
	if count <= 0 {
		return nil, nil
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	if db.hashIndex.trees[string(key)] == nil {
		return nil, nil
	}
	db.hashIndex.idxTree = db.hashIndex.trees[string(key)]
	fields := db.hashIndex.idxTree.PrefixScan(prefix, count)
	if len(fields) == 0 {
		return nil, nil
	}

	var reg *regexp.Regexp
	if pattern != "" {
		var err error
		if reg, err = regexp.Compile(pattern); err != nil {
			return nil, err
		}
	}

	values := make([][]byte, 2*len(fields))
	var index int
	for _, field := range fields {
		if reg != nil && !reg.Match(field) {
			continue
		}
		val, err := db.getVal(field, Hash)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		values[index], values[index+1] = field, val
		index += 2
	}
	return values, nil
}

// HIncrby increments the number stored at field in the hash stored at key by increment.
// If key does not exist, a new key holding a hash is created. If field does not exist
// the value is set to 0 before the operation is performed. The range of values supported
//  by HINCRBY is limited to 64bit signed integers.
func (db *RoseDB) HIncrBy(key, field []byte, incr int64) (int64, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}

	db.hashIndex.idxTree = db.hashIndex.trees[string(key)]
	val, err := db.getVal(field, Hash)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	if bytes.Equal(val, nil) {
		val = []byte("0")
	}
	valInt64, err := util.StrToInt64(string(val))
	if err != nil {
		return 0, ErrWrongValueType
	}

	if (incr < 0 && valInt64 < 0 && incr < (math.MinInt64-valInt64)) ||
		(incr > 0 && valInt64 > 0 && incr > (math.MaxInt64-valInt64)) {
		return 0, ErrIntegerOverflow
	}

	valInt64 += incr
	val = []byte(strconv.FormatInt(valInt64, 10))

	hashKey := db.encodeKey(key, field)
	ent := &logfile.LogEntry{Key: hashKey, Value: val}
	valuePos, err := db.writeLogEntry(ent, Hash)
	if err != nil {
		return 0, err
	}

	entry := &logfile.LogEntry{Key: field, Value: val}
	_, size := logfile.EncodeEntry(ent)
	valuePos.entrySize = size
	err = db.updateIndexTree(entry, valuePos, true, Hash)
	if err != nil {
		return 0, err
	}
	return valInt64, nil
}

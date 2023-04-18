package rosedb

import (
	"bytes"
	"errors"
	"math"
	"regexp"
	"strconv"
	"time"

	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"github.com/flower-corp/rosedb/util"
)

// Set set key to hold the string value. If key already holds a value, it is overwritten.
// Any previous time to live associated with the key is discarded on successful Set operation.
func (db *RoseDB) Set(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// write entry to log file.
	entry := &logfile.LogEntry{Key: key, Value: value}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	// set String index info, stored at adaptive radix tree.
	err = db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
	return err
}

// Get get the value of key.
// If the key does not exist the error ErrKeyNotFound is returned.
func (db *RoseDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getVal(db.strIndex.idxTree, key, String)
}

// MGet get the values of all specified keys.
// If the key that does not hold a string value or does not exist, nil is returned.
func (db *RoseDB) MGet(keys [][]byte) ([][]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}
	values := make([][]byte, len(keys))
	for i, key := range keys {
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		if err != nil && !errors.Is(ErrKeyNotFound, err) {
			return nil, err
		}
		values[i] = val
	}
	return values, nil
}

// GetRange returns the substring of the string value stored at key,
// determined by the offsets start and end.
func (db *RoseDB) GetRange(key []byte, start, end int) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return []byte{}, nil
	}
	// Negative offsets can be used in order to provide an offset starting from the end of the string.
	// So -1 means the last character, -2 the penultimate and so forth
	if start < 0 {
		start = len(val) + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = len(val) + end
		if end < 0 {
			end = 0
		}
	}

	// handles out of range requests by limiting the resulting range to the actual length of the string.
	if end > len(val)-1 {
		end = len(val) - 1
	}
	if start > len(val)-1 || start > end {
		return []byte{}, nil
	}
	return val[start : end+1], nil
}

// GetDel gets the value of the key and deletes the key. This method is similar
// to Get method. It also deletes the key if it exists.
func (db *RoseDB) GetDel(key []byte) ([]byte, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && err != ErrKeyNotFound {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	entry := &logfile.LogEntry{Key: key, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return nil, err
	}

	oldVal, updated := db.strIndex.idxTree.Delete(key)
	db.sendDiscard(oldVal, updated, String)
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discards[String].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	return val, nil
}

// Delete value at the given key.
func (db *RoseDB) Delete(key []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	entry := &logfile.LogEntry{Key: key, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	val, updated := db.strIndex.idxTree.Delete(key)
	db.sendDiscard(val, updated, String)
	// The deleted entry itself is also invalid.
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discards[String].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	return nil
}

// SetEX set key to hold the string value and set key to timeout after the given duration.
func (db *RoseDB) SetEX(key, value []byte, duration time.Duration) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	expiredAt := time.Now().Add(duration).Unix()
	entry := &logfile.LogEntry{Key: key, Value: value, ExpiredAt: expiredAt}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}

	return db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
}

// SetNX sets the key-value pair if it is not exist. It returns nil if the key already exists.
func (db *RoseDB) SetNX(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	// Key exists in db.
	if val != nil {
		return nil
	}

	entry := &logfile.LogEntry{Key: key, Value: value}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}

	return db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
}

// MSet is multiple set command. Parameter order should be like "key", "value", "key", "value", ...
func (db *RoseDB) MSet(args ...[]byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	if len(args) == 0 || len(args)%2 != 0 {
		return ErrWrongNumberOfArgs
	}

	// Add multiple key-value pairs.
	for i := 0; i < len(args); i += 2 {
		key, value := args[i], args[i+1]
		entry := &logfile.LogEntry{Key: key, Value: value}
		valuePos, err := db.writeLogEntry(entry, String)
		if err != nil {
			return err
		}
		err = db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
		if err != nil {
			return err
		}
	}
	return nil
}

// MSetNX sets given keys to their respective values. MSetNX will not perform
// any operation at all even if just a single key already exists.
func (db *RoseDB) MSetNX(args ...[]byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	if len(args) == 0 || len(args)%2 != 0 {
		return ErrWrongNumberOfArgs
	}

	// Firstly, check each keys whether they are exists.
	for i := 0; i < len(args); i += 2 {
		key := args[i]
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}

		// Key exists in db. We discard the rest of the key-value pairs. It
		// provides the atomicity of the method.
		if val != nil {
			return nil
		}
	}

	var addedKeys = make(map[uint64]struct{})
	// Set keys to their values.
	for i := 0; i < len(args); i += 2 {
		key, value := args[i], args[i+1]
		h := util.MemHash(key)
		if _, ok := addedKeys[h]; ok {
			continue
		}
		entry := &logfile.LogEntry{Key: key, Value: value}
		valPos, err := db.writeLogEntry(entry, String)
		if err != nil {
			return err
		}
		err = db.updateIndexTree(db.strIndex.idxTree, entry, valPos, true, String)
		if err != nil {
			return err
		}
		addedKeys[h] = struct{}{}
	}
	return nil
}

// Append appends the value at the end of the old value if key already exists.
// It will be similar to Set if key does not exist.
func (db *RoseDB) Append(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	oldVal, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}

	// Key exists in db.
	if oldVal != nil {
		value = append(oldVal, value...)
	}
	// write entry to log file.
	entry := &logfile.LogEntry{Key: key, Value: value}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	err = db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
	return err
}

// Decr decrements the number stored at key by one. If the key does not exist,
// it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after decrementing the value.
func (db *RoseDB) Decr(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, -1)
}

// DecrBy decrements the number stored at key by decr. If the key doesn't
// exist, it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after decrementing the value.
func (db *RoseDB) DecrBy(key []byte, decr int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, -decr)
}

// Incr increments the number stored at key by one. If the key does not exist,
// it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after incrementing the value.
func (db *RoseDB) Incr(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, 1)
}

// IncrBy increments the number stored at key by incr. If the key doesn't
// exist, it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after incrementing the value.
func (db *RoseDB) IncrBy(key []byte, incr int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, incr)
}

// incrDecrBy is a helper method for Incr, IncrBy, Decr, and DecrBy methods. It updates the key by incr.
func (db *RoseDB) incrDecrBy(key []byte, incr int64) (int64, error) {
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	if bytes.Equal(val, nil) {
		val = []byte("0")
	}
	valInt64, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, ErrWrongValueType
	}

	if (incr < 0 && valInt64 < 0 && incr < (math.MinInt64-valInt64)) ||
		(incr > 0 && valInt64 > 0 && incr > (math.MaxInt64-valInt64)) {
		return 0, ErrIntegerOverflow
	}

	valInt64 += incr
	val = []byte(strconv.FormatInt(valInt64, 10))
	entry := &logfile.LogEntry{Key: key, Value: val}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return 0, err
	}
	err = db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
	if err != nil {
		return 0, err
	}
	return valInt64, nil
}

// StrLen returns the length of the string value stored at key. If the key
// doesn't exist, it returns 0.
func (db *RoseDB) StrLen(key []byte) int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return 0
	}
	return len(val)
}

// Count returns the total number of keys of String.
func (db *RoseDB) Count() int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	if db.strIndex.idxTree == nil {
		return 0
	}
	return db.strIndex.idxTree.Size()
}

// Scan iterates over all keys of type String and finds its value.
// Parameter prefix will match key`s prefix, and pattern is a regular expression that also matchs the key.
// Parameter count limits the number of keys, a nil slice will be returned if count is not a positive number.
// The returned values will be a mixed data of keys and values, like [key1, value1, key2, value2, etc...].
func (db *RoseDB) Scan(prefix []byte, pattern string, count int) ([][]byte, error) {
	if count <= 0 {
		return nil, nil
	}

	var reg *regexp.Regexp
	if pattern != "" {
		var err error
		if reg, err = regexp.Compile(pattern); err != nil {
			return nil, err
		}
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	if db.strIndex.idxTree == nil {
		return nil, nil
	}
	keys := db.strIndex.idxTree.PrefixScan(prefix, count)
	if len(keys) == 0 {
		return nil, nil
	}

	var results [][]byte
	for _, key := range keys {
		if reg != nil && !reg.Match(key) {
			continue
		}
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		if err != ErrKeyNotFound {
			results = append(results, key, val)
		}
	}
	return results, nil
}

// Expire set the expiration time for the given key.
func (db *RoseDB) Expire(key []byte, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	db.strIndex.mu.Lock()
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		db.strIndex.mu.Unlock()
		return err
	}
	db.strIndex.mu.Unlock()
	return db.SetEX(key, val, duration)
}

// TTL get ttl(time to live) for the given key.
func (db *RoseDB) TTL(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	node, err := db.getIndexNode(db.strIndex.idxTree, key)
	if err != nil {
		return 0, err
	}
	var ttl int64
	if node.expiredAt != 0 {
		ttl = node.expiredAt - time.Now().Unix()
	}
	return ttl, nil
}

// Persist remove the expiration time for the given key.
func (db *RoseDB) Persist(key []byte) error {
	db.strIndex.mu.Lock()
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		db.strIndex.mu.Unlock()
		return err
	}
	db.strIndex.mu.Unlock()

	return db.Set(key, val)
}

// GetStrsKeys get all stored keys of type String.
func (db *RoseDB) GetStrsKeys() ([][]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	if db.strIndex.idxTree == nil {
		return nil, nil
	}

	var keys [][]byte
	iter := db.strIndex.idxTree.Iterator()
	ts := time.Now().Unix()
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		indexNode, _ := node.Value().(*indexNode)
		if indexNode == nil {
			continue
		}
		if indexNode.expiredAt != 0 && indexNode.expiredAt <= ts {
			continue
		}
		keys = append(keys, node.Key())
	}
	return keys, nil
}

// Cas Compare And Set. If current value of the key is the same as oldValue, set newValue.
// The whole process is concurrency safe.
func (db *RoseDB) Cas(key, oldValue, newValue []byte) (bool, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	curValue, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return false, err
	}

	if !bytes.Equal(oldValue, curValue) {
		return false, nil
	}

	// write entry to log file.
	entry := &logfile.LogEntry{Key: key, Value: newValue}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return false, err
	}

	// set String index info, stored at adaptive radix tree.
	err = db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
	return true, err
}

// Cad Compare And Delete. If current value of the key is the same as delValue, delete the kv.
// The whole process is concurrency safe.
func (db *RoseDB) Cad(key, delValue []byte) (bool, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	curValue, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return false, err
	}

	if !bytes.Equal(curValue, delValue) {
		return false, nil
	}

	entry := &logfile.LogEntry{Key: key, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return false, err
	}

	val, updated := db.strIndex.idxTree.Delete(key)
	db.sendDiscard(val, updated, String)

	// The deleted entry itself is also invalid.
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discards[String].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	return true, nil
}

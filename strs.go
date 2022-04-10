package rosedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"strconv"
	"time"
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
	err = db.updateIndexTree(entry, valuePos, true, String)
	return err
}

// Get get the value of key. If the key does not exist an error is returned.
func (db *RoseDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getVal(key, String)
}

// MGet get the values of all specified keys.
// If the key that does not hold a string value or does not exist, nil is returned.
func (db *RoseDB) MGet(keys [][]byte) ([][]byte, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}

	values := make([][]byte, len(keys))
	for i, key := range keys {
		if val, err := db.getVal(key, String); err != nil && !errors.Is(ErrKeyNotFound, err) {
			return nil, err
		} else {
			values[i] = val
		}
	}
	return values, nil
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
	db.sendDiscard(val, updated)
	// The deleted entry itself is also invalid.
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discard.valChan <- node:
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

	err = db.updateIndexTree(entry, valuePos, true, String)
	return err
}

// SetNX sets the key-value pair if it is not exist. It returns nil if the key already exists.
func (db *RoseDB) SetNX(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	val, err := db.getVal(key, String)
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

	return db.updateIndexTree(entry, valuePos, true, String)
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
		entry := &logfile.LogEntry{
			Key:   key,
			Value: value,
		}
		valuePos, err := db.writeLogEntry(entry, String)
		if err != nil {
			return err
		}
		err = db.updateIndexTree(entry, valuePos, true, String)
		if err != nil {
			return err
		}
	}
	return nil
}

// Append appends the value at the end of the old value if key already exists.
// It will be similar to Set if key does not exist.
func (db *RoseDB) Append(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	oldVal, err := db.getVal(key, String)
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
	err = db.updateIndexTree(entry, valuePos, true, String)
	return err
}

// Decr decrements the number stored at key by one. If the key does not exist,
// it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after decrementing the value.
func (db *RoseDB) Decr(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.decrBy(key, 1)
}

// DecrBy decrements the number stored at key by decr. If the key doesn't
// exist, it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after decrementing the value.
func (db *RoseDB) DecrBy(key []byte, decr int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.decrBy(key, decr)
}

// decrBy is a helper method for Decr and DecrBy methods. It updates the key by decr.
func (db *RoseDB) decrBy(key []byte, decr int64) (int64, error) {
	val, err := db.getVal(key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	if bytes.Equal(val, nil) {
		val = []byte("0")
	}
	valInt64, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, ErrWrongKeyType
	}
	// Checks integer overflow. When the number exceeds math.MinInt64 value,
	// it makes the value math.MaxInt64-remaining.
	if valInt64 < 0 && valInt64-decr > 0 {
		return 0, ErrIntegerOverflow
	}
	valInt64 -= decr
	val = []byte(strconv.FormatInt(valInt64, 10))
	entry := &logfile.LogEntry{Key: key, Value: val}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return 0, err
	}
	err = db.updateIndexTree(entry, valuePos, true, String)
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

	val, err := db.getVal(key, String)
	if err != nil {
		return 0
	}

	return binary.Size(val)
}

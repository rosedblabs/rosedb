package rosedb

import (
	"bytes"
	"errors"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
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
	err = db.updateStrIndex(entry, valuePos, true)
	return err
}

// Get get the value of key. If the key does not exist an error is returned.
func (db *RoseDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getVal(key)
}

// MGet get the values of all specified keys.
// If the key that does not hold a string value or does not exist, nil is returned.
func (db *RoseDB) MGet(keys [][]byte) ([][]byte, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	values := make([][]byte, len(keys))
	for i, key := range keys {
		if val, err := db.getVal(key); err != nil && !errors.Is(ErrKeyNotFound, err) {
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

	err = db.updateStrIndex(entry, valuePos, true)
	return err
}

// SetNX sets the key-value pair if it is not exist. It returns nil if the key already exists.
func (db *RoseDB) SetNX(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	val, err := db.getVal(key)
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

	return db.updateStrIndex(entry, valuePos, true)
}

// MSet is multiple set command. Parameter order should be like "key", "value",
// "key", "value", ...
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
		err = db.updateStrIndex(entry, valuePos, true)
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
		val, _ := db.getVal(key)

		// Key exists in db. We discard the rest of the key-value pairs. It
		// provides the atomicity of the method.
		if val != nil {
			return nil
		}
	}

	var addedKeys [][]byte
	// Set keys to their values.
	for i := 0; i < len(args); i += 2 {
		key, value := args[i], args[i+1]
		if addedBefore(key, addedKeys) {
			continue
		}
		entry := &logfile.LogEntry{Key: key, Value: value}
		valPos, err := db.writeLogEntry(entry, String)
		if err != nil {
			return err
		}
		err = db.updateStrIndex(entry, valPos, true)
		if err != nil {
			return err
		}
		addedKeys = append(addedKeys, key)
	}
	return nil
}

// addedBefore is a helper function that controls the key was set before.
func addedBefore(key []byte, addedKeys [][]byte) bool {
	for _, k := range addedKeys {
		if bytes.Equal(k, key) {
			return true
		}
	}
	return false
}

func (db *RoseDB) updateStrIndex(ent *logfile.LogEntry, pos *valuePos, sendDiscard bool) error {
	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	// in KeyValueMemMode, both key and value will store in memory.
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	oldVal, updated := db.strIndex.idxTree.Put(ent.Key, idxNode)
	if sendDiscard {
		db.sendDiscard(oldVal, updated)
	}
	return nil
}

func (db *RoseDB) getVal(key []byte) ([]byte, error) {
	// Get index info from a skip list in memory.
	rawValue := db.strIndex.idxTree.Get(key)
	if rawValue == nil {
		return nil, ErrKeyNotFound
	}
	idxNode, _ := rawValue.(*indexNode)
	if idxNode == nil {
		return nil, ErrKeyNotFound
	}

	ts := time.Now().Unix()
	if idxNode.expiredAt != 0 && idxNode.expiredAt <= ts {
		return nil, ErrKeyNotFound
	}
	// In KeyValueMemMode, the value will be stored in memory.
	// So get the value from the index info.
	if db.opts.IndexMode == KeyValueMemMode && len(idxNode.value) != 0 {
		return idxNode.value, nil
	}

	// In KeyOnlyMemMode, the value not in memory, so get the value from log file at the offset.
	logFile := db.getActiveLogFile(String)
	if logFile.Fid != idxNode.fid {
		logFile = db.getArchivedLogFile(String, idxNode.fid)
	}
	if logFile == nil {
		return nil, ErrLogFileNotFound
	}

	ent, _, err := logFile.ReadLogEntry(idxNode.offset)
	if err != nil {
		return nil, err
	}
	// key exists, but is invalid(deleted or expired)
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		return nil, ErrKeyNotFound
	}
	return ent.Value, nil
}

func (db *RoseDB) sendDiscard(oldVal interface{}, updated bool) {
	if !updated || oldVal == nil {
		return
	}
	node, _ := oldVal.(*indexNode)
	if node == nil || node.entrySize <= 0 {
		return
	}
	select {
	case db.discard.valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
}

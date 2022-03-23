package rosedb

import (
	"github.com/flower-corp/rosedb/logfile"
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
	err = db.updateStrIndex(entry, valuePos)
	return err
}

// Get get the value of key. If the key does not exist an error is returned.
func (db *RoseDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getVal(key)
}

// Delete value at the given key.
func (db *RoseDB) Delete(key []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	entry := &logfile.LogEntry{Key: key, Type: logfile.TypeDelete}
	if _, err := db.writeLogEntry(entry, String); err != nil {
		return err
	}
	db.strIndex.idxTree.Delete(key)
	return nil
}

func (db *RoseDB) updateStrIndex(ent *logfile.LogEntry, pos *valuePos) error {
	idxNode := &strIndexNode{fid: pos.fid, offset: pos.offset}
	// in KeyValueMemMode, both key and value will store in memory.
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	db.strIndex.idxTree.Put(ent.Key, idxNode)
	return nil
}

func (db *RoseDB) getVal(key []byte) ([]byte, error) {
	// Get index info from a skip list in memory.
	rawValue := db.strIndex.idxTree.Get(key)
	if rawValue == nil {
		return nil, ErrKeyNotFound
	}
	idxNode, _ := rawValue.(*strIndexNode)
	if idxNode == nil {
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
	ts := time.Now().Unix()
	// key exists, but is invalid(deleted or expired)
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		return nil, ErrKeyNotFound
	}
	return ent.Value, nil
}

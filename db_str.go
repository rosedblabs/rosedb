package rosedb

import (
	"bytes"
	"github.com/roseduan/rosedb/index"
	"github.com/roseduan/rosedb/storage"
	"strings"
	"sync"
	"time"
)

// StrIdx string index.
type StrIdx struct {
	mu      *sync.RWMutex
	idxList *index.SkipList
}

func newStrIdx() *StrIdx {
	return &StrIdx{
		idxList: index.NewSkipList(), mu: new(sync.RWMutex),
	}
}

// Set set key to hold the string value. If key already holds a value, it is overwritten.
// Any previous time to live associated with the key is discarded on successful Set operation.
func (db *RoseDB) Set(key, value []byte) error {
	return db.setVal(key, value)
}

// SetNx is short for "Set if not exists", set key to hold string value if key does not exist.
// In that case, it is equal to Set. When key already holds a value, no operation is performed.
func (db *RoseDB) SetNx(key, value []byte) (res uint32, err error) {
	if exist := db.StrExists(key); exist {
		return
	}

	if err = db.Set(key, value); err == nil {
		res = 1
	}
	return
}

// SetEx set key to hold the string value and set key to timeout after a given number of seconds.
func (db *RoseDB) SetEx(key, value []byte, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(key, value, deadline, String, StringExpire)
	if err = db.store(e); err != nil {
		return
	}

	// set String index info, stored at skip list.
	db.setIndexer(e)
	// set expired info.
	db.expires[String][string(key)] = deadline
	return
}

// Get get the value of key. If the key does not exist an error is returned.
func (db *RoseDB) Get(key []byte) ([]byte, error) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil, err
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	return db.getVal(key)
}

// GetSet set key to value and returns the old value stored at key.
// If the key not exist, return an err.
func (db *RoseDB) GetSet(key, val []byte) (res []byte, err error) {
	res, err = db.Get(key)
	if err != nil && err != ErrKeyNotExist {
		return
	}
	if err = db.Set(key, val); err != nil {
		return
	}
	return
}

// Append if key already exists and is a string, this command appends the value at the end of the string.
// If key does not exist it is created and set as an empty string, so Append will be similar to Set in this special case.
func (db *RoseDB) Append(key, value []byte) error {
	if err := db.checkKeyValue(key, value); err != nil {
		return err
	}
	existVal, err := db.Get(key)
	if err != nil && err != ErrKeyNotExist && err != ErrKeyExpired {
		return err
	}

	if len(existVal) > 0 {
		existVal = append(existVal, value...)
	} else {
		existVal = value
	}
	return db.setVal(key, existVal)
}

// StrLen returns the length of the string value stored at key.
func (db *RoseDB) StrLen(key []byte) int {
	val, _ := db.getVal(key)
	return len(val)
}

// StrExists check whether the key exists.
func (db *RoseDB) StrExists(key []byte) bool {
	if err := db.checkKeyValue(key, nil); err != nil {
		return false
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	exist := db.strIndex.idxList.Exist(key)
	if exist && !db.checkExpired(key, String) {
		return true
	}
	return false
}

// StrRem remove the value stored at key.
func (db *RoseDB) StrRem(key []byte) error {
	if err := db.checkKeyValue(key, nil); err != nil {
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, nil, String, StringRem)
	if err := db.store(e); err != nil {
		return err
	}

	db.strIndex.idxList.Remove(key)
	delete(db.expires[String], string(key))
	return nil
}

// PrefixScan find the value corresponding to all matching keys based on the prefix.
// limit and offset control the range of value.
// if limit is negative, all matched values will return.
func (db *RoseDB) PrefixScan(prefix string, limit, offset int) (val [][]byte, err error) {
	if limit == 0 {
		return
	}
	if offset < 0 {
		offset = 0
	}
	if err = db.checkKeyValue([]byte(prefix), nil); err != nil {
		return
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	// Find the first matched key of the prefix.
	e := db.strIndex.idxList.FindPrefix([]byte(prefix))
	if limit > 0 {
		for i := 0; i < offset && e != nil && strings.HasPrefix(string(e.Key()), prefix); i++ {
			e = e.Next()
		}
	}

	for e != nil && strings.HasPrefix(string(e.Key()), prefix) && limit != 0 {
		item := e.Value().(*index.Indexer)
		var value []byte

		if db.config.IdxMode == KeyOnlyMemMode {
			value, err = db.Get(e.Key())
			if err != nil {
				return
			}
		} else {
			if item != nil {
				value = item.Meta.Value
			}
		}

		// Check if the key is expired.
		expired := db.checkExpired(e.Key(), String)
		if !expired {
			val = append(val, value)
			e = e.Next()
		}
		if limit > 0 && !expired {
			limit--
		}
	}
	return
}

// RangeScan find range of values from start to end.
func (db *RoseDB) RangeScan(start, end []byte) (val [][]byte, err error) {
	node := db.strIndex.idxList.Get(start)

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	for node != nil && bytes.Compare(node.Key(), end) <= 0 {
		if db.checkExpired(node.Key(), String) {
			node = node.Next()
			continue
		}

		var value []byte
		if db.config.IdxMode == KeyOnlyMemMode {
			value, err = db.Get(node.Key())
			if err != nil && err != ErrKeyNotExist {
				return nil, err
			}
		} else {
			value = node.Value().(*index.Indexer).Meta.Value
		}

		val = append(val, value)
		node = node.Next()
	}
	return
}

// Expire set the expiration time of the key.
func (db *RoseDB) Expire(key []byte, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	var value []byte
	if value, err = db.getVal(key); err != nil {
		return
	}

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(key, value, deadline, String, StringExpire)
	if err = db.store(e); err != nil {
		return err
	}

	db.expires[String][string(key)] = deadline
	return
}

// Persist clear expiration time.
func (db *RoseDB) Persist(key []byte) (err error) {
	val, err := db.Get(key)
	if err != nil {
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, val, String, StringPersist)
	if err = db.store(e); err != nil {
		return
	}

	delete(db.expires[String], string(key))
	return
}

// TTL Time to live.
func (db *RoseDB) TTL(key []byte) (ttl int64) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	deadline, exist := db.expires[String][string(key)]
	if !exist {
		return
	}
	if expired := db.checkExpired(key, String); expired {
		return
	}

	return deadline - time.Now().Unix()
}

func (db *RoseDB) setVal(key, value []byte) (err error) {
	if err = db.checkKeyValue(key, value); err != nil {
		return err
	}

	// If the existed value is the same as the set value, nothing will be done.
	if db.config.IdxMode == KeyValueMemMode {
		if existVal, _ := db.Get(key); existVal != nil && bytes.Compare(existVal, value) == 0 {
			return
		}
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, value, String, StringSet)
	if err := db.store(e); err != nil {
		return err
	}

	// clear expire time.
	if _, ok := db.expires[String][string(key)]; ok {
		delete(db.expires[String], string(key))
	}
	// set String index info, stored at skip list.
	db.setIndexer(e)
	return
}

func (db *RoseDB) setIndexer(e *storage.Entry) {
	// string indexes, stored in skiplist.
	idx := &index.Indexer{
		Meta: &storage.Meta{
			Key: e.Meta.Key,
		},
		FileId: db.activeFileIds[String],
		Offset: db.activeFile[String].Offset - int64(e.Size()),
	}

	// in KeyValueMemMode, both key and value will store in memory.
	if db.config.IdxMode == KeyValueMemMode {
		idx.Meta.Value = e.Meta.Value
	}
	db.strIndex.idxList.Put(idx.Meta.Key, idx)
	return
}

func (db *RoseDB) getVal(key []byte) ([]byte, error) {
	// Get index info from a skip list in memory.
	node := db.strIndex.idxList.Get(key)
	if node == nil {
		return nil, ErrKeyNotExist
	}

	idx := node.Value().(*index.Indexer)
	if idx == nil {
		return nil, ErrNilIndexer
	}

	// Check if the key is expired.
	if db.checkExpired(key, String) {
		return nil, ErrKeyExpired
	}

	// In KeyValueMemMode, the value will be stored in memory.
	// So get the value from the index info.
	if db.config.IdxMode == KeyValueMemMode {
		return idx.Meta.Value, nil
	}

	// In KeyOnlyMemMode, the value not in memory.
	// So get the value from the db file at the offset.
	if db.config.IdxMode == KeyOnlyMemMode {
		df := db.activeFile[String]
		if idx.FileId != db.activeFileIds[String] {
			df = db.archFiles[String][idx.FileId]
		}

		e, err := df.Read(idx.Offset)
		if err != nil {
			return nil, err
		}
		return e.Meta.Value, nil
	}
	return nil, ErrKeyNotExist
}

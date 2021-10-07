package rosedb

import (
	"bytes"
	"strings"
	"sync"
	"time"

	"github.com/roseduan/rosedb/index"
	"github.com/roseduan/rosedb/storage"
	"github.com/roseduan/rosedb/utils"
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
func (db *RoseDB) Set(key, value interface{}) error {
	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return err
	}
	return db.setVal(encKey, encVal)
}

// SetNx is short for "Set if not exists", set key to hold string value if key does not exist.
// In that case, it is equal to Set. When key already holds a value, no operation is performed.
func (db *RoseDB) SetNx(key, value interface{}) (ok bool, err error) {
	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return false, err
	}
	if exist := db.StrExists(encKey); exist {
		return
	}

	if err = db.Set(encKey, encVal); err == nil {
		ok = true
	}
	return
}

// SetEx set key to hold the string value and set key to timeout after a given number of seconds.
func (db *RoseDB) SetEx(key, value interface{}, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}

	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(encKey, encVal, deadline, String, StringExpire)
	if err = db.store(e); err != nil {
		return
	}

	// set String index info, stored at skip list.
	if err = db.setIndexer(e); err != nil {
		return
	}
	// set expired info.
	db.expires[String][string(encKey)] = deadline
	return
}

// Get get the value of key. If the key does not exist an error is returned.
func (db *RoseDB) Get(key, dest interface{}) error {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return err
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	val, err := db.getVal(encKey)
	if err != nil {
		return err
	}

	if len(val) > 0 {
		err = utils.DecodeValue(val, dest)
	}
	return err
}

// GetSet set key to value and returns the old value stored at key.
// If the key not exist, return an err.
func (db *RoseDB) GetSet(key, value, dest interface{}) (err error) {
	err = db.Get(key, dest)
	if err != nil && err != ErrKeyNotExist && err != ErrKeyExpired {
		return
	}
	return db.Set(key, value)
}

// MSet set multiple keys to multiple values
func (db *RoseDB) MSet(values ...interface{}) error {
	if len(values)%2 != 0 {
		return ErrWrongNumberOfArgs
	}

	keys := make([][]byte, 0)
	vals := make([][]byte, 0)

	if db.config.IdxMode == KeyValueMemMode {
		for i := 0; i < len(values); i += 2 {
			encKey, encVal, err := db.encode(values[i], values[i+1])
			if err != nil {
				return err
			}

			if err := db.checkKeyValue(encKey, encVal); err != nil {
				return err
			}

			existVal, err := db.getVal(encKey)
			if err != nil && err != ErrKeyExpired && err != ErrKeyNotExist {
				return err
			}

			// if the existed value is the same as the set value, pass this key and value
			if bytes.Compare(existVal, encVal) != 0 {
				keys = append(keys, encKey)
				vals = append(vals, encVal)
			}
		}
	} else {
		for i := 0; i < len(values); i += 2 {
			encKey, encVal, err := db.encode(values[i], values[i+1])
			if err != nil {
				return err
			}

			if err := db.checkKeyValue(encKey, encVal); err != nil {
				return err
			}

			keys = append(keys, encKey)
			vals = append(vals, encVal)
		}
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	for i := 0; i < len(keys); i++ {
		e := storage.NewEntryNoExtra(keys[i], vals[i], String, StringSet)
		if err := db.store(e); err != nil {
			return err
		}

		// clear expire time.
		if _, ok := db.expires[String][string(keys[i])]; ok {
			delete(db.expires[String], string(keys[i]))
		}

		// set String index info, stored at skip list.
		if err := db.setIndexer(e); err != nil {
			return err
		}
	}

	return nil
}

// MGet get the values of all the given keys
func (db *RoseDB) MGet(keys ...interface{}) ([][]byte, error) {
	encKeys := make([][]byte, 0)
	for _, key := range keys {
		encKey, err := utils.EncodeKey(key)
		if err != nil {
			return nil, err
		}

		if err := db.checkKeyValue(encKey, nil); err != nil {
			return nil, err
		}

		encKeys = append(encKeys, encKey)
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	vals := make([][]byte, 0)
	for _, encKey := range encKeys {
		val, err := db.getVal(encKey)
		if err != nil {
			return nil, err
		}

		vals = append(vals, val)
	}

	return vals, nil
}

// Append if key already exists and is a string, this command appends the value at the end of the string.
// If key does not exist it is created and set as an empty string, so Append will be similar to Set in this special case.
func (db *RoseDB) Append(key interface{}, value string) (err error) {
	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return err
	}
	if err := db.checkKeyValue(encKey, encVal); err != nil {
		return err
	}

	var existVal []byte
	err = db.Get(key, &existVal)
	if err != nil && err != ErrKeyNotExist && err != ErrKeyExpired {
		return err
	}

	existVal = append(existVal, []byte(value)...)
	return db.Set(encKey, existVal)
}

// StrExists check whether the key exists.
func (db *RoseDB) StrExists(key interface{}) bool {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return false
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return false
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	exist := db.strIndex.idxList.Exist(encKey)
	if exist && !db.checkExpired(encKey, String) {
		return true
	}
	return false
}

// Remove remove the value stored at key.
func (db *RoseDB) Remove(key interface{}) error {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(encKey, nil, String, StringRem)
	if err := db.store(e); err != nil {
		return err
	}

	db.strIndex.idxList.Remove(encKey)
	delete(db.expires[String], string(encKey))
	return nil
}

// PrefixScan find the value corresponding to all matching keys based on the prefix.
// limit and offset control the range of value.
// if limit is negative, all matched values will return.
func (db *RoseDB) PrefixScan(prefix string, limit, offset int) (val []interface{}, err error) {
	if limit <= 0 {
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
		var value interface{}

		if db.config.IdxMode == KeyOnlyMemMode {
			if err = db.Get(e.Key(), &value); err != nil {
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
func (db *RoseDB) RangeScan(start, end interface{}) (val []interface{}, err error) {
	startKey, err := utils.EncodeKey(start)
	if err != nil {
		return nil, err
	}
	endKey, err := utils.EncodeKey(end)
	if err != nil {
		return nil, err
	}

	node := db.strIndex.idxList.Get(startKey)

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	for node != nil && bytes.Compare(node.Key(), endKey) <= 0 {
		if db.checkExpired(node.Key(), String) {
			node = node.Next()
			continue
		}

		var value interface{}
		if db.config.IdxMode == KeyOnlyMemMode {
			err = db.Get(node.Key(), &value)
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
func (db *RoseDB) Expire(key interface{}, duration int64) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if duration <= 0 {
		return ErrInvalidTTL
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	var value []byte
	if value, err = db.getVal(encKey); err != nil {
		return
	}

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(encKey, value, deadline, String, StringExpire)
	if err = db.store(e); err != nil {
		return err
	}

	db.expires[String][string(encKey)] = deadline
	return
}

// Persist clear expiration time.
func (db *RoseDB) Persist(key interface{}) (err error) {
	var val interface{}
	if err = db.Get(key, &val); err != nil {
		return
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	encKey, encVal, err := db.encode(key, val)
	if err != nil {
		return err
	}
	e := storage.NewEntryNoExtra(encKey, encVal, String, StringPersist)
	if err = db.store(e); err != nil {
		return
	}

	delete(db.expires[String], string(encKey))
	return
}

// TTL Time to live.
func (db *RoseDB) TTL(key interface{}) (ttl int64) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	deadline, exist := db.expires[String][string(encKey)]
	if !exist {
		return
	}
	if expired := db.checkExpired(encKey, String); expired {
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
		var existVal []byte
		existVal, err = db.getVal(key)
		if err != nil && err != ErrKeyExpired && err != ErrKeyNotExist {
			return
		}

		if bytes.Compare(existVal, value) == 0 {
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
	if err = db.setIndexer(e); err != nil {
		return
	}
	return
}

func (db *RoseDB) setIndexer(e *storage.Entry) error {
	activeFile, err := db.getActiveFile(String)
	if err != nil {
		return err
	}
	// string indexes, stored in skiplist.
	idx := &index.Indexer{
		Meta: &storage.Meta{
			Key: e.Meta.Key,
		},
		FileId: activeFile.Id,
		Offset: activeFile.Offset - int64(e.Size()),
	}

	// in KeyValueMemMode, both key and value will store in memory.
	if db.config.IdxMode == KeyValueMemMode {
		idx.Meta.Value = e.Meta.Value
	}
	db.strIndex.idxList.Put(idx.Meta.Key, idx)
	return nil
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
	// So get the value from cache if exists in lru cache.
	// Otherwise, get the value from the db file at the offset.
	if db.config.IdxMode == KeyOnlyMemMode {
		if value, ok := db.cache.Get(key); ok {
			return value, nil
		}

		df, err := db.getActiveFile(String)
		if err != nil {
			return nil, err
		}
		if idx.FileId != df.Id {
			df = db.archFiles[String][idx.FileId]
		}

		e, err := df.Read(idx.Offset)
		if err != nil {
			return nil, err
		}
		value := e.Meta.Value
		db.cache.Set(key, value)
		return value, nil
	}
	return nil, ErrKeyNotExist
}

package rosedb

import (
	"bytes"
	"github.com/roseduan/rosedb/index"
	"github.com/roseduan/rosedb/storage"
	"log"
	"strings"
	"sync"
	"time"
)

// StrIdx string index.
type StrIdx struct {
	mu      sync.RWMutex
	idxList *index.SkipList
}

func newStrIdx() *StrIdx {
	return &StrIdx{idxList: index.NewSkipList()}
}

// Set set key to hold the string value. If key already holds a value, it is overwritten.
// Any previous time to live associated with the key is discarded on successful Set operation.
func (db *RoseDB) Set(key, value []byte) error {
	if err := db.doSet(key, value); err != nil {
		return err
	}

	// Clear the expire time of the key.
	db.Persist(key)
	return nil
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

// Get get the value of key. If the key does not exist an error is returned.
func (db *RoseDB) Get(key []byte) ([]byte, error) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil, err
	}

	// Get index info from a skip list in memory.
	node := db.strIndex.idxList.Get(key)
	if node == nil {
		return nil, ErrKeyNotExist
	}

	idx := node.Value().(*index.Indexer)
	if idx == nil {
		return nil, ErrNilIndexer
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	// Check if the key is expired.
	if db.expireIfNeeded(key) {
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

// GetSet set key to value and returns the old value stored at key.
// If the key not exist, return an err.
func (db *RoseDB) GetSet(key, val []byte) (res []byte, err error) {
	if res, err = db.Get(key); err != nil {
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
	e, err := db.Get(key)
	if err != nil && err != ErrKeyNotExist {
		return err
	}

	// Check if the key is expired.
	if db.expireIfNeeded(key) {
		return ErrKeyExpired
	}

	appendExist := false
	if e != nil {
		appendExist = true
		e = append(e, value...)
	} else {
		e = value
	}

	if err := db.doSet(key, e); err != nil {
		return err
	}
	if !appendExist {
		db.Persist(key)
	}
	return nil
}

// StrLen returns the length of the string value stored at key.
func (db *RoseDB) StrLen(key []byte) int {
	if err := db.checkKeyValue(key, nil); err != nil {
		return 0
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	e := db.strIndex.idxList.Get(key)
	if e != nil {
		if db.expireIfNeeded(key) {
			return 0
		}
		idx := e.Value().(*index.Indexer)
		return int(idx.Meta.ValueSize)
	}

	return 0
}

// StrExists check whether the key exists.
func (db *RoseDB) StrExists(key []byte) bool {
	if err := db.checkKeyValue(key, nil); err != nil {
		return false
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	exist := db.strIndex.idxList.Exist(key)
	if exist && !db.expireIfNeeded(key) {
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

	db.incrReclaimableSpace(key)

	if ele := db.strIndex.idxList.Remove(key); ele != nil {
		delete(db.expires, string(key))
		e := storage.NewEntryNoExtra(key, nil, String, StringRem)
		if err := db.store(e); err != nil {
			return err
		}
	}
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
		expired := db.expireIfNeeded(e.Key())
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
		if db.expireIfNeeded(node.Key()) {
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
func (db *RoseDB) Expire(key []byte, seconds uint32) (err error) {
	if exist := db.StrExists(key); !exist {
		return ErrKeyNotExist
	}
	if seconds <= 0 {
		return ErrInvalidTTL
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	deadline := uint32(time.Now().Unix()) + seconds
	db.expires[string(key)] = deadline
	return
}

// Persist clear expiration time.
func (db *RoseDB) Persist(key []byte) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	delete(db.expires, string(key))
}

// TTL Time to live.
func (db *RoseDB) TTL(key []byte) (ttl uint32) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	if db.expireIfNeeded(key) {
		return
	}
	deadline, exist := db.expires[string(key)]
	if !exist {
		return
	}

	now := uint32(time.Now().Unix())
	if deadline > now {
		ttl = deadline - now
	}
	return
}

// Check whether key is expired and delete it if needed.
func (db *RoseDB) expireIfNeeded(key []byte) (expired bool) {
	deadline := db.expires[string(key)]
	if deadline <= 0 {
		return
	}

	if time.Now().Unix() > int64(deadline) {
		expired = true
		// delete the expire info stored at key.
		delete(db.expires, string(key))

		// delete the index.
		if ele := db.strIndex.idxList.Remove(key); ele != nil {
			// add reclaimable space.
			db.incrReclaimableSpace(key)

			e := storage.NewEntryNoExtra(key, nil, String, StringRem)
			if err := db.store(e); err != nil {
				log.Printf("remove expired key err [%+v] [%+v]\n", key, err)
			}
		}
	}
	return
}

func (db *RoseDB) doSet(key, value []byte) (err error) {
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

	db.incrReclaimableSpace(key)

	// string indexes, stored in skiplist.
	idx := &index.Indexer{
		Meta: &storage.Meta{
			KeySize: uint32(len(e.Meta.Key)),
			Key:     e.Meta.Key,
		},
		FileId:    db.activeFileIds[String],
		EntrySize: e.Size(),
		Offset:    db.activeFile[String].Offset - int64(e.Size()),
	}

	if err = db.buildIndex(e, idx); err != nil {
		return err
	}
	return
}

// Get the original index info and add reclaimable space for the db file.
func (db *RoseDB) incrReclaimableSpace(key []byte) {
	oldIdx := db.strIndex.idxList.Get(key)
	if oldIdx != nil {
		indexer := oldIdx.Value().(*index.Indexer)

		if indexer != nil {
			space := int64(indexer.EntrySize)
			db.meta.ReclaimableSpace[indexer.FileId] += space
		}
	}
}

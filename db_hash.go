package rosedb

import (
	"bytes"
	"github.com/roseduan/rosedb/ds/hash"
	"github.com/roseduan/rosedb/storage"
	"github.com/roseduan/rosedb/utils"
	"sync"
	"time"
)

// HashIdx hash index.
type HashIdx struct {
	mu      *sync.RWMutex
	indexes *hash.Hash
}

// create a new hash index.
func newHashIdx() *HashIdx {
	return &HashIdx{indexes: hash.New(), mu: new(sync.RWMutex)}
}

// HSet sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
// If field already exists in the hash, it is overwritten.
// Return num of elements in hash of the specified key.
func (db *RoseDB) HSet(key interface{}, field []byte, value []byte) (res int, err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return 0, err
	}
	if err = db.checkKeyValue(encKey, value); err != nil {
		return
	}

	// If the existed value is the same as the set value, nothing will be done.
	oldVal := db.HGet(encKey, field)
	if bytes.Compare(oldVal, value) == 0 {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	e := storage.NewEntry(encKey, value, field, Hash, HashHSet)
	if err = db.store(e); err != nil {
		return
	}

	res = db.hashIndex.indexes.HSet(string(encKey), string(field), value)
	return
}

// HSetNx Sets field in the hash stored at key to value, only if field does not yet exist.
// If key does not exist, a new key holding a hash is created. If field already exists, this operation has no effect.
// Return if the operation is successful.
func (db *RoseDB) HSetNx(key interface{}, field, value []byte) (res int, err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return 0, err
	}

	if err = db.checkKeyValue(encKey, value); err != nil {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if res = db.hashIndex.indexes.HSetNx(string(encKey), string(field), value); res == 1 {
		e := storage.NewEntry(encKey, value, field, Hash, HashHSet)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

// HGet returns the value associated with field in the hash stored at key.
func (db *RoseDB) HGet(key interface{}, field []byte) []byte {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return nil
	}

	return db.hashIndex.indexes.HGet(string(encKey), string(field))
}

// HGetAll returns all fields and values of the hash stored at key.
// In the returned value, every field name is followed by its value, so the length of the reply is twice the size of the hash.
func (db *RoseDB) HGetAll(key interface{}) [][]byte {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return nil
	}

	return db.hashIndex.indexes.HGetAll(string(encKey))
}

// HDel removes the specified fields from the hash stored at key.
// Specified fields that do not exist within this hash are ignored.
// If key does not exist, it is treated as an empty hash and this command returns false.
func (db *RoseDB) HDel(key interface{}, field ...[]byte) (res int, err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return 0, err
	}
	if err = db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	if field == nil || len(field) == 0 {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	for _, f := range field {
		if ok := db.hashIndex.indexes.HDel(string(encKey), string(f)); ok == 1 {
			e := storage.NewEntry(encKey, nil, f, Hash, HashHDel)
			if err = db.store(e); err != nil {
				return
			}
			res++
		}
	}
	return
}

// HKeyExists returns if the key is existed in hash.
func (db *RoseDB) HKeyExists(key interface{}) (ok bool) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return false
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return
	}
	return db.hashIndex.indexes.HKeyExists(string(encKey))
}

// HExists returns if field is an existing field in the hash stored at key.
func (db *RoseDB) HExists(key, field []byte) int {
	if err := db.checkKeyValue(key, nil); err != nil {
		return 0
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, Hash) {
		return 0
	}

	return db.hashIndex.indexes.HExists(string(key), string(field))
}

// HLen returns the number of fields contained in the hash stored at key.
func (db *RoseDB) HLen(key interface{}) int {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return 0
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return 0
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return 0
	}

	return db.hashIndex.indexes.HLen(string(encKey))
}

// HKeys returns all field names in the hash stored at key.
func (db *RoseDB) HKeys(key interface{}) (val []string) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return nil
	}

	return db.hashIndex.indexes.HKeys(string(encKey))
}

// HVals returns all values in the hash stored at key.
func (db *RoseDB) HVals(key interface{}) (val [][]byte) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return nil
	}

	return db.hashIndex.indexes.HVals(string(encKey))
}

// HClear clear the key in hash.
func (db *RoseDB) HClear(key interface{}) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if err = db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	if !db.HKeyExists(key) {
		return ErrKeyNotExist
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(encKey, nil, Hash, HashHClear)
	if err := db.store(e); err != nil {
		return err
	}

	db.hashIndex.indexes.HClear(string(encKey))
	delete(db.expires[Hash], string(encKey))
	return
}

// HExpire set expired time for a hash key.
func (db *RoseDB) HExpire(key interface{}, duration int64) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if duration <= 0 {
		return ErrInvalidTTL
	}
	if err = db.checkKeyValue(encKey, nil); err != nil {
		return
	}
	if !db.HKeyExists(key) {
		return ErrKeyNotExist
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(encKey, nil, deadline, Hash, HashHExpire)
	if err := db.store(e); err != nil {
		return err
	}

	db.expires[Hash][string(encKey)] = deadline
	return
}

// HTTL return time to live for the key.
func (db *RoseDB) HTTL(key interface{}) (ttl int64) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return 0
	}
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return
	}

	deadline, exist := db.expires[Hash][string(encKey)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}

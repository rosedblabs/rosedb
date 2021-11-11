package rosedb

import (
	"bytes"
	"github.com/roseduan/rosedb/utils"
	"sync"
	"time"

	"github.com/roseduan/rosedb/ds/hash"
	"github.com/roseduan/rosedb/storage"
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
func (db *RoseDB) HSet(key, field, value interface{}) (res int, err error) {
	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return -1, err
	}
	encField, err := utils.EncodeKey(field)
	if err != nil {
		return -1, err
	}

	if err = db.checkKeyValue(encKey, encVal); err != nil {
		return
	}
	if err = db.checkKeyValue(encField, nil); err != nil {
		return
	}

	// If the existed value is the same as the set value, nothing will be done.
	oldVal := db.HGet(encKey, encField)
	if bytes.Compare(oldVal, encVal) == 0 {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	e := storage.NewEntry(encKey, encVal, encField, Hash, HashHSet)
	if err = db.store(e); err != nil {
		return
	}

	res = db.hashIndex.indexes.HSet(string(encKey), string(encField), encVal)
	return
}

// HSetNx Sets field in the hash stored at key to value, only if field does not yet exist.
// If key does not exist, a new key holding a hash is created. If field already exists, this operation has no effect.
// Return if the operation is successful.
func (db *RoseDB) HSetNx(key, field, value interface{}) (res int, err error) {
	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return -1, err
	}

	encField, err := utils.EncodeKey(field)
	if err != nil {
		return -1, err
	}

	if err = db.checkKeyValue(encKey, encVal); err != nil {
		return
	}

	if err = db.checkKeyValue(encField, nil); err != nil {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if res = db.hashIndex.indexes.HSetNx(string(encKey), string(encField), encVal); res == 1 {
		e := storage.NewEntry(encKey, encVal, encField, Hash, HashHSet)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

// HGet returns the value associated with field in the hash stored at key.
func (db *RoseDB) HGet(key, field interface{}) []byte {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}

	encField, err := utils.EncodeKey(field)
	if err != nil {
		return nil
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return nil
	}

	return db.hashIndex.indexes.HGet(string(encKey), string(encField))
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

// HMSet set multiple hash fields to multiple values
func (db *RoseDB) HMSet(key interface{}, values ...interface{}) error {
	if len(values)%2 != 0 {
		return ErrWrongNumberOfArgs
	}

	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}

	var encVals [][]byte
	for i := 0; i < len(values); i++ {
		eval, err := utils.EncodeValue(values[i])

		if err != nil {
			return err
		}
		if err = db.checkKeyValue(encKey, eval); err != nil {
			return err
		}

		encVals = append(encVals, eval)
	}

	fields := make([]interface{}, 0)
	for i := 0; i < len(values); i += 2 {
		fields = append(fields, encVals[i])
	}

	existVals := db.HMGet(encKey, fields...)

	var encFields [][]byte
	for i := 0; i < len(fields); i++ {
		efields, err := utils.EncodeValue(fields[i])
		if err != nil {
			return err
		}
		if err = db.checkKeyValue(efields, nil); err != nil {
			return err
		}

		encFields = append(encFields, efields)
	}

	// field1 value1 field2 value2 ...
	insertVals := make([][]byte, 0)

	if existVals == nil {
		// existVals means key expired
		insertVals = encVals
	} else {
		for i := 0; i < len(existVals); i++ {
			// If the existed value is the same as the set value, pass this field and value
			if bytes.Compare(encVals[i*2+1], existVals[i]) != 0 {
				insertVals = append(insertVals, encFields[i], encVals[i*2+1])
			}
		}
	}

	// check all fields and values.
	if err := db.checkKeyValue(encKey, insertVals...); err != nil {
		return err
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	for i := 0; i < len(insertVals); i += 2 {
		e := storage.NewEntry(encKey, insertVals[i+1], insertVals[i], Hash, HashHSet)
		if err := db.store(e); err != nil {
			return err
		}

		db.hashIndex.indexes.HSet(string(encKey), string(insertVals[i]), insertVals[i+1])
	}

	return nil
}

// HMGet get the values of all the given hash fields
func (db *RoseDB) HMGet(key interface{}, fields ...interface{}) [][]byte {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	var encFields [][]byte
	for i := 0; i < len(fields); i++ {
		efield, err := utils.EncodeValue(fields[i])

		if err != nil {
			return nil
		}
		if err = db.checkKeyValue(efield, nil); err != nil {
			return nil
		}

		encFields = append(encFields, efield)
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return nil
	}

	values := make([][]byte, 0)

	for _, field := range encFields {
		value := db.hashIndex.indexes.HGet(string(encKey), string(field))
		values = append(values, value)
	}

	return values
}

// HDel removes the specified fields from the hash stored at key.
// Specified fields that do not exist within this hash are ignored.
// If key does not exist, it is treated as an empty hash and this command returns false.
func (db *RoseDB) HDel(key interface{}, fields ...interface{}) (res int, err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return
	}
	if fields == nil || len(fields) == 0 {
		return
	}

	var encFields [][]byte
	for i := 0; i < len(fields); i++ {
		efield, err := utils.EncodeValue(fields[i])

		if err != nil {
			return
		}
		if err = db.checkKeyValue(efield, nil); err != nil {
			return
		}

		encFields = append(encFields, efield)
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	for _, f := range encFields {
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
		return
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
func (db *RoseDB) HExists(key, field interface{}) (ok bool) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	encField, err := utils.EncodeKey(field)
	if err != nil {
		return
	}

	if err := db.checkKeyValue(encField, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return
	}

	return db.hashIndex.indexes.HExists(string(encKey), string(encField))
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
		return
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
		return
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
		return
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	if !db.HKeyExists(encKey) {
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
	if duration <= 0 {
		return ErrInvalidTTL
	}
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}
	if err = db.checkKeyValue(encKey, nil); err != nil {
		return
	}
	if !db.HKeyExists(encKey) {
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
		return
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return
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

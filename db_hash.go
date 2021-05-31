package rosedb

import (
	"bytes"
	"github.com/roseduan/rosedb/ds/hash"
	"github.com/roseduan/rosedb/storage"
	"sync"
)

// HashIdx hash idx
type HashIdx struct {
	mu      sync.RWMutex
	indexes *hash.Hash
}

func newHashIdx() *HashIdx {
	return &HashIdx{indexes: hash.New()}
}

// HSet 将哈希表 hash 中域 field 的值设置为 value
// 如果给定的哈希表并不存在， 那么一个新的哈希表将被创建并执行 HSet 操作
// 如果域 field 已经存在于哈希表中， 那么它的旧值将被新值 value 覆盖
// 返回操作后key所属哈希表中的元素个数

// Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
// If field already exists in the hash, it is overwritten.
func (db *RoseDB) HSet(key, field, value []byte) (res int, err error) {
	if err = db.checkKeyValue(key, value); err != nil {
		return
	}

	// If the existed value is the same as the set value, nothing will be done.
	oldVal := db.HGet(key, field)
	if bytes.Compare(oldVal, value) == 0 {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	e := storage.NewEntry(key, value, field, Hash, HashHSet)
	if err = db.store(e); err != nil {
		return
	}

	res = db.hashIndex.indexes.HSet(string(key), string(field), value)
	return
}

// HSetNx 当且仅当域 field 尚未存在于哈希表的情况下， 将它的值设置为 value
// 如果给定域已经存在于哈希表当中， 那么命令将放弃执行设置操作
// 返回操作是否成功

// Sets field in the hash stored at key to value, only if field does not yet exist.
// If key does not exist, a new key holding a hash is created. If field already exists, this operation has no effect.
// return if the operation successful
func (db *RoseDB) HSetNx(key, field, value []byte) (res bool, err error) {
	if err = db.checkKeyValue(key, value); err != nil {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if res = db.hashIndex.indexes.HSetNx(string(key), string(field), value); res {
		e := storage.NewEntry(key, value, field, Hash, HashHSet)
		if err = db.store(e); err != nil {
			return
		}
	}

	return
}

// HGet 返回哈希表中给定域的值
// Returns the value associated with field in the hash stored at key.
func (db *RoseDB) HGet(key, field []byte) []byte {

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HGet(string(key), string(field))
}

// HGetAll 返回哈希表 key 中，所有的域和值
// Returns all fields and values of the hash stored at key.
// In the returned value, every field name is followed by its value, so the length of the reply is twice the size of the hash.
func (db *RoseDB) HGetAll(key []byte) [][]byte {

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HGetAll(string(key))
}

// HDel 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略
// 返回被成功移除的元素个数
// Removes the specified fields from the hash stored at key. Specified fields that do not exist within this hash are ignored.
// If key does not exist, it is treated as an empty hash and this command returns false.
func (db *RoseDB) HDel(key []byte, field ...[]byte) (res int, err error) {
	if field == nil || len(field) == 0 {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	for _, f := range field {
		if ok := db.hashIndex.indexes.HDel(string(key), string(f)); ok {
			e := storage.NewEntry(key, nil, f, Hash, HashHDel)
			if err = db.store(e); err != nil {
				return
			}
			res++
		}
	}
	return
}

// HExists 检查给定域 field 是否存在于key对应的哈希表中
// Returns if field is an existing field in the hash stored at key.
func (db *RoseDB) HExists(key, field []byte) bool {
	if err := db.checkKeyValue(key, nil); err != nil {
		return false
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HExists(string(key), string(field))
}

// HLen 返回哈希表 key 中域的数量
// Returns the number of fields contained in the hash stored at key.
func (db *RoseDB) HLen(key []byte) int {
	if err := db.checkKeyValue(key, nil); err != nil {
		return 0
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HLen(string(key))
}

// HKeys 返回哈希表 key 中的所有域
// Returns all field names in the hash stored at key.
func (db *RoseDB) HKeys(key []byte) (val []string) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HKeys(string(key))
}

// HValues 返回哈希表 key 中的所有域对应的值
// Returns all values in the hash stored at key.
func (db *RoseDB) HValues(key []byte) (val [][]byte) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HValues(string(key))
}

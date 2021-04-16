package rosedb

import (
	"rosedb/ds/hash"
	"rosedb/storage"
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
func (db *RoseDB) HSet(key, field, value []byte) (res int, err error) {

	if err = db.checkKeyValue(key, value); err != nil {
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
func (db *RoseDB) HGet(key, field []byte) []byte {

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HGet(string(key), string(field))
}

// HGetAll 返回哈希表 key 中，所有的域和值
func (db *RoseDB) HGetAll(key []byte) [][]byte {

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HGetAll(string(key))
}

// HDel 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略
// 返回被成功移除的元素个数
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
func (db *RoseDB) HExists(key, field []byte) bool {

	if err := db.checkKeyValue(key, nil); err != nil {
		return false
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HExists(string(key), string(field))
}

// HLen 返回哈希表 key 中域的数量
func (db *RoseDB) HLen(key []byte) int {
	if err := db.checkKeyValue(key, nil); err != nil {
		return 0
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HLen(string(key))
}

// HKeys 返回哈希表 key 中的所有域
func (db *RoseDB) HKeys(key []byte) (val []string) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HKeys(string(key))
}

// HValues 返回哈希表 key 中的所有域对应的值
func (db *RoseDB) HValues(key []byte) (val [][]byte) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HValues(string(key))
}

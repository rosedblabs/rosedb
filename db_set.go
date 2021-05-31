package rosedb

import (
	"github.com/roseduan/rosedb/ds/set"
	"github.com/roseduan/rosedb/storage"
	"sync"
)

// SetIdx the set idx
type SetIdx struct {
	mu      sync.RWMutex
	indexes *set.Set
}

func newSetIdx() *SetIdx {
	return &SetIdx{indexes: set.New()}
}

// SAdd 添加元素，返回添加后的集合中的元素个数
// Add the specified members to the set stored at key.
// Specified members that are already a member of this set are ignored.
// If key does not exist, a new set is created before adding the specified members.
func (db *RoseDB) SAdd(key []byte, members ...[]byte) (res int, err error) {
	if err = db.checkKeyValue(key, members...); err != nil {
		return
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	for _, m := range members {
		exist := db.setIndex.indexes.SIsMember(string(key), m)
		if !exist {
			e := storage.NewEntryNoExtra(key, m, Set, SetSAdd)
			if err = db.store(e); err != nil {
				return
			}
			res = db.setIndex.indexes.SAdd(string(key), m)
		}
	}
	return
}

// SPop 随机移除并返回集合中的count个元素
// Removes and returns one or more random members from the set value store at key.
func (db *RoseDB) SPop(key []byte, count int) (values [][]byte, err error) {
	if err = db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	values = db.setIndex.indexes.SPop(string(key), count)
	for _, v := range values {
		e := storage.NewEntryNoExtra(key, v, Set, SetSRem)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

// SIsMember 判断 member 元素是不是集合 key 的成员
// Returns if member is a member of the set stored at key.
func (db *RoseDB) SIsMember(key, member []byte) bool {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	return db.setIndex.indexes.SIsMember(string(key), member)
}

// SRandMember 从集合中返回随机元素，count的可选值如下：
// 如果 count 为正数，且小于集合元素数量，则返回一个包含 count 个元素的数组，数组中的元素各不相同
// 如果 count 大于等于集合元素数量，那么返回整个集合
// 如果 count 为负数，则返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值
// When called with just the key argument, return a random element from the set value stored at key.
func (db *RoseDB) SRandMember(key []byte, count int) [][]byte {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	return db.setIndex.indexes.SRandMember(string(key), count)
}

// SRem 移除集合 key 中的一个或多个 member 元素，不存在的 member 元素会被忽略
// 被成功移除的元素的数量，不包括被忽略的元素
// Remove the specified members from the set stored at key.
// Specified members that are not a member of this set are ignored.
// If key does not exist, it is treated as an empty set and this command returns 0.
func (db *RoseDB) SRem(key []byte, members ...[]byte) (res int, err error) {
	if err = db.checkKeyValue(key, members...); err != nil {
		return
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	for _, m := range members {
		if ok := db.setIndex.indexes.SRem(string(key), m); ok {
			e := storage.NewEntryNoExtra(key, m, Set, SetSRem)
			if err = db.store(e); err != nil {
				return
			}
			res++
		}
	}
	return
}

// SMove 将 member 元素从 src 集合移动到 dst 集合
// Move member from the set at source to the set at destination.
func (db *RoseDB) SMove(src, dst, member []byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if ok := db.setIndex.indexes.SMove(string(src), string(dst), member); ok {
		e := storage.NewEntry(src, member, dst, Set, SetSMove)
		if err := db.store(e); err != nil {
			return err
		}
	}
	return nil
}

// SCard 返回集合中的元素个数
// Returns the set cardinality (number of elements) of the set stored at key.
func (db *RoseDB) SCard(key []byte) int {
	if err := db.checkKeyValue(key, nil); err != nil {
		return 0
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	return db.setIndex.indexes.SCard(string(key))
}

// SMembers 返回集合中的所有元素
// Returns all the members of the set value stored at key.
func (db *RoseDB) SMembers(key []byte) (val [][]byte) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	return db.setIndex.indexes.SMembers(string(key))
}

// SUnion 返回给定全部集合数据的并集
// Returns the members of the set resulting from the union of all the given sets.
func (db *RoseDB) SUnion(keys ...[]byte) (val [][]byte) {
	if keys == nil || len(keys) == 0 {
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	var s []string
	for _, k := range keys {
		s = append(s, string(k))
	}

	return db.setIndex.indexes.SUnion(s...)
}

// SDiff 返回给定集合数据的差集
// Returns the members of the set resulting from the difference between the first set and all the successive sets.
func (db *RoseDB) SDiff(keys ...[]byte) (val [][]byte) {
	if keys == nil || len(keys) == 0 {
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	var s []string
	for _, k := range keys {
		s = append(s, string(k))
	}

	return db.setIndex.indexes.SDiff(s...)
}

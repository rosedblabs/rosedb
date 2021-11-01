package rosedb

import (
	"github.com/roseduan/rosedb/ds/zset"
	"github.com/roseduan/rosedb/storage"
	"github.com/roseduan/rosedb/utils"
	"sync"
	"time"
)

// ZsetIdx the zset idx.
type ZsetIdx struct {
	mu      *sync.RWMutex
	indexes *zset.SortedSet
}

// create a new zset index.
func newZsetIdx() *ZsetIdx {
	return &ZsetIdx{indexes: zset.New(), mu: new(sync.RWMutex)}
}

// ZAdd adds the specified member with the specified score to the sorted set stored at key.
func (db *RoseDB) ZAdd(key interface{}, score float64, member interface{}) error {
	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return err
	}
	if err := db.checkKeyValue(encKey, encMember); err != nil {
		return err
	}

	// if the score corresponding to the key and member already exist, nothing will be done.
	if ok, oldScore := db.ZScore(encKey, encMember); ok && oldScore == score {
		return nil
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	extra := []byte(utils.Float64ToStr(score))
	e := storage.NewEntry(encKey, encMember, extra, ZSet, ZSetZAdd)
	if err := db.store(e); err != nil {
		return err
	}

	db.zsetIndex.indexes.ZAdd(string(encKey), score, string(encMember))
	return nil
}

// ZScore returns the score of member in the sorted set at key.
func (db *RoseDB) ZScore(key, member interface{}) (ok bool, score float64) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return false, -1
	}
	if db.checkExpired(encKey, ZSet) {
		return
	}

	return db.zsetIndex.indexes.ZScore(string(encKey), string(encMember))
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func (db *RoseDB) ZCard(key interface{}) int {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return 0
	}
	if db.checkExpired(encKey, ZSet) {
		return 0
	}

	return db.zsetIndex.indexes.ZCard(string(encKey))
}

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
// The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
func (db *RoseDB) ZRank(key, member interface{}) int64 {
	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return -1
	}

	if err := db.checkKeyValue(encKey, encMember); err != nil {
		return -1
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return -1
	}

	return db.zsetIndex.indexes.ZRank(string(encKey), string(encMember))
}

// ZRevRank returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
// The rank (or index) is 0-based, which means that the member with the highest score has rank 0.
func (db *RoseDB) ZRevRank(key, member interface{}) int64 {
	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return -1
	}
	if err := db.checkKeyValue(encKey, encMember); err != nil {
		return -1
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return -1
	}

	return db.zsetIndex.indexes.ZRevRank(string(encKey), string(encMember))
}

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
// If key does not exist, a new sorted set with the specified member as its sole member is created.
func (db *RoseDB) ZIncrBy(key interface{}, increment float64, member interface{}) (float64, error) {
	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return increment, err
	}
	if err := db.checkKeyValue(encKey, encMember); err != nil {
		return increment, err
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	increment = db.zsetIndex.indexes.ZIncrBy(string(encKey), increment, string(encMember))

	extra := utils.Float64ToStr(increment)
	e := storage.NewEntry(encKey, encMember, []byte(extra), ZSet, ZSetZAdd)
	if err := db.store(e); err != nil {
		return increment, err
	}

	return increment, nil
}

// ZRange returns the specified range of elements in the sorted set stored at key.
func (db *RoseDB) ZRange(key interface{}, start, stop int) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRange(string(encKey), start, stop)
}

// ZRangeWithScores returns the specified range of elements in the sorted set stored at key.
func (db *RoseDB) ZRangeWithScores(key interface{}, start, stop int) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRangeWithScores(string(encKey), start, stop)
}

// ZRevRange returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
// Descending lexicographical order is used for elements with equal score.
func (db *RoseDB) ZRevRange(key interface{}, start, stop int) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRevRange(string(encKey), start, stop)
}

// ZRevRangeWithScores returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
// Descending lexicographical order is used for elements with equal score.
func (db *RoseDB) ZRevRangeWithScores(key interface{}, start, stop int) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRevRangeWithScores(string(encKey), start, stop)
}

// ZRem removes the specified members from the sorted set stored at key. Non existing members are ignored.
// An error is returned when key exists and does not hold a sorted set.
func (db *RoseDB) ZRem(key, member interface{}) (ok bool, err error) {
	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return
	}
	if err = db.checkKeyValue(encKey, encMember); err != nil {
		return
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	if db.checkExpired(encKey, ZSet) {
		return
	}

	if ok = db.zsetIndex.indexes.ZRem(string(encKey), string(encMember)); ok {
		e := storage.NewEntryNoExtra(encKey, encMember, ZSet, ZSetZRem)
		if err = db.store(e); err != nil {
			return
		}
	}

	return
}

// ZGetByRank get the member at key by rank, the rank is ordered from lowest to highest.
// The rank of lowest is 0 and so on.
func (db *RoseDB) ZGetByRank(key interface{}, rank int) []interface{} {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZGetByRank(string(encKey), rank)
}

// ZRevGetByRank get the member at key by rank, the rank is ordered from highest to lowest.
// The rank of highest is 0 and so on.
func (db *RoseDB) ZRevGetByRank(key interface{}, rank int) []interface{} {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRevGetByRank(string(encKey), rank)
}

// ZScoreRange returns all the elements in the sorted set at key with a score between min and max (including elements with score equal to min or max).
// The elements are considered to be ordered from low to high scores.
func (db *RoseDB) ZScoreRange(key interface{}, min, max float64) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZScoreRange(string(encKey), min, max)
}

// ZRevScoreRange returns all the elements in the sorted set at key with a score between max and min (including elements with score equal to max or min).
// In contrary to the default ordering of sorted sets, for this command the elements are considered to be ordered from high to low scores.
func (db *RoseDB) ZRevScoreRange(key interface{}, max, min float64) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRevScoreRange(string(encKey), max, min)
}

// ZKeyExists check if the key exists in zset.
func (db *RoseDB) ZKeyExists(key interface{}) (ok bool) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return false
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return
	}

	ok = db.zsetIndex.indexes.ZKeyExists(string(encKey))
	return
}

// ZClear clear the specified key in zset.
func (db *RoseDB) ZClear(key interface{}) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if !db.ZKeyExists(encKey) {
		return ErrKeyNotExist
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(encKey, nil, ZSet, ZSetZClear)
	if err = db.store(e); err != nil {
		return
	}
	db.zsetIndex.indexes.ZClear(string(encKey))
	return
}

// ZExpire set expired time for the key in zset.
func (db *RoseDB) ZExpire(key interface{}, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if !db.ZKeyExists(encKey) {
		return ErrKeyNotExist
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(encKey, nil, deadline, ZSet, ZSetZExpire)
	if err = db.store(e); err != nil {
		return err
	}

	db.expires[ZSet][string(encKey)] = deadline
	return
}

// ZTTL return time to live of the key.
func (db *RoseDB) ZTTL(key interface{}) (ttl int64) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}
	if !db.ZKeyExists(encKey) {
		return
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	deadline, exist := db.expires[ZSet][string(encKey)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}

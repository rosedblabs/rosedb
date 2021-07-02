package rosedb

import (
	"github.com/roseduan/rosedb/ds/zset"
	"github.com/roseduan/rosedb/storage"
	"github.com/roseduan/rosedb/utils"
	"math"
	"sync"
	"time"
)

// ZsetIdx the zset idx.
type ZsetIdx struct {
	mu      sync.RWMutex
	indexes *zset.SortedSet
}

// create a new zset index.
func newZsetIdx() *ZsetIdx {
	return &ZsetIdx{indexes: zset.New()}
}

// ZAdd adds the specified member with the specified score to the sorted set stored at key.
func (db *RoseDB) ZAdd(key []byte, score float64, member []byte) error {
	if err := db.checkKeyValue(key, member); err != nil {
		return err
	}

	// if the score corresponding to the key and member already exist, nothing will be done.
	if oldScore := db.ZScore(key, member); oldScore == score {
		return nil
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	extra := []byte(utils.Float64ToStr(score))
	e := storage.NewEntry(key, member, extra, ZSet, ZSetZAdd)
	if err := db.store(e); err != nil {
		return err
	}

	db.zsetIndex.indexes.ZAdd(string(key), score, string(member))
	return nil
}

// ZScore returns the score of member in the sorted set at key.
func (db *RoseDB) ZScore(key, member []byte) float64 {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return math.MinInt64
	}

	return db.zsetIndex.indexes.ZScore(string(key), string(member))
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func (db *RoseDB) ZCard(key []byte) int {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return 0
	}

	return db.zsetIndex.indexes.ZCard(string(key))
}

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
// The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
func (db *RoseDB) ZRank(key, member []byte) int64 {
	if err := db.checkKeyValue(key, member); err != nil {
		return -1
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return -1
	}

	return db.zsetIndex.indexes.ZRank(string(key), string(member))
}

// ZRevRank returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
// The rank (or index) is 0-based, which means that the member with the highest score has rank 0.
func (db *RoseDB) ZRevRank(key, member []byte) int64 {
	if err := db.checkKeyValue(key, member); err != nil {
		return -1
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return -1
	}

	return db.zsetIndex.indexes.ZRevRank(string(key), string(member))
}

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
// If key does not exist, a new sorted set with the specified member as its sole member is created.
func (db *RoseDB) ZIncrBy(key []byte, increment float64, member []byte) (float64, error) {
	if err := db.checkKeyValue(key, member); err != nil {
		return increment, err
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	increment = db.zsetIndex.indexes.ZIncrBy(string(key), increment, string(member))

	extra := utils.Float64ToStr(increment)
	e := storage.NewEntry(key, member, []byte(extra), ZSet, ZSetZAdd)
	if err := db.store(e); err != nil {
		return increment, err
	}

	return increment, nil
}

// ZRange returns the specified range of elements in the sorted set stored at key.
func (db *RoseDB) ZRange(key []byte, start, stop int) []interface{} {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRange(string(key), start, stop)
}

// ZRangeWithScores returns the specified range of elements in the sorted set stored at key.
func (db *RoseDB) ZRangeWithScores(key []byte, start, stop int) []interface{} {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRangeWithScores(string(key), start, stop)
}

// ZRevRange returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
// Descending lexicographical order is used for elements with equal score.
func (db *RoseDB) ZRevRange(key []byte, start, stop int) []interface{} {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRevRange(string(key), start, stop)
}

// ZRevRangeWithScores returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
// Descending lexicographical order is used for elements with equal score.
func (db *RoseDB) ZRevRangeWithScores(key []byte, start, stop int) []interface{} {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRevRangeWithScores(string(key), start, stop)
}

// ZRem removes the specified members from the sorted set stored at key. Non existing members are ignored.
// An error is returned when key exists and does not hold a sorted set.
func (db *RoseDB) ZRem(key, member []byte) (ok bool, err error) {
	if err = db.checkKeyValue(key, member); err != nil {
		return
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	if ok = db.zsetIndex.indexes.ZRem(string(key), string(member)); ok {
		e := storage.NewEntryNoExtra(key, member, ZSet, ZSetZRem)
		if err = db.store(e); err != nil {
			return
		}
	}

	return
}

// ZGetByRank get the member at key by rank, the rank is ordered from lowest to highest.
// The rank of lowest is 0 and so on.
func (db *RoseDB) ZGetByRank(key []byte, rank int) []interface{} {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZGetByRank(string(key), rank)
}

// ZRevGetByRank get the member at key by rank, the rank is ordered from highest to lowest.
// The rank of highest is 0 and so on.
func (db *RoseDB) ZRevGetByRank(key []byte, rank int) []interface{} {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRevGetByRank(string(key), rank)
}

// ZScoreRange returns all the elements in the sorted set at key with a score between min and max (including elements with score equal to min or max).
// The elements are considered to be ordered from low to high scores.
func (db *RoseDB) ZScoreRange(key []byte, min, max float64) []interface{} {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZScoreRange(string(key), min, max)
}

// ZRevScoreRange returns all the elements in the sorted set at key with a score between max and min (including elements with score equal to max or min).
// In contrary to the default ordering of sorted sets, for this command the elements are considered to be ordered from high to low scores.
func (db *RoseDB) ZRevScoreRange(key []byte, max, min float64) []interface{} {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRevScoreRange(string(key), max, min)
}

// ZKeyExists check if the key exists in zset.
func (db *RoseDB) ZKeyExists(key []byte) (ok bool) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return
	}

	ok = db.zsetIndex.indexes.ZKeyExists(string(key))
	return
}

// ZClear clear the specified key in zset.
func (db *RoseDB) ZClear(key []byte) (err error) {
	if !db.ZKeyExists(key) {
		return ErrKeyNotExist
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, nil, ZSet, ZSetZClear)
	if err = db.store(e); err != nil {
		return
	}
	db.zsetIndex.indexes.ZClear(string(key))
	return
}

// ZExpire set expired time for the key in zset.
func (db *RoseDB) ZExpire(key []byte, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}
	if !db.ZKeyExists(key) {
		return ErrKeyNotExist
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(key, nil, deadline, ZSet, ZSetZExpire)
	if err = db.store(e); err != nil {
		return err
	}

	db.expires[ZSet][string(key)] = deadline
	return
}

// ZTTL return time to live of the key.
func (db *RoseDB) ZTTL(key []byte) (ttl int64) {
	if !db.ZKeyExists(key) {
		return
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	deadline, exist := db.expires[ZSet][string(key)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}

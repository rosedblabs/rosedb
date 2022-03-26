package rosedb

import (
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/util"
)

// ZAdd adds the specified member with the specified score to the sorted set stored at key.
func (db *RoseDB) ZAdd(key []byte, score float64, member []byte) error {
	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	scoreBuf := []byte(util.Float64ToStr(score))
	zsetKey := db.encodeKey(key, scoreBuf)
	entry := &logfile.LogEntry{Key: zsetKey, Value: member}
	if _, err := db.writeLogEntry(entry, ZSet); err != nil {
		return err
	}
	db.zsetIndex.indexes.ZAdd(string(key), score, string(member))
	return nil
}

// ZScore returns the score of member in the sorted set at key.
func (db *RoseDB) ZScore(key, member []byte) (ok bool, score float64) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	return db.zsetIndex.indexes.ZScore(string(key), string(member))
}

// ZRem removes the specified members from the sorted set stored at key. Non existing members are ignored.
// An error is returned when key exists and does not hold a sorted set.
func (db *RoseDB) ZRem(key, member []byte) error {
	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	if ok := db.zsetIndex.indexes.ZRem(string(key), string(member)); ok {
		entry := &logfile.LogEntry{Key: key, Value: member, Type: logfile.TypeDelete}
		if _, err := db.writeLogEntry(entry, ZSet); err != nil {
			return err
		}
	}
	return nil
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func (db *RoseDB) ZCard(key []byte) int {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	return db.zsetIndex.indexes.ZCard(string(key))
}

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
// If key does not exist, a new sorted set with the specified member as its sole member is created.
func (db *RoseDB) ZIncrBy(key []byte, increment float64, member []byte) (float64, error) {
	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	scoreBuf := []byte(util.Float64ToStr(increment))
	zsetKey := db.encodeKey(key, scoreBuf)
	entry := &logfile.LogEntry{Key: zsetKey, Value: member}
	if _, err := db.writeLogEntry(entry, ZSet); err != nil {
		return 0, err
	}
	incrBy := db.zsetIndex.indexes.ZIncrBy(string(key), increment, string(member))
	return incrBy, nil
}

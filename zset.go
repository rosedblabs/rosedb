package rosedb

import (
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/util"
)

// ZAdd adds the specified member with the specified score to the sorted set stored at key.
func (db *RoseDB) ZAdd(key []byte, score float64, member []byte) error {
	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return err
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()
	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewART()
	}
	db.zsetIndex.idxTree = db.zsetIndex.trees[string(key)]

	scoreBuf := []byte(util.Float64ToStr(score))
	zsetKey := db.encodeKey(key, scoreBuf)
	entry := &logfile.LogEntry{Key: zsetKey, Value: member}
	pos, err := db.writeLogEntry(entry, ZSet)
	if err != nil {
		return err
	}

	_, size := logfile.EncodeEntry(entry)
	pos.entrySize = size
	ent := &logfile.LogEntry{Key: sum, Value: member}
	if err := db.updateIndexTree(ent, pos, true, ZSet); err != nil {
		return err
	}
	db.zsetIndex.indexes.ZAdd(string(key), score, string(sum))
	return nil
}

// ZScore returns the score of member in the sorted set at key.
func (db *RoseDB) ZScore(key, member []byte) (ok bool, score float64) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return false, 0
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()
	return db.zsetIndex.indexes.ZScore(string(key), string(sum))
}

// ZRem removes the specified members from the sorted set stored at key. Non existing members are ignored.
// An error is returned when key exists and does not hold a sorted set.
func (db *RoseDB) ZRem(key, member []byte) error {
	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return err
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()

	ok := db.zsetIndex.indexes.ZRem(string(key), string(sum))
	if !ok {
		return nil
	}

	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewART()
	}
	db.zsetIndex.idxTree = db.zsetIndex.trees[string(key)]
	db.zsetIndex.idxTree.Delete(sum)
	entry := &logfile.LogEntry{Key: key, Value: sum, Type: logfile.TypeDelete}
	if _, err := db.writeLogEntry(entry, ZSet); err != nil {
		return err
	}
	return nil
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func (db *RoseDB) ZCard(key []byte) int {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	return db.zsetIndex.indexes.ZCard(string(key))
}

// ZRange returns the specified range of elements in the sorted set stored at key.
func (db *RoseDB) ZRange(key []byte, start, stop int) ([][]byte, error) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewART()
	}
	db.zsetIndex.idxTree = db.zsetIndex.trees[string(key)]

	var res [][]byte
	values := db.zsetIndex.indexes.ZRange(string(key), start, stop)
	for _, val := range values {
		v, _ := val.(string)
		if val, err := db.getVal([]byte(v), ZSet); err != nil {
			return nil, err
		} else {
			res = append(res, val)
		}
	}
	return res, nil
}

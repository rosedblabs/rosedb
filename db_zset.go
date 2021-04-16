package rosedb

import (
	"rosedb/ds/zset"
	"rosedb/storage"
	"rosedb/utils"
	"sync"
)

// ZsetIdx the zset idx
type ZsetIdx struct {
	mu      sync.RWMutex
	indexes *zset.SortedSet
}

func newZsetIdx() *ZsetIdx {
	return &ZsetIdx{indexes: zset.New()}
}

// ZAdd 将 member 元素及其 score 值加入到有序集 key 当中
func (db *RoseDB) ZAdd(key []byte, score float64, member []byte) error {
	if err := db.checkKeyValue(key, member); err != nil {
		return err
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

// ZScore 返回集合key中对应member的score值，如果不存在则返回负无穷
func (db *RoseDB) ZScore(key, member []byte) float64 {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	return db.zsetIndex.indexes.ZScore(string(key), string(member))
}

// ZCard 返回指定集合key中的元素个数
func (db *RoseDB) ZCard(key []byte) int {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	return db.zsetIndex.indexes.ZCard(string(key))
}

// ZRank 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递增(从小到大)顺序排列
//排名以 0 为底，也就是说， score 值最小的成员排名为 0
func (db *RoseDB) ZRank(key, member []byte) int64 {
	if err := db.checkKeyValue(key, member); err != nil {
		return -1
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	return db.zsetIndex.indexes.ZRank(string(key), string(member))
}

// ZRevRank 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递减(从大到小)排序
//排名以 0 为底，也就是说， score 值最大的成员排名为 0
func (db *RoseDB) ZRevRank(key, member []byte) int64 {
	if err := db.checkKeyValue(key, member); err != nil {
		return -1
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	return db.zsetIndex.indexes.ZRevRank(string(key), string(member))
}

// ZIncrBy 为有序集 key 的成员 member 的 score 值加上增量 increment
//当 key 不存在，或 member 不是 key 的成员时，ZIncrBy 等同于 ZAdd
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

// ZRange 返回有序集 key 中，指定区间内的成员，其中成员的位置按 score 值递增(从小到大)来排序
//具有相同 score 值的成员按字典序(lexicographical order )来排列
func (db *RoseDB) ZRange(key []byte, start, stop int) []interface{} {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	return db.zsetIndex.indexes.ZRange(string(key), start, stop)
}

// ZRevRange 返回有序集 key 中，指定区间内的成员，其中成员的位置按 score 值递减(从大到小)来排列
//具有相同 score 值的成员按字典序的逆序(reverse lexicographical order)排列
func (db *RoseDB) ZRevRange(key []byte, start, stop int) []interface{} {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	return db.zsetIndex.indexes.ZRevRange(string(key), start, stop)
}

// ZRem 移除有序集 key 中的 member 成员，不存在则将被忽略
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

// ZGetByRank 根据排名获取member及分值信息，从小到大排列遍历，即分值最低排名为0，依次类推
func (db *RoseDB) ZGetByRank(key []byte, rank int) []interface{} {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	return db.zsetIndex.indexes.ZGetByRank(string(key), rank)
}

// ZRevGetByRank 根据排名获取member及分值信息，从大到小排列遍历，即分值最高排名为0，依次类推
func (db *RoseDB) ZRevGetByRank(key []byte, rank int) []interface{} {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	return db.zsetIndex.indexes.ZRevGetByRank(string(key), rank)
}

// ZScoreRange 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员
//有序集成员按 score 值递增(从小到大)次序排列
func (db *RoseDB) ZScoreRange(key []byte, min, max float64) []interface{} {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	return db.zsetIndex.indexes.ZScoreRange(string(key), min, max)
}

// ZRevScoreRange 返回有序集 key 中， score 值介于 max 和 min 之间(包括等于 max 或 min )的所有的成员
//有序集成员按 score 值递减(从大到小)的次序排列
func (db *RoseDB) ZRevScoreRange(key []byte, max, min float64) []interface{} {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	return db.zsetIndex.indexes.ZRevScoreRange(string(key), max, min)
}

package zset

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func InitZSet() *SortedSet {
	zSet := New()
	zSet.ZAdd("myzset", 19, "ced")
	zSet.ZAdd("myzset", 12, "acd")
	zSet.ZAdd("myzset", 17, "bcd")
	zSet.ZAdd("myzset", 32, "acc")
	zSet.ZAdd("myzset", 17, "mcd")
	zSet.ZAdd("myzset", 21, "ccd")
	zSet.ZAdd("myzset", 17, "ecd")

	return zSet
}

func TestSortedSet_ZAdd(t *testing.T) {
	zSet := InitZSet()
	zSet.ZAdd("myzset", 39, "mmd")

	c1 := zSet.ZCard("myzset")
	assert.Equal(t, 8, c1)
}

func TestSortedSet_ZScore(t *testing.T) {
	zSet := InitZSet()
	ok, s1 := zSet.ZScore("myzset", "acd")
	assert.Equal(t, true, ok)
	assert.Equal(t, float64(12), s1)

	ok, s2 := zSet.ZScore("myzset", "aaa")
	assert.Equal(t, false, ok)
	assert.Equal(t, float64(0), s2)
}

func TestSortedSet_ZRank(t *testing.T) {
	key := "myzset"
	zset := InitZSet()
	r1 := zset.ZRank(key, "acd")
	assert.Equal(t, int64(0), r1)

	r2 := zset.ZRank(key, "bcd")
	assert.Equal(t, int64(1), r2)

	r3 := zset.ZRank(key, "not exist")
	assert.Equal(t, int64(-1), r3)
}

func TestSortedSet_ZRevRank(t *testing.T) {
	key := "myzset"
	zset := InitZSet()
	r1 := zset.ZRevRank(key, "acd")
	assert.Equal(t, int64(6), r1)

	r2 := zset.ZRevRank(key, "bcd")
	assert.Equal(t, int64(5), r2)

	r3 := zset.ZRevRank(key, "not exist")
	assert.Equal(t, int64(-1), r3)
}

func TestSortedSet_ZIncrBy(t *testing.T) {
	zset := InitZSet()
	key := "myzset"

	incr1 := zset.ZIncrBy(key, 300, "acd")
	assert.Equal(t, float64(312), incr1)

	incr2 := zset.ZIncrBy(key, 100, "acc")
	assert.Equal(t, float64(132), incr2)
}

func TestSortedSet_ZRange(t *testing.T) {
	zSet := InitZSet()
	key := "myzset"

	ran := zSet.ZRange(key, 0, -1)
	assert.Equal(t, 7, len(ran))

	for _, v := range ran {
		assert.NotNil(t, v)
	}
}

func TestSortedSet_ZRangeWithScores(t *testing.T) {
	zSet := InitZSet()
	key := "myzset"

	values := zSet.ZRangeWithScores(key, 0, -1)
	assert.NotNil(t, values)

	for _, v := range values {
		assert.NotNil(t, v)
	}
}

func TestSortedSet_ZRevRange(t *testing.T) {
	zSet := InitZSet()
	key := "myzset"

	values := zSet.ZRevRange(key, 0, -1)
	assert.NotNil(t, values)

	for _, v := range values {
		assert.NotNil(t, v)
	}
}

func TestSortedSet_ZRevRangeWithScores(t *testing.T) {
	zSet := InitZSet()
	key := "myzset"

	values := zSet.ZRevRangeWithScores(key, 0, -1)
	assert.NotNil(t, values)

	for _, v := range values {
		assert.NotNil(t, v)
	}
}

func TestSortedSet_ZRem(t *testing.T) {
	zset := InitZSet()
	key := "myzset"

	ok1 := zset.ZRem(key, "acd")
	assert.Equal(t, true, ok1)

	ok2 := zset.ZRem(key, "aaaaaaa")
	assert.Equal(t, false, ok2)
}

func TestSortedSet_ZGetByRank(t *testing.T) {
	zset := InitZSet()
	key := "myzset"

	getRank := func(rank int) {
		val := zset.ZGetByRank(key, rank)
		if val != nil {
			for _, v := range val {
				assert.NotNil(t, v)
			}
		}
	}
	getRank(0)
	getRank(4)
	getRank(6)
}

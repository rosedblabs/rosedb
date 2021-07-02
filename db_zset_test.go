package rosedb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRoseDB_ZAdd(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	key := []byte("my_zset")
	err := db.ZAdd(key, 310.23, []byte("roseduan"))

	db.ZAdd(nil, 0, nil)
	db.ZAdd(key, 30.234554, []byte("Java"))
	db.ZAdd(key, 92.2233, []byte("Golang"))
	db.ZAdd(key, 221.24, []byte("Python"))
	db.ZAdd(key, 221.24, []byte("Python-tensorflow"))
	db.ZAdd(key, 221.24, []byte("Python-flask"))
	db.ZAdd(key, 221.24, []byte("Python-django"))
	db.ZAdd(key, 221.24, []byte("Python-scrapy"))
	db.ZAdd(key, 54.30003, []byte("C"))
	db.ZAdd(key, 54.30003, []byte("C plus plus"))

	if err != nil {
		t.Log(err)
	}
}

func TestRoseDB_ZRem(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

	db.ZRem(nil, nil)
	_, _ = db.ZRem(key, []byte("C"))
	_, _ = db.ZRem(key, []byte("Java"))
}

func TestRoseDB_ZScore(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

	s := db.ZScore(key, []byte("roseduan"))
	t.Log(s)
}

func TestRoseDB_ZCard(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

	t.Log(db.ZCard(key))
}

func TestRoseDB_ZRank(t *testing.T) {
	db := ReopenDb()
	db.Close()
	key := []byte("my_zset")

	db.ZRank(nil, nil)
	rank := db.ZRank(key, []byte("Python-flask"))
	t.Log(rank)

	t.Log(db.ZRank(key, []byte("Java")))
	t.Log(db.ZRank(key, []byte("roseduan")))
}

func TestRoseDB_ZRevRank(t *testing.T) {
	db := ReopenDb()
	db.Close()
	key := []byte("my_zset")

	db.ZRevRank(nil, nil)
	rank := db.ZRevRank(key, []byte("Python-flask"))
	t.Log(rank)

	t.Log(db.ZRevRank(key, []byte("Java")))
	t.Log(db.ZRevRank(key, []byte("roseduan")))
}

func TestRoseDB_ZRange(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

	db.ZRange(nil, 0, -1)
	vals := db.ZRange(key, 0, -1)
	for _, v := range vals {
		t.Logf("%+v ", v)
	}
}

func TestRoseDB_ZRangeWithScores(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

	db.ZRangeWithScores(nil, 0, -1)
	vals := db.ZRangeWithScores(key, 0, -1)
	for _, v := range vals {
		t.Logf("%+v ", v)
	}
}

func TestRoseDB_ZRevRange(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

	db.ZRevRange(nil, 0, -1)
	vals := db.ZRevRange(key, 0, -1)
	for _, v := range vals {
		t.Logf("%+v ", v)
	}
}

func TestRoseDB_ZRevRangeWithScores(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

	db.ZRevRangeWithScores(nil, 0, -1)
	vals := db.ZRevRangeWithScores(key, 0, -1)
	for _, v := range vals {
		t.Logf("%+v ", v)
	}
}

func TestRoseDB_ZIncrBy(t *testing.T) {

	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

	db.ZIncrBy(nil, 10, nil)
	incr, err := db.ZIncrBy(key, 100, []byte("Java"))
	if err != nil {
		t.Log(err)
	}

	t.Log(incr)
	t.Log(db.ZScore(key, []byte("Java")))
}

func TestRoseDB_ZGetByRank(t *testing.T) {

	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

	getRank := func(rank int) {
		val := db.ZGetByRank(key, rank)
		for _, v := range val {
			t.Logf("%+v ", v)
		}
	}

	getRank(0)
	getRank(4)
	getRank(8)
	getRank(2)
}

func TestRoseDB_ZRevGetByRank(t *testing.T) {

	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

	getRevRank := func(rank int) {
		val := db.ZRevGetByRank(key, rank)
		for _, v := range val {
			t.Logf("%+v ", v)
		}
	}
	getRevRank(0)
	getRevRank(9)
}

func TestRoseDB_ZScoreRange(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

	scoreRange := func(min, max float64) {
		vals := db.ZScoreRange(key, min, max)
		for _, v := range vals {
			t.Logf("%+v ", v)
		}
	}

	db.ZScoreRange(nil, 0, -1)
	scoreRange(50, 100)
	scoreRange(100, 200)
	scoreRange(200, 500)
}

func TestRoseDB_ZRevScoreRange(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

	recScoreRange := func(max, min float64) {
		vals := db.ZRevScoreRange(key, max, min)
		for _, v := range vals {
			t.Logf("%+v ", v)
		}
	}

	db.ZRevScoreRange(nil, 0, -1)
	recScoreRange(100, 50)
	recScoreRange(200, 100)
	recScoreRange(500, 200)
}

func TestRoseDB_ZExpire(t *testing.T) {
	db := InitDb()
	defer db.Close()

	key := []byte("my_zset_key")
	db.ZAdd(key, 423.21, []byte("val-1"))
	db.ZAdd(key, 675.15, []byte("val-2"))

	err := db.ZExpire(key, 100)
	assert.Equal(t, err, nil)

	for i := 0; i < 5; i++ {
		t.Log(db.ZTTL(key))
		//time.Sleep(time.Second * 2)
	}
}

func TestRoseDB_ZTTL(t *testing.T) {
	db := InitDb()
	defer db.Close()

	key := []byte("my_zset_key_2")
	db.ZAdd(key, 423.21, []byte("val-1"))

	db.ZExpire(key, 20)
	t.Log(db.ZTTL(key))
	for i := 0; i < 5; i++ {
		t.Log(db.ZTTL(key))
		//time.Sleep(time.Second * 2)
	}
}

func TestRoseDB_ZKeyExists(t *testing.T) {
	db := InitDb()
	defer db.Close()

	key := []byte("my_zset_key_3")
	db.ZAdd(key, 43.21, []byte("val-1"))

	ok1 := db.ZKeyExists(key)
	assert.Equal(t, ok1, true)

	ok2 := db.ZKeyExists([]byte("my"))
	assert.Equal(t, ok2, false)
}

func TestRoseDB_ZClear(t *testing.T) {
	db := InitDb()
	defer db.Close()

	key := []byte("my_zset_key_3")
	db.ZAdd(key, 43.21, []byte("val-1"))

	err := db.ZClear(key)
	assert.Equal(t, err, nil)
}

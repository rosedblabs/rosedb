package rosedb

import (
	"testing"
)

func TestRoseDB_ZAdd(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	key := []byte("my_zset")
	err := db.ZAdd(key, 310.23, []byte("roseduan"))

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

func TestRoseDB_ZScore(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

	s := db.ZScore(key, []byte("roseduan"))
	t.Log(s)

	t.Log(db.ZScore(key, []byte("not exist")))
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

	rank := db.ZRank(key, []byte("Python-flask"))
	t.Log(rank)

	t.Log(db.ZRank(key, []byte("Java")))
	t.Log(db.ZRank(key, []byte("roseduan")))
}

func TestRoseDB_ZRevRank(t *testing.T) {
	db := ReopenDb()
	db.Close()
	key := []byte("my_zset")

	rank := db.ZRevRank(key, []byte("Python-flask"))
	t.Log(rank)

	t.Log(db.ZRevRank(key, []byte("Java")))
	t.Log(db.ZRevRank(key, []byte("roseduan")))
}

func TestRoseDB_ZRange(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

	vals := db.ZRange(key, 0, -1)
	for _, v := range vals {
		t.Logf("%+v ", v)
	}
}

func TestRoseDB_ZRevRange(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

	vals := db.ZRevRange(key, 0, -1)
	for _, v := range vals {
		t.Logf("%+v ", v)
	}
}

func TestRoseDB_ZIncrBy(t *testing.T) {

	db := ReopenDb()
	defer db.Close()
	key := []byte("my_zset")

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
		t.Log("------------")
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
		t.Log("------------")
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
		t.Log("---------------")
	}

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
		t.Log("---------------")
	}

	recScoreRange(100, 50)
	recScoreRange(200, 100)
	recScoreRange(500, 200)
}

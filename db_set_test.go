package rosedb

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestRoseDB_SAdd(t *testing.T) {
	var key = "my_set"

	t.Run("normal situation", func(t *testing.T) {
		db := InitDb()
		defer db.Close()

		db.SAdd([]byte(key), []byte("set_data_001"), []byte("set_data_002"), []byte("set_data_003"))
		res, _ := db.SAdd([]byte(key), []byte("set_data_004"), []byte("set_data_005"), []byte("set_data_006"))
		t.Log(res)
	})

	t.Run("reopen and add", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		res, _ := db.SAdd([]byte(key), []byte("set_data_007"), []byte("set_data_008"), []byte("set_data_009"))
		t.Log(res)
	})

	t.Run("large data", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()
		rand.Seed(time.Now().Unix())

		valPrefix := "set_data_"

		var res int
		for i := 0; i < 100000; i++ {
			val := valPrefix + strconv.Itoa(rand.Intn(1000000))

			res, _ = db.SAdd([]byte(key), []byte(val))
		}
		t.Log(res)
	})
}

func TestRoseDB_SPop(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	var key = []byte("my_set")

	values, _ := db.SPop(key, 2)
	for _, v := range values {
		t.Log(string(v))
	}
}

func TestRoseDB_SCard(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	var key = []byte("my_set")

	card := db.SCard(key)
	t.Log(card)

	card1 := db.SCard([]byte("not exist"))
	t.Log(card1)
}

func TestRoseDB_SIsMember(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	var key = []byte("my_set")

	t.Log(db.SIsMember(key, []byte("set_data_009")))
	t.Log(db.SIsMember(key, []byte("set_data_001")))
	t.Log(db.SIsMember(key, []byte("not exist one")))
}

func TestRoseDB_SMembers(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	var key = []byte("my_set")

	members := db.SMembers(key)
	for _, m := range members {
		t.Log(string(m))
	}
}

func TestRoseDB_SRem(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	var key = []byte("my_set")

	t.Log(db.SRem(key, []byte("set_data_009")))
	t.Log(db.SRem(key, []byte("set_data_001")))
	t.Log(db.SRem(key, []byte("not exist one")))
}

func TestRoseDB_SRandMember(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	var key = []byte("my_set")

	randMem := func(count int) {
		vals := db.SRandMember(key, count)
		for _, v := range vals {
			t.Log(string(v))
		}

		t.Log("---------")
	}

	randMem(3)
	randMem(20)
	randMem(-3)
	randMem(-20)
}

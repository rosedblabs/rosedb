package rosedb

import (
	"testing"
)

func TestRoseDB_SAdd(t *testing.T) {
	var key = "my_set"

	t.Run("normal situation", func(t *testing.T) {
		db := InitDb()
		defer db.Close()

		db.SAdd(nil, nil)

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
	//
	//t.Run("large data", func(t *testing.T) {
	//	db := ReopenDb()
	//	defer db.Close()
	//	rand.Seed(time.Now().Unix())
	//
	//	valPrefix := "set_data_"
	//
	//	var res int
	//	for i := 0; i < 100000; i++ {
	//		val := valPrefix + strconv.Itoa(rand.Intn(1000000))
	//
	//		res, _ = db.SAdd([]byte(key), []byte(val))
	//	}
	//	t.Log(res)
	//})
}

func TestRoseDB_SPop(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	var key = []byte("my_set")

	db.SPop(nil, 3)
	values, _ := db.SPop(key, 2)
	for _, v := range values {
		t.Log(string(v))
	}
}

func TestRoseDB_SCard(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	var key = []byte("my_set")

	db.SCard(nil)

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

	db.SMembers(nil)
	members := db.SMembers(key)
	for _, m := range members {
		t.Log(string(m))
	}
}

func TestRoseDB_SRem(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	var key = []byte("my_set")

	db.SRem(nil, nil)
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
	}
	randMem(3)
	randMem(20)
	randMem(-3)
	randMem(-20)
}

func TestRoseDB_SDiff(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	var keys = [][]byte{
		[]byte("my_set1"),
		[]byte("my_set2"),
	}

	var emptyKeys [][]byte
	db.SDiff(emptyKeys...)
	db.SDiff(keys...)
}

func TestRoseDB_SMove(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	var key1 = []byte("my_set1")
	var key2 = []byte("my_set2")

	db.SAdd(key1, [][]byte{[]byte("1")}...)
	db.SAdd(key2, [][]byte{[]byte("2")}...)

	db.SMove(nil, nil, nil)
	db.SMove(key1, key2, []byte("1"))
}

func TestRoseDB_SUnion(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	var keys = [][]byte{
		[]byte("my_set1"),
		[]byte("my_set2"),
	}

	var emptyKeys [][]byte
	db.SUnion(emptyKeys...)
	db.SUnion(keys...)
}

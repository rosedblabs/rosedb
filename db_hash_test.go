package rosedb

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var key = "myhash"

func TestRoseDB_HSet(t *testing.T) {

	t.Run("test1", func(t *testing.T) {
		db := InitDb()
		defer db.Close()

		res, _ := db.HSet([]byte(key), []byte("my_name"), []byte("roseduan"))
		t.Log(res)
	})

	t.Run("reopen and set", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		res, _ := db.HSet([]byte(key), []byte("my_hobby"), []byte("coding better"))
		res, _ = db.HSet([]byte(key), []byte("my_lang"), []byte("Java and Go"))
		t.Log(res)
	})

	t.Run("multi data", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		rand.Seed(time.Now().Unix())

		fieldPrefix := "hash_field_"
		valPrefix := "hash_data_"

		var res int
		for i := 0; i < 100000; i++ {
			field := fieldPrefix + strconv.Itoa(rand.Intn(1000000))
			val := valPrefix + strconv.Itoa(rand.Intn(1000000))

			res, _ = db.HSet([]byte(key), []byte(field), []byte(val))
		}
		t.Log(res)
	})
}

func TestRoseDB_HSetNx(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	ok, _ := db.HSetNx([]byte(key), []byte("my_hobby"), []byte("coding better"))
	t.Log(ok)
	ok, _ = db.HSetNx([]byte(key), []byte("my_new_lang"), []byte("Java Go Python"))
	t.Log(ok)

	t.Log(db.HLen([]byte(key)))
}

func TestRoseDB_HGet(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	val1 := db.HGet([]byte(key), []byte("my_name"))
	val2 := db.HGet([]byte(key), []byte("not exist"))
	val3 := db.HGet([]byte(key), []byte("my_hobby"))

	t.Log(string(val1))
	t.Log(string(val2))
	t.Log(string(val3))

	val4 := db.HGet([]byte(key), []byte("hash_field_732328"))
	val5 := db.HGet([]byte(key), []byte("hash_field_112243"))

	t.Log(string(val4))
	t.Log(string(val5))
}

func TestRoseDB_HGetAll(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	values := db.HGetAll([]byte(key))
	for _, v := range values {
		t.Log(string(v))
	}
}

func TestRoseDB_HDel(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	res, _ := db.HDel([]byte(key), []byte("my_name"), []byte("my_name2"), []byte("my_name3"))
	t.Log(res)
}

func TestRoseDB_HExists(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	ok := db.HExists([]byte(key), []byte("my_name"))
	t.Log(ok)

	t.Log(db.HExists([]byte(key), []byte("my_hobby")))
	t.Log(db.HExists([]byte(key), []byte("my_name1")))
	t.Log(db.HExists([]byte(key+"abcd"), []byte("my_hobby")))
}

func TestRoseDB_HKeys(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	keys := db.HKeys([]byte(key))
	for _, k := range keys {
		t.Log(k)
	}
}

func TestRoseDB_HValues(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	keys := db.HValues([]byte(key))
	for _, k := range keys {
		t.Log(string(k))
	}
}

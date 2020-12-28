package rosedb

import (
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestRoseDB_Set(t *testing.T) {

	t.Run("normal situation", func(t *testing.T) {
		db := InitDb()
		defer db.Close()

		err := db.Set([]byte("test_key"), []byte("I am roseduan"))
		if err != nil {
			log.Fatal("write data error ", err)
		}
	})

	t.Run("reopen and set", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		db.Set([]byte("test_key001"), []byte("test_val001"))
		db.Set([]byte("test_key002"), []byte("test_val002"))
	})

	t.Run("large data", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		writeLargeData(db, t)
	})
}

func TestRoseDB_Get(t *testing.T) {

	t.Run("normal situation", func(t *testing.T) {
		db := InitDb()
		defer db.Close()

		db.Set([]byte("test_key_001"), []byte("test_val_001"))
		db.Set([]byte("test_key_002"), []byte("test_val_002"))
		db.Set([]byte("test_key_003"), []byte("test_val_003"))
		db.Set([]byte("test_key_004"), []byte("test_val_004"))
		db.Set([]byte("test_key_005"), []byte("test_val_005"))

		val, err := db.Get([]byte("test_key_001"))
		if err != nil {
			log.Fatal("get val error : ", err)
		}

		t.Log(string(val))

		val, _ = db.Get([]byte("test_key_002"))
		t.Log(string(val))

		val, _ = db.Get([]byte("test_key_003"))
		t.Log(string(val))
	})

	t.Run("reopen and get", func(t *testing.T) {
		db := ReopenDb()
		t.Log("reopen db...")

		val, err := db.Get([]byte("test_key_001"))
		if err != nil {
			log.Fatal("get val error : ", err)
		}

		t.Log(string(val))

		val, _ = db.Get([]byte("test_key_002"))
		t.Log(string(val))

		val, _ = db.Get([]byte("test_key_003"))
		t.Log(string(val))
	})

	t.Run("large data", func(t *testing.T) {
		now := time.Now()
		db := ReopenDb()
		t.Log("reopen db time spent : ", time.Since(now))

		defer db.Close()

		val, _ := db.Get([]byte("test_key_001"))
		t.Log(string(val))

		val, _ = db.Get([]byte("test_key_534647"))
		t.Log(string(val))

		val, _ = db.Get([]byte("test_key_378893"))
		t.Log(string(val))
	})
}

func TestRoseDB_GetSet(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	val, err := db.GetSet([]byte("test_key_001"), []byte("test_new_val_001"))
	if err != nil {
		log.Fatal(err)
	}
	t.Log("original val : ", string(val))

	val, _ = db.Get([]byte("test_key_001"))
	t.Log("new val : ", string(val))
}

func TestRoseDB_Append(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	err := db.Append([]byte("test_key_26385"), []byte("_abcd"))
	if err != nil {
		t.Log(err)
	}

	val, _ := db.Get([]byte("test_key_26385"))
	t.Log(string(val))
}

func TestRoseDB_StrLen(t *testing.T) {
	db := ReopenDb()
	//defer db.Close()

	length := db.StrLen([]byte("test_key_26385"))
	t.Log(length)
	t.Log(len([]byte("test_value_121294_abcd")))
}

func writeLargeData(db *RoseDB, t *testing.T) {
	keyPrefix := "test_key_"
	valPrefix := "test_value_"
	rand.Seed(time.Now().Unix())

	start := time.Now()
	for i := 0; i < 100000; i++ {
		key := keyPrefix + strconv.Itoa(rand.Intn(1000000))
		val := valPrefix + strconv.Itoa(rand.Intn(1000000))

		err := db.Set([]byte(key), []byte(val))
		if err != nil {
			t.Error("数据写入发生错误 ", err)
		}
	}
	t.Log("time spent : ", time.Since(start).Milliseconds())

	t.Log("写入的有效数据量 : ", db.idxList.Len)
}

package rosedb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
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

		// both nil
		db.Set(nil, nil)

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

		for i := 0; i < 250000; i++ {
			key := "k---" + strconv.Itoa(rand.Intn(100000))
			val := "v---" + strconv.Itoa(rand.Intn(100000))
			err := db.Set([]byte(key), []byte(val))
			if err != nil {
				log.Println("数据写入发生错误 ", err)
			}
		}
	})
}

func TestRoseDB_SetNx(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	_ = db.Set([]byte("test_key"), []byte("test_value"))
	result, _ := db.SetNx([]byte("test_key"), []byte("value_001"))
	if result != 0 {
		t.Fatal("SetNx result error")
	}
	_, _ = db.SetNx([]byte("test_key_new11111111111"), []byte("value_002"))

	val1, _ := db.Get([]byte("test_key"))
	if string(val1) != "test_value" {
		t.Fatal("set and get not equals")
	}
}

func TestRoseDB_Get(t *testing.T) {

	t.Run("normal situation", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		db.Get(nil)
		db.Get([]byte("hahahaha"))

		val, err := db.Get([]byte("test_key001"))
		if err != nil {
			log.Fatal("get val error : ", err)
		}

		t.Log(string(val))

		val, _ = db.Get([]byte("test_key002"))
		t.Log(string(val))

		val, _ = db.Get([]byte("test_key"))
		t.Log(string(val))
	})

	t.Run("reopen and get", func(t *testing.T) {
		db := ReopenDb()

		val, _ := db.Get([]byte("test_key"))
		log.Println(string(val))
	})

	//t.Run("large data", func(t *testing.T) {
	//	now := time.Now()
	//	db := ReopenDb()
	//	t.Log("reopen db time spent : ", time.Since(now))
	//
	//	defer db.Close()
	//
	//	val, _ := db.Get([]byte("test_key_001"))
	//	t.Log(string(val))
	//
	//	val, _ = db.Get([]byte("test_key_534647"))
	//	t.Log(string(val))
	//
	//	val, _ = db.Get([]byte("test_key_378893"))
	//	t.Log(string(val))
	//})
}

func TestRoseDB_GetSet(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	db.Set([]byte("test_get_set"), []byte("test_get_set_val"))
	val, err := db.GetSet([]byte("test_key001"), []byte("test_new_val_001"))
	if err != nil {
		log.Fatal(err)
	}
	t.Log("original val : ", string(val))

	val, _ = db.Get([]byte("test_key001"))
	t.Log("new val : ", string(val))

	db.GetSet(nil, nil)

	db.GetSet([]byte("test_key004"), nil)
}

func TestRoseDB_Append(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	db.Append(nil, nil)
	db.Append([]byte("test_not_exist"), []byte(" whatever"))

	t.Run("not exist", func(t *testing.T) {
		key, val := []byte("my_name"), []byte("roseduan")
		err := db.Append(key, val)
		assert.Equal(t, err, nil)

		v, _ := db.Get(key)
		assert.Equal(t, v, val)
	})

	t.Run("exist", func(t *testing.T) {
		key, val := []byte("my_age"), []byte("24444444")
		err := db.Set(key, val)
		assert.Equal(t, err, nil)

		err = db.Append(key, []byte("---rosedb"))
		assert.Equal(t, err, nil)
	})
}

func TestRoseDB_StrLen(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	db.StrLen(nil)

	length := db.StrLen([]byte("test_key_26385"))
	t.Log(length)
	t.Log(len([]byte("test_value_121294_abcd")))

	db.Set([]byte("111"), []byte("222"))
	//assert.Equal(t, db.StrLen([]byte("111")), 3)
	t.Log(db.StrLen([]byte("111")))

	key, val := []byte("my_age"), []byte("24444444")
	db.Set(key, val)
	t.Log(db.StrLen(key))
}

func TestRoseDB_StrExists(t *testing.T) {
	db := InitDb()
	defer db.Close()

	assert.Equal(t, db.StrExists(nil), false)

	exists := db.StrExists([]byte("my_age"))
	assert.Equal(t, exists, true)

	exist2 := db.StrExists([]byte("111111--22"))
	assert.Equal(t, exist2, false)
}

func TestRoseDB_StrRem(t *testing.T) {
	db := InitDb()
	defer db.Close()

	db.Remove(nil)
	_ = db.Remove([]byte("my_age"))

	key := []byte("bb-aa")
	db.Set(key, []byte("rosedb"))
	db.Remove(key)

	_, err := db.Get([]byte("my_age"))
	assert.Equal(t, err, ErrKeyNotExist)
	_, err = db.Get(key)
	assert.Equal(t, err, ErrKeyNotExist)
}

func TestRoseDB_PrefixScan(t *testing.T) {
	db := InitDb()
	defer db.Close()

	db.Set([]byte("ac"), []byte("3"))
	db.Set([]byte("aa"), []byte("1"))
	db.Set([]byte("ae"), []byte("4"))
	db.Set([]byte("ar"), []byte("6"))
	db.Set([]byte("ba"), []byte("7"))
	db.Set([]byte("ab"), []byte("2"))
	db.Set([]byte("af"), []byte("5"))

	db.PrefixScan("", 0, 0)

	findPrefix := func(limit, offset int) {
		values, err := db.PrefixScan("a", limit, offset)
		if err != nil {
			log.Fatal(err)
		}

		t.Logf("-----find prefix--- limit: %d, offset: %d -----", limit, offset)
		if len(values) > 0 {
			for _, v := range values {
				t.Log(string(v))
			}
		}
	}

	findPrefix(-1, 0)
	findPrefix(0, 0)
	findPrefix(2, 0)
	findPrefix(2, 2)
	findPrefix(2, -1)
}

func TestRoseDB_RangeScan(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	_ = db.Set([]byte("100054"), []byte("ddfd"))
	_ = db.Set([]byte("100009"), []byte("dfad"))
	_ = db.Set([]byte("100007"), []byte("rrwe"))
	_ = db.Set([]byte("100011"), []byte("eeda"))
	_ = db.Set([]byte("100023"), []byte("ghtr"))
	_ = db.Set([]byte("100056"), []byte("yhtb"))

	db.RangeScan(nil, nil)

	val, err := db.RangeScan([]byte("100007"), []byte("100030"))
	if err != nil {
		log.Fatal(err)
	}

	if len(val) > 0 {
		for _, v := range val {
			t.Log(string(v))
		}
	}
}

func TestRoseDB_Expire(t *testing.T) {
	db := InitDb()
	defer db.Close()

	key1 := []byte("key-1")
	db.Set(key1, []byte("val-1"))

	err := db.Expire(key1, 100)
	assert.Equal(t, err, nil)

	printTTL := func(key []byte) {
		t.Log(db.TTL(key1))
	}

	for i := 0; i < 10; i++ {
		printTTL(key1)
		time.Sleep(2 * time.Second)
	}
}

func TestRoseDB_Persist(t *testing.T) {
	db := InitDb()
	defer db.Close()

	key := []byte("my_hobby")
	_ = db.Append(key, []byte("Coding and reading"))

	db.Expire(key, 10)
	time.Sleep(2 * time.Second)

	t.Log(db.TTL(key))
	//
	err := db.Persist(key)
	assert.Equal(t, err, nil)
	val, err := db.Get(key)
	t.Log(err)
	t.Log("val = ", string(val))
}

func TestRoseDB_TTL(t *testing.T) {
	db := InitDb()
	defer db.Close()

	assert.Equal(t, db.TTL([]byte("1")), int64(0))
	assert.Equal(t, db.TTL([]byte("12323")), int64(0))

	key := []byte("my_hobby")
	_ = db.Append(key, []byte("Coding and reading"))

	db.Expire(key, 10)
	//time.Sleep(4 * time.Second)

	t.Log(db.TTL(key))
}

func TestRoseDB_Expire2(t *testing.T) {
	db := InitDb()
	defer db.Close()

	exKey := []byte("ex_key_001")

	//db.Set(exKey, []byte("111"))
	//
	//db.Expire(exKey, 20)
	//time.Sleep(4 * time.Second)
	//
	//t.Log(db.TTL(exKey))
	//db.Append(exKey, []byte(" some thing"))
	//
	//time.Sleep(2 * time.Second)
	//t.Log(db.TTL(exKey))

	v, _ := db.Get(exKey)
	fmt.Println(string(v))
}

func TestDoSet(t *testing.T) {
	db := InitDb()
	defer db.Close()

	key := []byte("str key")
	err := db.Set(key, []byte("Jack And Me"))
	if err != nil {
		log.Println(err)
	}

	db.Append(key, []byte(" append some val"))

	val, err := db.Get(key)
	if err != nil {
		log.Println(err)
	}
	fmt.Println("val = ", string(val))
}

func TestRoseDB_Get2(t *testing.T) {
	config := DefaultConfig()
	db, err := Open(config)
	if err != nil {
		log.Println(err)
		return
	}

	defer db.Close()

	db.Set([]byte("kkkkkk"), []byte("kkkkkk"))
	db.Get([]byte("kkkkkk"))
	db.PrefixScan("kk", 10, 0)
	val, _ := db.RangeScan([]byte("kkkkkk"), []byte("kkkkkk"))
	for _, v := range val {
		t.Log(string(v))
	}

	db.Set([]byte("for_ttl"), []byte("for_ttl_val"))
	db.Expire([]byte("for_ttl"), 1)
	time.Sleep(2 * time.Second)
	db.Get([]byte("for_ttl"))
}

func writeLargeData(db *RoseDB, t *testing.T) {
	keyPrefix := "test_key_"
	valPrefix := "test_value_"
	rand.Seed(time.Now().Unix())

	start := time.Now()
	for i := 0; i < 5000000; i++ {
		key := keyPrefix + strconv.Itoa(rand.Intn(100000))
		val := valPrefix + strconv.Itoa(rand.Intn(100000))

		err := db.Set([]byte(key), []byte(val))
		if err != nil {
			t.Error("数据写入发生错误 ", err)
		}
	}
	t.Log("time spent : ", time.Since(start).Milliseconds())

	t.Log("写入的有效数据量 : ", db.strIndex.idxList.Len)
}

func TestOpen3(t *testing.T) {
	config := DefaultConfig()
	config.IdxMode = KeyOnlyMemMode
	config.DirPath = "bench/rosedb"
	db, err := Open(config)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	writeLargeData(db, t)
}

func TestRoseDB_Reclaim2(t *testing.T) {
	db, _ := Reopen("bench/rosedb")
	defer db.Close()

	start := time.Now()
	err := db.Reclaim()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("reclaim time spent: ", time.Since(start))

	t.Log("valid keys: ", db.strIndex.idxList.Len)
}

func TestRoseDB_SetEx(t *testing.T) {
	config := DefaultConfig()
	config.DirPath = "/tmp/rosedb"
	db, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("kk11")
	//err = db.SetEx(key, []byte("mmpp"), 100)
	//t.Log(err)

	//time.Sleep(5 * time.Second)
	//db.Persist(key)

	v, err := db.Get(key)
	t.Log(string(v), err)

	ttl := db.TTL(key)
	t.Log(ttl)

	ttl = db.TTL(key)
	t.Log(ttl)
}

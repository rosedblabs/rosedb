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

func TestRoseDB_SetNx(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	_ = db.Set([]byte("test_key"), []byte("test_value"))
	_ = db.SetNx([]byte("test_key"), []byte("value_001"))
	_ = db.SetNx([]byte("test_key_new"), []byte("value_002"))

	val1, _ := db.Get([]byte("test_key"))
	val2, _ := db.Get([]byte("test_key_new"))
	t.Log(string(val1))
	t.Log(string(val2))
}

func TestRoseDB_Get(t *testing.T) {

	t.Run("normal situation", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

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
		t.Log("reopen db...")

		val, err := db.Get([]byte("test_key_924252"))
		if err != nil {
			log.Fatal("get val error : ", err)
		}

		t.Log(string(val))

		val, _ = db.Get([]byte("test_key_470054"))
		//test_value_135824
		t.Log(string(val))

		//test_value_63214
		val, _ = db.Get([]byte("test_key_648543"))
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

	val, err := db.GetSet([]byte("test_key001"), []byte("test_new_val_001"))
	if err != nil {
		log.Fatal(err)
	}
	t.Log("original val : ", string(val))

	val, _ = db.Get([]byte("test_key001"))
	t.Log("new val : ", string(val))
}

func TestRoseDB_Append(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	//test_value_746656
	err := db.Append([]byte("test_key_747172"), []byte(" some bug"))
	if err != nil {
		t.Log(err)
	}

	val, _ := db.Get([]byte("test_key_747172"))
	t.Log(string(val))
}

func TestRoseDB_StrLen(t *testing.T) {
	db := ReopenDb()
	//defer db.Close()

	length := db.StrLen([]byte("test_key_26385"))
	t.Log(length)
	t.Log(len([]byte("test_value_121294_abcd")))
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

	findPrefix := func(limit, offset int) {
		values, err := db.PrefixScan("a", limit, offset)
		if err != nil {
			log.Fatal(err)
		}

		if len(values) > 0 {
			for _, v := range values {
				t.Log(string(v))
			}
		}
	}

	//findPrefix(-1, 0)
	//findPrefix(2, 0)
	//findPrefix(2, 2)
	//findPrefix(1, 3)
	findPrefix(1, 20)
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
	db := ReopenDb()
	defer db.Close()

	//_ = db.Set([]byte("test_key_004"), []byte("test_val_004"))
	//_ = db.Set([]byte("test_key_005"), []byte("test_val_005"))
	//_ = db.Set([]byte("test_key_006"), []byte("test_val_006"))
	//
	//if err := db.Expire([]byte("test_key_005"), 50); err != nil {
	//	log.Println("set expire err : ", err)
	//}

	key := []byte("test_key_005")
	desc := func() {
		ttl := db.TTL(key)
		t.Log(ttl)
	}

	val, _ := db.Get(key)
	t.Log("val = ", string(val))

	desc()

	time.Sleep(2 * time.Second)
	desc()

	time.Sleep(2 * time.Second)
	desc()

	time.Sleep(2 * time.Second)
	desc()
}

func TestRoseDB_Persist(t *testing.T) {
	db := InitDb()
	defer db.Close()

	key := []byte("my_name4")
	_ = db.Append(key, []byte("I am roseduan "))

	db.Expire(key, 10)

	time.Sleep(3 * time.Second)
	t.Log(db.TTL(key))

	time.Sleep(3 * time.Second)
	t.Log(db.TTL(key))

	val, err := db.Get(key)
	t.Log(err)
	t.Log("val = ", string(val))

	db.Persist(key)
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

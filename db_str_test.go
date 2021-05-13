package rosedb

import (
	"fmt"
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

		for i := 0; i < 100000; i++ {
			key := "k---" + strconv.Itoa(rand.Intn(1000))
			val := "v---" + strconv.Itoa(rand.Intn(1000))
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
	_ = db.SetNx([]byte("test_key"), []byte("value_001"))
	_ = db.SetNx([]byte("test_key_new11111111111"), []byte("value_002"))

	val1, _ := db.Get([]byte("test_key"))
	val2, _ := db.Get([]byte("test_key_new11111111111"))
	t.Log(string(val1))
	t.Log(string(val2))
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
		t.Log("reopen db...")

		val, _ := db.Get([]byte("test_key_924252"))
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
	db.Append([]byte("test_not_exist"), []byte(" some bug"))

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
	defer db.Close()

	db.StrLen(nil)

	length := db.StrLen([]byte("test_key_26385"))
	t.Log(length)
	t.Log(len([]byte("test_value_121294_abcd")))

	db.Set([]byte("111"), []byte("222"))
	db.StrLen([]byte("111"))
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
	db := ReopenDb()
	defer db.Close()

	db.Expire([]byte("test_1111"), 10)
	db.Expire([]byte("test_key_004"), 0)

	_ = db.Set([]byte("test_key_004"), []byte("test_val_004"))
	_ = db.Set([]byte("test_key_005"), []byte("test_val_005"))
	_ = db.Set([]byte("test_key_006"), []byte("test_val_006"))

	if err := db.Expire([]byte("test_key_005"), 50); err != nil {
		log.Println("set expire err : ", err)
	}

	db.Expire([]byte("test_key_005"), 1)
	time.Sleep(1200 * time.Millisecond)
	db.Get([]byte("test_key_005"))

	//key := []byte("test_key_005")
	//desc := func() {
	//	ttl := db.TTL(key)
	//	t.Log(ttl)
	//}
	//
	//val, _ := db.Get(key)
	//t.Log("val = ", string(val))
	//
	//desc()
	//
	//time.Sleep(2 * time.Second)
	//desc()
	//
	//time.Sleep(2 * time.Second)
	//desc()
	//
	//time.Sleep(2 * time.Second)
	//desc()
}

func TestRoseDB_Persist(t *testing.T) {
	db := InitDb()
	defer db.Close()

	key := []byte("my_name4")
	_ = db.Append(key, []byte("I am roseduan "))

	db.Expire(key, 10)
	db.Persist(key)

	//time.Sleep(3 * time.Second)
	//t.Log(db.TTL(key))
	//
	//time.Sleep(3 * time.Second)
	//t.Log(db.TTL(key))
	//
	//val, err := db.Get(key)
	//t.Log(err)
	//t.Log("val = ", string(va
	//
	//db.Persist(key)
}

func TestRoseDB_Expire2(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	exKey := []byte("ex_key_001")
	db.Set(exKey, []byte("111"))

	db.Expire(exKey, 1)
	time.Sleep(1200 * time.Millisecond)
	db.TTL(exKey)
	db.Get(exKey)
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

func TestRoseDB_StrExists(t *testing.T) {
	db := InitDb()
	defer db.Close()

	db.StrExists(nil)
	_ = db.StrExists([]byte("11111"))
}

func TestRoseDB_StrRem(t *testing.T) {
	db := InitDb()
	defer db.Close()

	db.StrRem(nil)
	_ = db.StrRem([]byte("11111"))

	key := []byte("bb-aa")
	db.Set(key, []byte("rosedb"))
	db.StrRem(key)
}

func TestRoseDB_TTL(t *testing.T) {
	db := InitDb()
	defer db.Close()

	db.TTL([]byte("1"))
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
	config.IdxMode = KeyOnlyRamMode
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

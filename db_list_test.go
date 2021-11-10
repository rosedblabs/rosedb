package rosedb

import (
	"github.com/roseduan/rosedb/ds/list"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func TestRoseDB_LPush(t *testing.T) {

	t.Run("normal situation", func(t *testing.T) {
		db := InitDb()
		defer db.Close()

		key := []byte("mylist")
		// empty
		_, _ = db.LPush(nil)

		res, err := db.LPush(key, []byte("list_data_001"), []byte("list_data_002"), []byte("list_data_003"))
		if err != nil {
			log.Fatal(err)
		}

		t.Log(res)
	})

	t.Run("reopen and lpush", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		key := []byte("mylist")
		res, err := db.LPush(key, []byte("list_data_004"), []byte("list_data_005"), []byte("list_data_006"))
		if err != nil {
			log.Fatal(err)
		}
		t.Log(res)
	})

	t.Run("multi data insert", func(t *testing.T) {
		db := InitDb()
		defer db.Close()

		key := []byte("mylist")
		// empty
		_, _ = db.LPush(nil)

		res, err := db.LPush(key, []byte("list_data_001"), []byte("list_data_002"), []byte("list_data_003"), "123", 1, false, "2222-333")
		if err != nil {
			log.Fatal(err)
		}

		t.Log(res)
	})
	//
	//t.Run("large data", func(t *testing.T) {
	//	db := ReopenDb()
	//	defer db.Close()
	//
	//	rand.Seed(time.Now().Unix())
	//
	//	key := []byte("mylist")
	//	valPrefix := "list_data_"
	//
	//	var flag = 99990
	//	for i := 0; i < 100000; i++ {
	//		val := valPrefix + strconv.Itoa(rand.Intn(1000000))
	//		_, err := db.LPush(key, []byte(val))
	//		if err != nil {
	//			log.Fatal(err)
	//		}
	//
	//		if i > flag {
	//			t.Log(val)
	//		}
	//	}
	//
	//	t.Log(db.listIndex.indexes.LLen(string(key)))
	//})
}

func TestRoseDB_LPop(t *testing.T) {
	key := []byte("mylist")

	t.Run("normal", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		val, _ := db.LPop(key)
		t.Log(string(val))
	})

	//t.Run("multi data", func(t *testing.T) {
	//	db := ReopenDb()
	//	defer db.Close()
	//
	//	for i := 0; i < 10; i++ {
	//		val, err := db.LPop(key)
	//		if err != nil {
	//			t.Fatal(err)
	//		}
	//
	//		t.Log(string(val))
	//	}
	//})
}

func TestRoseDB_RPush(t *testing.T) {

	t.Run("normal situation", func(t *testing.T) {
		db := InitDb()
		defer db.Close()

		_, _ = db.RPush(nil)

		key := []byte("mylist")
		_, err := db.RPush(key, []byte("list_data_001"), []byte("list_data_002"), []byte("list_data_003"))
		if err != nil {
			log.Fatal(err)
		}
	})

	t.Run("reopen and rpush", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		key := []byte("mylist")
		_, err := db.RPush(key, []byte("list_data_004"), []byte("list_data_005"), []byte("list_data_006"))
		if err != nil {
			log.Fatal(err)
		}
	})

	t.Run("multi data insert", func(t *testing.T) {
		db := InitDb()
		defer db.Close()

		key := []byte("mylist")
		// empty
		_, _ = db.RPush(nil)

		res, err := db.RPush(key, []byte("list_data_001"), []byte("list_data_002"), []byte("list_data_003"), "123", 1, false, "2222-333")
		if err != nil {
			log.Fatal(err)
		}

		t.Log(res)
	})
	//
	//t.Run("large data", func(t *testing.T) {
	//	db := ReopenDb()
	//	defer db.Close()
	//
	//	rand.Seed(time.Now().Unix())
	//
	//	key := []byte("mylist")
	//	valPrefix := "list_data_"
	//
	//	for i := 0; i < 100000; i++ {
	//		val := valPrefix + strconv.Itoa(rand.Intn(1000000))
	//		_, err := db.RPush(key, []byte(val))
	//		if err != nil {
	//			log.Fatal(err)
	//		}
	//
	//		if i >= 99990 {
	//			t.Log(val)
	//		}
	//	}
	//
	//	t.Log(db.listIndex.indexes.LLen(string(key)))
	//})
}

func TestRoseDB_RPop(t *testing.T) {
	key := []byte("mylist")

	t.Run("normal", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		val, _ := db.RPop(key)
		t.Log(string(val))
	})

	t.Run("multi", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		for i := 0; i < 10; i++ {
			val, _ := db.RPop(key)
			t.Log(string(val))
		}
	})
}

func TestRoseDB_LIndex(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	key := []byte("mylist")

	t.Log(db.LLen(key))

	val := db.LIndex(key, 0)
	t.Log(string(val))
	t.Log(string(db.LIndex(key, 1)))
	t.Log(string(db.LIndex(key, 100)))
	t.Log(string(db.LIndex(key, 5)))
	t.Log(string(db.LIndex(key, -3)))
}

func TestRoseDB_LRange(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	key := []byte("mylist")

	db.LRange(nil, 0, -1)

	lrange := func(start, stop int) {
		vals, err := db.LRange(key, start, stop)
		if err != nil {
			log.Fatal(err)
		}

		for _, v := range vals {
			t.Log(string(v))
		}
	}
	lrange(0, -1)
	lrange(-12, -1)
	lrange(50, 100)
}

func TestRoseDB_LRem(t *testing.T) {

	t.Run("normal situation", func(t *testing.T) {
		db := InitDb()
		defer db.Close()

		key := []byte("mylist")
		_, err := db.RPush(key, []byte("list_data_0011"), []byte("list_data_0022"), []byte("list_data_0033"), "123", 11, 22)
		if err != nil {
			log.Fatal(err)
		}

		res, err := db.LRem(key, []byte("list_data_0022"), 0)
		if err != nil {
			log.Fatal(err)
		}

		t.Log(res)

		res, err = db.LRem(key, "123", 0)
		if err != nil {
			log.Fatal(err)
		}

		t.Log(res)

	})

	t.Run("reopen", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		key := []byte("mylist")

		vals, err := db.LRange(key, 0, -1)
		if err != nil {
			log.Fatal(err)
		}

		for _, v := range vals {
			t.Log(string(v))
		}
	})
}

func TestRoseDB_LInsert(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	_, _ = db.LInsert("", list.Before, nil, nil)
	db.LInsert("123", list.After, []byte("11\\0"), nil)

	key := []byte("mylist")
	count, err := db.LInsert(string(key), list.Before, []byte("list_data_0011"), []byte("I am roseduan"))
	if err != nil {
		log.Fatal(err)
	}
	t.Log(count)

	vals, _ := db.LRange(key, 0, -1)
	for _, v := range vals {
		t.Log(string(v))
	}
}

func TestRoseDB_LSet(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	key := []byte("mylist")

	db.LSet(nil, 0, nil)

	ok, err := db.LSet(key, 0, []byte("list_data_new_001"))
	t.Log(ok, err)
	ok, err = db.LSet(key, 5, []byte("list_data_new_005"))
	t.Log(ok, err)
	ok, err = db.LSet(key, -3, []byte("list_data_new_00-3"))
	t.Log(ok, err)
	ok, err = db.LSet(key, -2, "123")
	t.Log(ok, err)
	ok, err = db.LSet(key, -2, 2222)
	t.Log(ok, err)
}

func TestRoseDB_LTrim(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	key := []byte("mylist")

	err := db.LTrim(key, 0, 0)
	if err != nil {
		t.Log(err)
	}
}

func TestRoseDB_LLen(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	key := []byte("mylist")
	db.LLen(key)
}

func TestRoseDB_LKeyExists(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	key := []byte("mylist")
	ok := db.LKeyExists(key)
	t.Log(ok)

	ok1 := db.LKeyExists([]byte("aaa"))
	t.Log(ok1)
}

func TestRoseDB_LClear(t *testing.T) {
	err := setup()
	assert.NoError(t, err)
	db := InitDb()
	defer db.Close()

	key := []byte("my_list")
	res, err := db.LPush(key, []byte("val-1"), []byte("val-2"), []byte("val-3"), "123", "222", 111)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, 6)

	err = db.LClear(key)
	assert.Equal(t, err, nil)
}

func TestRoseDB_LExpire(t *testing.T) {
	err := setup()
	assert.NoError(t, err)
	db := InitDb()
	defer db.Close()

	key := []byte("my_list")
	res, err := db.LPush(key, []byte("val-1"), []byte("val-2"), []byte("val-3"), "123", "222", 111)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, 6)

	err = db.LExpire(key, 300)
	assert.Equal(t, err, nil)
}

func TestRoseDB_LTTL(t *testing.T) {
	err := setup()
	assert.NoError(t, err)
	db := InitDb()
	defer db.Close()

	key := []byte("my_list")
	res, err := db.LPush(key, []byte("val-1"), []byte("val-2"), []byte("val-3"), "123", "222", 111)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, 6)

	db.LExpire(key, 20)
	t.Log(db.LTTL(key))
	//for i := 0; i < 5; i++ {
	//	time.Sleep(time.Second * 2)
	//	t.Log(db.LTTL(key))
	//}
}
func setup() error {
	err := os.RemoveAll(DefaultDirPath)
	return err
}

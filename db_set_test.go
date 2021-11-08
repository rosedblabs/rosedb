package rosedb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRoseDB_SAdd(t *testing.T) {
	var key = "my_set"

	var multi = "multi_set"

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

	t.Run("multi situation ", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		res, _ := db.SAdd([]byte(multi), []byte("set_data_010"), []byte("set_data_008"), 1, true, "rosedb", 0)
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
	t.Run("normal situation ", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()
		var key = []byte("my_set")

		db.SPop(nil, 3)
		values, _ := db.SPop(key, 2)
		for _, v := range values {
			t.Log(string(v))
		}
	})

	t.Run("multi situation", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()
		var key = []byte("multi_set")

		values, _ := db.SPop(key, 6)
		for _, v := range values {
			t.Log(string(v))
		}
	})
}

func TestRoseDB_SCard(t *testing.T) {
	t.Run("normal situation ", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()
		var key = []byte("my_set")

		db.SCard(nil)

		card := db.SCard(key)
		t.Log(card)

		card1 := db.SCard([]byte("not exist"))
		t.Log(card1)
	})
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

	var keys = []interface{}{
		[]byte("my_set1"),
		[]byte("my_set2"),
	}

	var emptyKeys []interface{}
	db.SDiff(emptyKeys...)
	db.SDiff(keys...)
}

func TestRoseDB_SMove(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	var key1 = []byte("my_set1")
	var key2 = []byte("my_set2")

	db.SAdd(key1, []interface{}{[]byte("1")}...)
	db.SAdd(key2, []interface{}{[]byte("2")}...)

	db.SMove(nil, nil, nil)
	db.SMove(key1, key2, []byte("1"))
}

func TestRoseDB_SUnion(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	var keys = []interface{}{
		[]byte("my_set1"),
		[]byte("my_set2"),
	}

	var emptyKeys []interface{}
	db.SUnion(emptyKeys...)
	db.SUnion(keys...)
}

func TestRoseDB_SExpire(t *testing.T) {
	db := InitDb()
	defer db.Close()

	key := []byte("my_set_key")
	res, err := db.SAdd(key, []byte("set-val-4"), []byte("set-val-5"), []byte("set-val-6"))
	assert.Equal(t, err, nil)
	t.Log(res)

	//err = db.SExpire(key, 100)
	//assert.Equal(t, err, nil)

	t.Log(db.STTL(key))

	val := db.SMembers(key)
	t.Log(len(val))
}

func TestRoseDB_STTL(t *testing.T) {
	db := InitDb()
	defer db.Close()

	key := []byte("my_set_key")

	err := db.SExpire(key, 100)
	assert.Equal(t, err, nil)

	for i := 0; i < 5; i++ {
		t.Log(db.STTL(key))
		time.Sleep(time.Second * 2)
	}
}

func TestRoseDB_SKeyExists(t *testing.T) {
	db := InitDb()
	defer db.Close()

	key := []byte("my_set_key_2")
	res, err := db.SAdd(key, []byte("set-val-4"), []byte("set-val-5"), []byte("set-val-6"))
	assert.Equal(t, err, nil)
	t.Log(res)

	ok1 := db.SKeyExists(key)
	assert.Equal(t, ok1, true)

	ok2 := db.SKeyExists([]byte("qqqqqqqq"))
	assert.Equal(t, ok2, false)
}

func TestRoseDB_SClear(t *testing.T) {
	db := InitDb()
	defer db.Close()

	key := []byte("my_set_key_3")
	res, err := db.SAdd(key, []byte("set-val-4"), []byte("set-val-5"), []byte("set-val-6"))
	assert.Equal(t, err, nil)
	t.Log(res)

	err = db.SClear(key)
	t.Log(err)

	v := db.SMembers(key)
	assert.Equal(t, len(v), 0)
}

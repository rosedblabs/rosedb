package rosedb

import (
	"github.com/roseduan/rosedb/storage"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoseDB_Set(t *testing.T) {
	roseDB := InitDB(DefaultConfig())
	defer DestroyDB(roseDB)

	t.Run("1", func(t *testing.T) {
		tests := generateMultiTypesTestData()
		for _, tt := range tests {
			err := roseDB.Set(tt.key, tt.val)
			assert.Equal(t, err, nil)
		}
	})

	t.Run("2", func(t *testing.T) {
		// slice
		sli := []int{1, 3, 43}
		err := roseDB.Set(sli, "slice-val-1")
		assert.Equal(t, err, nil)

		// map
		m := map[string]interface{}{
			"m1": "a",
			"m2": "b",
		}

		err = roseDB.Set(m, "map-val-1")
		assert.Equal(t, err, nil)

		// struct
		sk1 := struct {
			f1 string
			f2 int
			f3 []byte
		}{
			"1", 23, []byte("aa"),
		}
		err = roseDB.Set(sk1, "struct-val-1")
		assert.Equal(t, err, nil)
	})
}

func TestRoseDB_Set_MMap(t *testing.T) {
	config := DefaultConfig()
	config.RwMethod = storage.MMap
	roseDB := InitDB(config)
	defer DestroyDB(roseDB)

	t.Run("1", func(t *testing.T) {
		tests := generateMultiTypesTestData()
		for _, tt := range tests {
			err := roseDB.Set(tt.key, tt.val)
			assert.Equal(t, err, nil)
		}
	})

	t.Run("2", func(t *testing.T) {
		// slice
		sli := []int{1, 3, 43}
		err := roseDB.Set(sli, "slice-val-1")
		assert.Equal(t, err, nil)

		// map
		m := map[string]interface{}{
			"m1": "a",
			"m2": "b",
		}

		err = roseDB.Set(m, "map-val-1")
		assert.Equal(t, err, nil)

		// struct
		sk1 := struct {
			f1 string
			f2 int
			f3 []byte
		}{
			"1", 23, []byte("aa"),
		}
		err = roseDB.Set(sk1, "struct-val-1")
		assert.Equal(t, err, nil)
	})
}

func TestRoseDB_SetNx(t *testing.T) {
	write := func(method storage.FileRWMethod) {
		config := DefaultConfig()
		config.RwMethod = method
		roseDB := InitDB(config)
		defer DestroyDB(roseDB)

		ok, err := roseDB.SetNx("set-nx-key-1", "nx-val-1")
		assert.Nil(t, err)
		assert.True(t, ok)

		ok1, err := roseDB.SetNx("set-nx-key-1", "nx-val-2")
		assert.Nil(t, err)
		assert.False(t, ok1)
	}

	t.Run("fileio", func(t *testing.T) {
		write(storage.FileIO)
	})

	t.Run("mmap", func(t *testing.T) {
		write(storage.MMap)
	})
}

func TestRoseDB_SetEx(t *testing.T) {
	write := func(method storage.FileRWMethod) {
		config := DefaultConfig()
		config.RwMethod = method
		roseDB := InitDB(config)
		defer DestroyDB(roseDB)

		err := roseDB.SetEx("set-ex-key-1", "ex-val-1", 10)
		assert.Nil(t, err)

		err = roseDB.SetEx("set-ex-key-2", "ex-val-2", 100)
		assert.Nil(t, err)

		ttl1 := roseDB.TTL("set-ex-key-1")
		ttl2 := roseDB.TTL("set-ex-key-2")
		t.Log(ttl1, ttl2)

		err = roseDB.SetEx("set-ex-key-3", "ex-val-3", -100)
		assert.Equal(t, err, ErrInvalidTTL)
	}

	write(storage.FileIO)
	write(storage.MMap)
}

func TestRoseDB_Get(t *testing.T) {
	testGet := func(method storage.FileRWMethod, cache bool) {
		config := DefaultConfig()
		config.RwMethod = method
		if cache {
			config.CacheCapacity = 100
		}
		roseDB := InitDB(config)
		defer DestroyDB(roseDB)

		tests := generateMultiTypesTestData()
		for _, tt := range tests {
			err := roseDB.Set(tt.key, tt.val)
			assert.Nil(t, err)
		}

		for _, tt := range tests {
			var i interface{}
			err := roseDB.Get(tt.key, &i)
			assert.Nil(t, err)
		}

		// test get from cache.
		if cache {
			for _, tt := range tests {
				var i interface{}
				err := roseDB.Get(tt.key, &i)
				assert.Nil(t, err)
			}
		}
	}

	t.Run("fileio", func(t *testing.T) {
		testGet(storage.FileIO, false)
	})

	t.Run("mmap", func(t *testing.T) {
		testGet(storage.MMap, false)
	})

	t.Run("with cache", func(t *testing.T) {
		testGet(storage.FileIO, true)
	})
}

func TestRoseDB_GetSet(t *testing.T) {
	getSet := func(method storage.FileRWMethod) {
		config := DefaultConfig()
		config.RwMethod = method
		roseDB := InitDB(config)
		defer DestroyDB(roseDB)

		err := roseDB.GetSet("get-set-key", "val-1", nil)
		assert.Nil(t, err)

		var r1 string
		err = roseDB.Get("get-set-key", &r1)
		assert.Nil(t, err)
		assert.Equal(t, r1, "val-1")

		var r2 string
		err = roseDB.GetSet("get-set-key", "val-2", &r2)
		assert.Nil(t, err)
		assert.Equal(t, r2, "val-1")

		var r3 string
		err = roseDB.Get("get-set-key", &r3)
		assert.Nil(t, err)
		assert.Equal(t, r3, "val-2")
	}

	t.Run("fileio", func(t *testing.T) {
		getSet(storage.FileIO)
	})

	t.Run("mmap", func(t *testing.T) {
		getSet(storage.MMap)
	})
}

func TestRoseDB_MSet(t *testing.T) {
	config := DefaultConfig()
	roseDB := InitDB(config)
	defer DestroyDB(roseDB)

	err := roseDB.MSet(1, 2, nil, 4)
	assert.Nil(t, err)

	// skip values.
	err = roseDB.MSet(1, 2, nil, 4)
	assert.Nil(t, err)
}

func TestRoseDB_MGet(t *testing.T) {
	config := DefaultConfig()
	roseDB := InitDB(config)
	defer DestroyDB(roseDB)

	tests := generateMultiTypesTestData()
	for _, tt := range tests {
		err := roseDB.Set(tt.key, tt.val)
		assert.Nil(t, err)
	}

	values, err := roseDB.MGet(true, false, nil, "str-key-1", 1)
	assert.Nil(t, err)

	for _, v := range values {
		assert.NotNil(t, v)
	}
}

func TestRoseDB_Append(t *testing.T) {
	config := DefaultConfig()
	roseDB := InitDB(config)
	defer DestroyDB(roseDB)

	t.Run("not exist", func(t *testing.T) {
		err := roseDB.Append("app-key-1", "app-val-1")
		assert.Nil(t, err)

		var r1 string
		err = roseDB.Get("app-key-1", &r1)
		assert.Nil(t, err)
		assert.Equal(t, r1, "app-val-1")
	})

	t.Run("exist", func(t *testing.T) {
		err := roseDB.Set("app-key-2", "app-val-2")
		assert.Nil(t, err)

		err = roseDB.Append("app-key-2", " append val")
		assert.Nil(t, err)

		var r2 string
		err = roseDB.Get("app-key-2", &r2)
		assert.Nil(t, err)

		assert.Equal(t, r2, "app-val-2 append val")
	})

	t.Run("not string", func(t *testing.T) {
		err := roseDB.Set("app-key-3", 12.222)
		assert.Nil(t, err)

		err = roseDB.Append("app-key-3", " append val-22")
		assert.Nil(t, err)

		var r3 string
		err = roseDB.Get("app-key-3", &r3)
		assert.Nil(t, err)
		t.Log(r3)
	})
}

func TestRoseDB_StrExists(t *testing.T) {
	strExist := func(cache bool) {
		config := DefaultConfig()
		if cache {
			config.CacheCapacity = 10
		}
		roseDB := InitDB(config)
		defer DestroyDB(roseDB)

		ok1 := roseDB.StrExists("exist-0001")
		assert.Equal(t, ok1, false)

		err := roseDB.Set("exist-0001", 100)
		assert.Nil(t, err)

		ok2 := roseDB.StrExists("exist-0001")
		assert.Equal(t, ok2, true)

		roseDB.Remove("exist-0001")

		ok3 := roseDB.StrExists("exist-0001")
		assert.Equal(t, ok3, false)
	}

	t.Run("no cache", func(t *testing.T) {
		strExist(false)
	})

	t.Run("with cache", func(t *testing.T) {
		strExist(true)
	})
}

func TestRoseDB_Remove(t *testing.T) {
	config := DefaultConfig()
	roseDB := InitDB(config)
	defer DestroyDB(roseDB)

	err := roseDB.Remove("remove-key-1")
	assert.Nil(t, err)

	var r1 string
	err = roseDB.Set("remove-key-2", &r1)
	assert.Nil(t, err)

	err = roseDB.Remove("remove-key-2")
	assert.Nil(t, err)
}

func TestRoseDB_Expire(t *testing.T) {
	config := DefaultConfig()
	roseDB := InitDB(config)
	defer DestroyDB(roseDB)

	t.Run("normal", func(t *testing.T) {
		err := roseDB.Set("expire-key-1", 11)
		assert.Nil(t, err)

		err = roseDB.Expire("expire-key-1", 60)
		assert.Equal(t, err, nil)

		ttl := roseDB.TTL("expire-key-1")
		assert.Equal(t, ttl, int64(60))
	})

	t.Run("not exist", func(t *testing.T) {
		err := roseDB.Expire("expire-key-2", 60)
		assert.Equal(t, err, ErrKeyNotExist)

		err = roseDB.Expire(123, -100)
		assert.Equal(t, err, ErrInvalidTTL)
	})
}

func TestRoseDB_Persist(t *testing.T) {
	config := DefaultConfig()
	roseDB := InitDB(config)
	defer DestroyDB(roseDB)

	err := roseDB.Persist(111)
	assert.Equal(t, err, ErrKeyNotExist)

	err = roseDB.SetEx(111, 123, 100)
	assert.Equal(t, err, nil)

	err = roseDB.Persist(111)
	assert.Equal(t, err, nil)
}

func TestRoseDB_TTL(t *testing.T) {
	config := DefaultConfig()
	roseDB := InitDB(config)
	defer DestroyDB(roseDB)

	t.Run("1", func(t *testing.T) {
		err := roseDB.SetEx("k1", 12333, 20)
		assert.Equal(t, err, nil)

		//time.Sleep(3 * time.Second)
		ttl := roseDB.TTL("k1")
		assert.Equal(t, ttl, int64(20)) // 17
	})

	t.Run("2", func(t *testing.T) {
		ttl := roseDB.TTL("aaaaaaaa")
		assert.Equal(t, ttl, int64(0))
	})

	t.Run("3", func(t *testing.T) {
		k := []int{1, 4, 5}
		err := roseDB.Set(k, 20)
		assert.Equal(t, err, nil)

		var v int
		err = roseDB.Get(k, &v)
		assert.Equal(t, err, nil)
	})
}

func TestRoseDB_MSet2(t *testing.T) {
	config := DefaultConfig()
	roseDB := InitDB(config)
	defer DestroyDB(roseDB)

	t.Run("wrong number", func(t *testing.T) {
		err := roseDB.MSet("k1")
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrWrongNumberOfArgs)
	})

	t.Run("wrong key", func(t *testing.T) {
		err := roseDB.MSet("", "v1")
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrEmptyKey)
	})

	t.Run("wrong value", func(t *testing.T) {
		largeValue := strings.Builder{}
		// 9mb
		largeValue.Grow(int(DefaultMaxValueSize + DefaultMaxKeySize))
		for i := 0; i < int(DefaultMaxValueSize+DefaultMaxKeySize); i++ {
			largeValue.WriteByte('A')
		}

		err := roseDB.MSet("k3", largeValue.String())
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrValueTooLarge)
	})

	t.Run("success", func(t *testing.T) {
		err := roseDB.MSet("k1", "v1", "k2", 2)
		assert.Nil(t, err)
	})
}

type KeyValue struct {
	key interface{}
	val interface{}
}

func generateMultiTypesTestData() []KeyValue {
	tests := []KeyValue{
		// with nil value
		{nil, nil},
		{"set-key-1", nil},
		{nil, "set-val-1"},

		// with bool value.
		{true, 1232},
		{false, 1232},
		{"bool-key-1", true},
		{"bool-key-2", false},

		// int value.
		{1, 34.34},
		{-3921, 34.444},
		{uint8(123), 34.123},

		// float value.
		{float32(1.990), float32(9102.22)},
		{float32(5.4122), []byte("1")},
		{3.132, float64(4443)},

		// byte value.
		{[]byte("byte-key-1"), []byte("byte-val-1")},
		{[]byte("kk1"), []byte("rosedb1")},
		{[]byte("kk2"), []byte("rosedb2")},

		// string value.
		{"str-key-1", "str-value-1"},
		{"str-key-2", "str-value-2"},
		{"str-key-3", "str-value-3"},
	}
	return tests
}

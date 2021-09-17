package rosedb

import (
	"testing"

	"github.com/roseduan/rosedb/utils"
	"github.com/stretchr/testify/assert"
)

func TestRoseDB_Set(t *testing.T) {

	t.Run("1", func(t *testing.T) {
		tests := []struct {
			key interface{}
			val interface{}
		}{
			{nil, nil},
			{"aaa", nil},
			{nil, "bbb"},

			{[]byte("kk"), []byte("rosedb1")},
			{[]byte("kk"), []byte("rosedb2")},
			{[]byte("kk"), []byte("rosedb3")},
			{[]byte("kk1"), []byte("rosedb4")},
			{[]byte("kk2"), []byte("rosedb5")},

			{true, 1232},
			{true, 1232},

			{float32(4.4122), float32(9102.22)},
			{float32(4.4122), []byte("1")},

			{float64(3.132), float64(4443)},

			{"kk33", "a"},
			{"kk33", "b"},
			{"kk44", "c"},

			{1, 34},
			{-3921, 34},
		}

		for _, tt := range tests {
			err := roseDB.Set(tt.key, tt.val)
			assert.Equal(t, err, nil)
		}
	})

	t.Run("2", func(t *testing.T) {
		type KeyVal struct {
			Field1 []byte
			Field2 float64
			Field3 int
			Field4 string
		}

		tests := []KeyVal{
			{[]byte("a"), 343.33, 33, "rosedb"},
			{[]byte("b"), 343.33, 33, "rosedb"},
			{[]byte("c"), 343.33, 33, "rosedb"},
			{[]byte("d"), 343.33, 33, "rosedb"},
		}

		for _, tt := range tests {
			err := roseDB.Set(tt.Field1, tt)
			assert.Equal(t, err, nil)
		}

		for _, tt := range tests {
			err := roseDB.Set(tt, tt.Field1)
			assert.Equal(t, err, nil)
		}

		t.Log(roseDB.strIndex.idxList.Len)
	})
}

func TestRoseDB_SetNx(t *testing.T) {
	ok, err := roseDB.SetNx("set-nx", 1)
	assert.Equal(t, err, nil)
	assert.Equal(t, ok, true)

	ok, err = roseDB.SetNx("set-nx", 2)
	assert.Equal(t, err, nil)
	assert.Equal(t, ok, false)
}

func TestRoseDB_SetEx(t *testing.T) {
	err := roseDB.SetEx(934, 1, -4)
	assert.NotEmpty(t, err)

	err = roseDB.SetEx(934, 1, 993)
	assert.Empty(t, err)
}

func TestRoseDB_Get(t *testing.T) {

	t.Run("1", func(t *testing.T) {
		tests := []struct {
			key interface{}
			val interface{}
		}{
			{"aaa", nil},
			{nil, "bbb"},
			{[]byte("kk2"), []byte("rosedb5")},
			{true, 1232},
			{false, 1232},
			{float32(4.4122), float32(9102.22)},
			{"kk44", "c"},
			{1, 34},
		}

		for _, tt := range tests {
			err := roseDB.Set(tt.key, tt.val)
			assert.Equal(t, err, nil)
		}

		var v0 interface{}
		err := roseDB.Get(tests[0].key, &v0)
		assert.Empty(t, err)
		assert.Equal(t, v0, nil)

		var v1 string
		err = roseDB.Get(tests[1].key, &v1)
		assert.Empty(t, err)
		assert.Equal(t, v1, "bbb")

		var v2 int
		err = roseDB.Get(tests[7].key, &v2)
		assert.Empty(t, err)
		assert.Equal(t, v2, 34)
	})

	t.Run("2", func(t *testing.T) {
		type KeyVal struct {
			Field1 []byte
			Field2 float64
			Field3 int
			Field4 string
		}

		tests := []KeyVal{
			{[]byte("a"), 343.33, 33, "rosedb"},
		}

		err := roseDB.Set(tests[0], "rosedb")
		assert.Empty(t, err)

		var res string
		err = roseDB.Get(tests[0], &res)
		assert.Empty(t, err)
		assert.Equal(t, res, "rosedb")
	})
}

func TestRoseDB_GetSet(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		err := roseDB.Set(123, 456)
		assert.Empty(t, err)

		var res int
		err = roseDB.GetSet(123, 567, &res)
		assert.Empty(t, err)
		assert.Equal(t, res, 456)

		var r2 int
		err = roseDB.Get(123, &r2)
		assert.Empty(t, err)
		assert.Equal(t, r2, 567)
	})

	t.Run("2", func(t *testing.T) {
		var res interface{}
		err := roseDB.GetSet(123, 222, &res)
		assert.Equal(t, err, nil)
	})
}

func TestRoseDB_Append(t *testing.T) {

	t.Run("1", func(t *testing.T) {
		roseDB.Set("app-123", "11")

		// only support string.
		err := roseDB.Append("app-123", "666")
		assert.Equal(t, err, nil)

		var res string
		err = roseDB.Get("app-123", &res)
		assert.Equal(t, err, nil)
		t.Log(res)
	})

	t.Run("2", func(t *testing.T) {
		err := roseDB.Append(1555, "232")
		assert.Equal(t, err, nil)

		var res string
		err = roseDB.Get(1555, &res)
		assert.Equal(t, err, nil)
		t.Log(res)
	})
}

func TestRoseDB_StrExists(t *testing.T) {
	ok1 := roseDB.StrExists(1)
	assert.Equal(t, ok1, false)

	roseDB.Set(1, 100)

	ok2 := roseDB.StrExists(1)
	assert.Equal(t, ok2, true)
}

func TestRoseDB_Remove(t *testing.T) {
	err := roseDB.Remove(99932)
	assert.Equal(t, err, nil)

	roseDB.Set(1, 11)

	err = roseDB.Remove(1)
	assert.Equal(t, err, nil)

	var r int
	err = roseDB.Get(1, &r)
	t.Log(err)
}

func TestRoseDB_PrefixScan(t *testing.T) {
	roseDB.Set("acea", "1")
	roseDB.Set("aasd", "2")
	roseDB.Set("aesf", "3")
	roseDB.Set("arsg", "4")
	roseDB.Set("bagf", "5")
	roseDB.Set("aasb", "6")
	roseDB.Set("afbb", "7")

	val, _ := roseDB.PrefixScan("a", 3, 0)
	for _, v := range val {
		t.Log(string(v.([]byte)))
	}
}

func TestRoseDB_RangeScan(t *testing.T) {
	roseDB.Set("6", "1")
	roseDB.Set("4", "2")
	roseDB.Set("3", "3")
	roseDB.Set("8", "4")
	roseDB.Set("5", "5")
	roseDB.Set("9", "6")
	roseDB.Set("2", "7")

	val, err := roseDB.RangeScan("3", "7")
	t.Log(err)
	for _, v := range val {
		if vv, ok := v.([]byte); ok {
			t.Log(string(vv))
		}
	}
}

func TestRoseDB_Expire(t *testing.T) {

	t.Run("1", func(t *testing.T) {
		err := roseDB.Set(123, 444)
		assert.Equal(t, err, nil)

		err = roseDB.Expire(123, 100)
		assert.Equal(t, err, nil)

		//for i := 0; i < 10; i++ {
		//	time.Sleep(time.Second)
		//	t.Log(roseDB.TTL(123))
		//}
	})

	t.Run("2", func(t *testing.T) {
		err := roseDB.Expire("no-exist", 10)
		assert.Equal(t, err, ErrKeyNotExist)

		err = roseDB.Expire(123, -100)
		assert.Equal(t, err, ErrInvalidTTL)
	})
}

func TestRoseDB_Persist(t *testing.T) {
	err := roseDB.Persist(111)
	assert.Equal(t, err, ErrKeyNotExist)

	err = roseDB.SetEx(111, 123, 100)
	assert.Equal(t, err, nil)

	err = roseDB.Persist(111)
	assert.Equal(t, err, nil)
}

func TestRoseDB_TTL(t *testing.T) {

	t.Run("1", func(t *testing.T) {
		err := roseDB.SetEx("k1", 12333, 20)
		assert.Equal(t, err, nil)

		//time.Sleep(3 * time.Second)
		ttl := roseDB.TTL("k1")
		assert.Equal(t, ttl, 20) // 17
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
		t.Log(v)
	})
}

func TestRoseDB_MSet(t *testing.T) {
	t.Run("wrong number", func(t *testing.T) {
		err := roseDB.MSet("k1")
		assert.NotEmpty(t, err)
		assert.ErrorIs(t, err, ErrWrongNumberOfArgs)
	})

	t.Run("2", func(t *testing.T) {
		err := roseDB.MSet("k1", "v1", "k2", 2)
		assert.Empty(t, err)
	})
}

func TestRoseDB_MGet(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		err := roseDB.MSet("k1", "v1", "k2", 2)
		assert.Empty(t, err)

		vals, err := roseDB.MGet("k1", "k2")
		assert.Empty(t, err)
		assert.Equal(t, string(vals[0]), "v1")
		var i int
		err = utils.DecodeValue(vals[1], &i)
		assert.Empty(t, err)
		assert.Equal(t, i, 2)
	})
}

func BenchmarkRoseDB_MSet(b *testing.B) {
	b.ReportAllocs()

	values := make([]interface{}, 0, 20000)
	for i := 0; i < 10000; i++ {
		values = append(values, GetKey(i), GetValue())
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := roseDB.MSet(values...)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkRoseDB_MSetNormal(b *testing.B) {
	b.ReportAllocs()

	values := make([][]byte, 0, 20000)
	for i := 0; i < 10000; i++ {
		values = append(values, GetKey(i), GetValue())
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < len(values); j += 2 {
			err := roseDB.Set(values[j], values[j+1])
			if err != nil {
				panic(err)
			}
		}
	}
}

func BenchmarkRoseDB_Set(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := roseDB.Set(GetKey(i), GetValue())
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkRoseDB_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		roseDB.Set(GetKey(i), GetValue())
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var res interface{}
		err := roseDB.Get(GetKey(i), &res)
		if err != nil && err != ErrKeyNotExist {
			panic(err)
		}
	}
}

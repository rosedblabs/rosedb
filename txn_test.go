package rosedb

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

var roseDB *RoseDB

func init() {
	cfg := DefaultConfig()
	cfg.DirPath = "/tmp/rosedb"
	cfg.MergeThreshold = 1
	cfg.IdxMode = KeyOnlyMemMode
	now := time.Now()
	roseDB, _ = Open(cfg)
	fmt.Println("open time : ", time.Since(now).Milliseconds())
	rand.Seed(time.Now().Unix())
}

func TestTxn_Set(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		key := []byte("k1")
		val := []byte("val-1")
		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.Set(key, val)
			return err
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})

	t.Run("2", func(t *testing.T) {
		//err := roseDB.Txn(func(tx *Txn) error {
		//	for i := 0; i < 300000; i++ {
		//		err := tx.Set(GetKey(i), GetValue())
		//		if err != nil {
		//			return err
		//		}
		//	}
		//	return nil
		//})
		//if err != nil {
		//	t.Error("write data err ", err)
		//}
	})

	t.Run("3", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			err := tx.Set(1111111, "343")
			assert.Equal(t, err, nil)

			err = tx.Set("123", 9993)
			assert.Equal(t, err, nil)

			err = tx.Set(true, 9993)
			assert.Equal(t, err, nil)

			err = tx.Set(nil, 9993)
			assert.Equal(t, err, nil)
			err = tx.Set(nil, "jjjrrr")
			assert.Equal(t, err, nil)

			key := struct {
				Key   int
				Value string
			}{
				1, "aaa",
			}

			err = tx.Set(key, "rosedb")
			assert.Equal(t, err, nil)
			return nil
		})

		var r string
		_ = roseDB.Get(nil, &r)

		t.Log(r)
		count := roseDB.strIndex.idxList.Len
		assert.Equal(t, count, 5)
	})

	t.Run("4", func(t *testing.T) {
		tests := []struct {
			key   interface{}
			value interface{}
		}{
			{1.332, "12"},
			{1111, 33},
			{"rose", "jack"},
		}

		for _, tt := range tests {
			roseDB.Txn(func(tx *Txn) error {
				err := tx.Set(tt.key, tt.value)
				assert.Equal(t, err, nil)
				return err
			})
		}
	})
}

func TestTxn_SetNx(t *testing.T) {

	t.Run("1", func(t *testing.T) {
		key := []byte("k1")
		val := []byte("val-1")
		err := roseDB.Txn(func(tx *Txn) error {
			res, err := tx.SetNx(key, val)

			assert.Equal(t, res, true)
			return err
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})

	t.Run("2", func(t *testing.T) {
		roseDB.Set("k2", "v2")

		roseDB.Txn(func(tx *Txn) error {
			ok, err := tx.SetNx("k2", 2)
			assert.Equal(t, err, nil)
			assert.Equal(t, ok, false)
			return nil
		})
	})

	t.Run("3", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			tx.Set("k3", 3)
			ok, err := tx.SetNx("k3", 2)

			assert.Equal(t, err, nil)
			assert.Equal(t, ok, false)
			return nil
		})
	})
}

func TestTxn_Get(t *testing.T) {

	t.Run("1", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			err := tx.Set(123, 345)
			assert.Equal(t, err, nil)

			var r int
			err = tx.Get(123, &r)
			assert.Equal(t, err, nil)
			return nil
		})
	})

	t.Run("2", func(t *testing.T) {
		err := roseDB.Set(11, "2233")
		assert.Equal(t, err, nil)

		roseDB.Txn(func(tx *Txn) error {
			var r string
			err = tx.Get(11, &r)
			assert.Equal(t, err, nil)
			assert.Equal(t, r, "2233")
			return nil
		})
	})

	t.Run("3", func(t *testing.T) {
		key := struct {
			Key   int
			Value string
		}{
			1, "aaa",
		}
		roseDB.Set(key, "12345")

		roseDB.Txn(func(tx *Txn) error {
			var r string
			err := tx.Get(key, &r)
			assert.Equal(t, err, nil)
			assert.Equal(t, r, "12345")
			return nil
		})
	})
}

func TestTxn_GetSet(t *testing.T) {

	t.Run("1", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			val := struct {
				F1 int
				F2 string
			}{
				123, "111",
			}

			var r interface{}
			err := tx.GetSet("gs-001", val, &r)
			assert.Equal(t, err, nil)
			return nil
		})
	})

	t.Run("2", func(t *testing.T) {
		roseDB.Set("gs-002", "234")
		roseDB.Txn(func(tx *Txn) error {
			var r string
			err := tx.GetSet("gs-002", "abcd", &r)
			assert.Equal(t, err, nil)
			assert.Equal(t, r, "234")
			return nil
		})
	})

	t.Run("3", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			err := tx.GetSet("gs-003", "3", nil)
			assert.Equal(t, err, nil)

			var r1 string
			err = tx.Get("gs-003", &r1)
			assert.Equal(t, err, nil)
			assert.Equal(t, r1, "3")

			var r2 string
			err = tx.GetSet("gs-003", "new-3", &r2)
			assert.Equal(t, err, nil)
			assert.Equal(t, r2, "3")
			return nil
		})

		var r3 string
		err := roseDB.Get("gs-003", &r3)
		assert.Equal(t, err, nil)
		assert.Equal(t, r3, "new-3")
	})
}

func TestTxn_SetEx(t *testing.T) {

	t.Run("1", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.SetEx("ex-001", 9.45, 200)
			if err != nil {
				return err
			}

			var r float64
			err = tx.Get("ex-001", &r)
			assert.Equal(t, err, nil)
			assert.Equal(t, r, 9.45)
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})

	t.Run("2", func(t *testing.T) {
		roseDB.Set("ex-002", false)

		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.SetEx([]byte("ex-002"), true, 50)
			if err != nil {
				return err
			}

			var ok bool
			err = tx.Get("ex-002", &ok)
			assert.Equal(t, err, nil)
			assert.Equal(t, ok, true)
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})
}

func TestTxn_Append(t *testing.T) {
	key := []byte("app-1")
	val := "app-v-1"

	t.Run("1", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.Append(key, val)
			if err != nil {
				return err
			}

			var r string
			err = tx.Get(key, &r)
			assert.Equal(t, r, val)
			t.Log(r)
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})

	t.Run("2", func(t *testing.T) {
		roseDB.Set("app-2", "23")
		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.Append("app-2", " app-val")
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}

		roseDB.TxnView(func(tx *Txn) error {
			var r string
			err := tx.Get("app-2", &r)
			assert.Equal(t, err, nil)
			assert.Equal(t, r, "23 app-val")
			return nil
		})
	})
}

func TestTxn_StrExists(t *testing.T) {

	t.Run("1", func(t *testing.T) {
		key := []byte("se-001")

		err := roseDB.Txn(func(tx *Txn) error {
			ok := tx.StrExists(key)
			assert.Equal(t, ok, false)
			return nil
		})
		assert.Equal(t, err, nil)
	})

	t.Run("2", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			tx.Set("se-002", 1)

			ok := tx.StrExists("se-002")
			assert.Equal(t, ok, true)

			err := tx.Remove("se-002")
			assert.Equal(t, err, nil)

			ok1 := tx.StrExists("se-002")
			assert.Equal(t, ok1, false)
			return nil
		})
	})
}

func TestTxn_StrRem(t *testing.T) {

	t.Run("1", func(t *testing.T) {
		key := []byte("sr-001")
		err := roseDB.Txn(func(tx *Txn) error {
			strLen := tx.Remove(key)
			t.Log(strLen)
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})

	t.Run("2", func(t *testing.T) {
		key := []byte("sr-002")
		roseDB.Set(key, 1)
		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.Remove(key)
			assert.Equal(t, err, nil)

			err = tx.Get(key, nil)
			assert.Equal(t, err, ErrKeyNotExist)
			return nil
		})
		if err != nil {
			t.Log(err)
		}
	})

	t.Run("3", func(t *testing.T) {
		key := []byte("sr-003")
		err := roseDB.Txn(func(tx *Txn) error {
			tx.Set(key, []byte("roseduan"))

			err := tx.Remove(key)
			assert.Equal(t, err, nil)

			err = tx.Get(key, nil)
			assert.Equal(t, err, ErrKeyNotExist)
			return nil
		})
		if err != nil {
			t.Log(err)
		}
	})

	t.Run("4", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			err := tx.GetSet("sr-004", 2233, nil)
			assert.Equal(t, err, nil)

			err = tx.Remove("sr-004")
			assert.Equal(t, err, nil)
			return nil
		})
		err := roseDB.Get("sr-004", nil)
		assert.Equal(t, err, ErrKeyNotExist)
	})
}

func TestTxn_LPush(t *testing.T) {
	key := []byte("my_list")

	t.Run("temp", func(t *testing.T) {
		v, err := roseDB.RPop(key)
		assert.Equal(t, err, nil)
		t.Log(v)
	})

	t.Run("1", func(t *testing.T) {
		tests := []interface{}{
			123,
			34.12,
			true,
			[]byte("rosedb"),
			"roseduan",
			struct{}{},
		}

		err := roseDB.Txn(func(tx *Txn) error {
			for _, tt := range tests {
				err := tx.LPush(key, tt)
				assert.Equal(t, err, nil)
			}
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
		lLen := roseDB.LLen(key)
		assert.Equal(t, lLen, 6)
	})

}

func TestTxn_RPush(t *testing.T) {
	key := []byte("my_list_2")
	t.Run("1", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			tests := []interface{}{
				float32(134.3),
				nil,
				uint64(100),
				struct {
					F1 int
					F2 string
					F3 []byte
				}{123, "rosedb", []byte("nb")},
			}

			for _, tt := range tests {
				err := tx.RPush(key, tt)
				assert.Equal(t, err, nil)
			}
			return nil
		})
	})

	l := roseDB.LLen(key)
	t.Log(l)
}

func TestTxn_HSet(t *testing.T) {
	key := []byte("my_hash")

	t.Run("1", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			tests := []struct {
				Field interface{}
				Value interface{}
			}{
				{123, 3.44},
				{"h-v-1", "11"},
				{[]byte("h-v-1"), 2443},
				{true, 2443.23},
			}

			for _, tt := range tests {
				err := tx.HSet(key, tt.Field, tt.Value)
				assert.Equal(t, err, nil)
			}
			return nil
		})
	})
}

func TestTxn_HSetNx(t *testing.T) {

	t.Run("1", func(t *testing.T) {
		key := []byte("my_hash")
		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.HSetNx(key, "f1", "val--1")
			return err
		})
		assert.Equal(t, err, nil)

		vv := roseDB.HGet(key, []byte("f1"))
		t.Log(string(vv))
	})

	t.Run("2", func(t *testing.T) {
		key := []byte("my_hash")
		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.HSet(key, []byte("f1"), []byte("val--11"))
			if err != nil {
				return err
			}
			err = tx.HSetNx(key, []byte("f1"), []byte("val--22"))

			var v []byte
			err = tx.HGet(key, []byte("f1"), &v)
			assert.Equal(t, err, nil)
			assert.Equal(t, v, []byte("val--11"))
			return err
		})
		if err != nil {
			t.Log(err)
		}
	})

	t.Run("3", func(t *testing.T) {
		key := []byte("my_hash")
		roseDB.HSet(key, []byte("f3"), []byte("val--3"))
		err := roseDB.Txn(func(tx *Txn) error {
			tx.HSetNx(key, "f3", []byte("val--333"))
			tx.HDel(key, "f3")

			err := tx.HSetNx(key, "f3", []byte("val--444"))
			assert.Equal(t, err, nil)

			var v []byte
			err = tx.HGet(key, "f3", &v)
			assert.Equal(t, err, nil)
			t.Log(string(v))
			return nil
		})
		assert.Equal(t, err, nil)
	})
}

func TestTxn_HDel(t *testing.T) {
	key := []byte("my_hash")

	t.Run("1", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			err := tx.HDel(key, []byte("f1"))
			return err
		})
	})

	t.Run("2", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			tx.HSet(key, "f4", "val-4")
			tx.HDel(key, "f4")

			err := tx.HGet(key, "f4", nil)
			assert.Equal(t, err, nil)
			return nil
		})
	})

	t.Run("3", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			err := tx.HSetNx(key, "f5", "val-5")
			assert.Equal(t, err, nil)

			err = tx.HDel(key, "f5")
			assert.Equal(t, err, nil)
			return nil
		})
	})
}

func TestTxn_HGet(t *testing.T) {
	key := []byte("my_hash")

	t.Run("1", func(t *testing.T) {
		roseDB.HSet(key, []byte("f6"), []byte("val--6"))

		roseDB.Txn(func(tx *Txn) error {
			var r []byte
			err := tx.HGet(key, "f6", &r)
			assert.Equal(t, err, nil)
			t.Log(string(r))
			return nil
		})
	})

	t.Run("2", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			tx.HSet(key, "f6", []byte("rosedb777"))

			var r []byte
			err := tx.HGet(key, "f6", &r)
			assert.Equal(t, err, nil)
			t.Log(string(r))
			return nil
		})
	})
}

func TestTxn_HExists(t *testing.T) {
	key := []byte("my_hash")

	t.Run("1", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			ok := tx.HExists(key, 123.34)
			assert.Equal(t, ok, false)
			return nil
		})
		if err != nil {
			t.Log(err)
		}
	})

	t.Run("2", func(t *testing.T) {
		roseDB.HSet(key, []byte("f7"), []byte("val--7"))

		err := roseDB.Txn(func(tx *Txn) error {
			ok := tx.HExists(key, "f7")
			assert.Equal(t, ok, true)
			return nil
		})
		if err != nil {
			t.Log(err)
		}
	})

	t.Run("3", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			tx.HSet(key, "f8", 123.2)
			ok := tx.HExists(key, "f8")
			assert.Equal(t, ok, true)
			return nil
		})
		if err != nil {
			t.Log(err)
		}
	})
}

func TestTxn_SAdd(t *testing.T) {
	key := []byte("my_set")

	tests := []struct {
		Field interface{}
		Value interface{}
	}{
		{123, 1.23},
		{"aaa", []byte("jj")},
		{true, true},
		{nil, struct{}{}},
	}

	for _, tt := range tests {
		roseDB.Txn(func(tx *Txn) error {
			err := tx.SAdd(key, tt.Field, tt.Value)
			assert.Equal(t, err, nil)
			return err
		})
	}
}

func TestTxn_SIsMember(t *testing.T) {
	key := []byte("my_set")
	err := roseDB.Txn(func(tx *Txn) error {
		ok := tx.SIsMember(key, []byte("set-val-1"))
		t.Log(ok)
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func TestTxn_SRem(t *testing.T) {
	key := []byte("my_set")

	t.Run("1", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			//err := tx.SAdd(key, []byte("set-val-1"))
			//if err != nil {
			//	return err
			//}

			//ok := tx.SIsMember(key, []byte("set-val-1"))
			//t.Log(ok)
			//
			//err := tx.SRem(key, []byte("set-val-1"))
			//if err != nil {
			//	return err
			//}

			ok := tx.SIsMember(key, []byte("set-val-1"))
			t.Log(ok)
			return nil
		})
		if err != nil {
			t.Error(err)
		}
	})
}

func TestTxn_ZAdd(t *testing.T) {
	key := []byte("my_zset")
	err := roseDB.Txn(func(tx *Txn) error {
		err := tx.ZAdd(key, 121, []byte("zset-val-11"))
		return err
	})
	if err != nil {
		t.Error(err)
	}
}

func TestTxn_ZScore(t *testing.T) {
	key := []byte("my_zset")

	roseDB.TxnView(func(tx *Txn) error {
		exist, score, err := tx.ZScore(key, []byte("zset-val-11"))
		t.Log(exist, score)
		return err
	})
}

func TestTxn_ZRem(t *testing.T) {
	key := []byte("my_zset")

	t.Run("1", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			_, score, err := tx.ZScore(key, []byte("zset-val-11"))
			t.Log(score, err)
			//
			//err = tx.ZRem(key, []byte("zset-val-11"))
			//if err != nil {
			//	return err
			//}

			_, score, err = tx.ZScore(key, []byte("zset-val-11"))
			t.Log(score, err)

			return nil
		})
	})

}

func TestTxn_Commit(t *testing.T) {

	t.Run("1", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			for i := 0; i < 1000000; i++ {
				err := tx.Set(GetKey(i), GetValue())
				if err != nil {
					return err
				}
			}
			return nil
		})
	})

	t.Run("1", func(t *testing.T) {
		wg := new(sync.WaitGroup)
		wg.Add(2)

		go func() {
			t.Log("----txn 1 start----")
			defer func() {
				wg.Done()
				t.Log("----txn 1 end----")
			}()

			roseDB.Txn(func(tx *Txn) error {
				for i := 0; i < 500000; i++ {
					err := tx.Set(GetKey(i), GetValue())
					if err != nil {
						return err
					}
				}
				return nil
			})
		}()

		go func() {
			time.Sleep(500 * time.Millisecond)
			t.Log("----txn 2 start----")
			defer func() {
				wg.Done()
				t.Log("----txn 2 end----")
			}()

			roseDB.Txn(func(tx *Txn) error {
				for i := 0; i < 500000; i++ {
					err := tx.Set(GetKey(i), GetValue())
					if err != nil {
						return err
					}
				}
				return nil
			})

			roseDB.Txn(func(tx *Txn) error {
				key := []byte("k1-name")
				val := []byte("val-1-roseduan")
				err := tx.Set(key, val)
				return err
			})
		}()
		wg.Wait()
	})
}

func TestLoadTxnMeta(t *testing.T) {
	txnMeta, err := LoadTxnMeta(roseDB.config.DirPath + dbTxMetaSaveFile)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, txnMeta, nil)
}

func TestRoseDB_Txn(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		roseDB.Txn(func(tx *Txn) error {
			k1 := []byte("k1")
			v1 := []byte("str-val-1")
			if err := tx.Set(k1, v1); err != nil {
				return err
			}

			k2 := []byte("k2")
			v2 := []byte("list-val-2")
			if err := tx.LPush(k2, v2); err != nil {
				return err
			}

			k3 := []byte("k3")
			f1 := []byte("f1")
			v3 := []byte("hash-val-3")
			if err := tx.HSet(k3, f1, v3); err != nil {
				return err
			}

			k4 := []byte("k4")
			v4 := []byte("set-val-4")
			if err := tx.SAdd(k4, v4); err != nil {
				return err
			}

			k5 := []byte("k5")
			v5 := []byte("zset-val-5")
			if err := tx.ZAdd(k5, 1232.3, v5); err != nil {
				return err
			}
			return nil
		})
	})
}

func TestRoseDB_TxnView(t *testing.T) {
	key := []byte("k1")
	roseDB.TxnView(func(tx *Txn) error {
		var v []byte
		err := tx.Get(key, &v)
		if err != nil {
			return err
		}
		t.Log(string(v), err)

		k3 := []byte("k3")
		f1 := []byte("f1")

		var vv []byte
		err = tx.HGet(k3, f1, &vv)
		assert.Equal(t, err, nil)
		t.Log(string(vv))

		k4 := []byte("k4")
		v4 := []byte("set-val-4")
		ok := tx.SIsMember(k4, v4)
		t.Log(ok)

		k5 := []byte("k5")
		v5 := []byte("zset-val-5")
		exist, score, err := tx.ZScore(k5, v5)
		if err != nil {
			return err
		}
		t.Log(exist, score)
		return nil
	})
}

func TestTxn_Rollback(t *testing.T) {
	tx := roseDB.NewTransaction()
	k1 := []byte("k100")
	v1 := []byte("str-val-100")
	tx.Set(k1, v1)

	//tx.Rollback()
	if err := tx.Commit(); err != nil {
		t.Log(err)
	}
}

const alphabet = "abcdefghijklmnopqrstuvwxyz"

func GetKey(n int) []byte {
	return []byte("test_key_" + fmt.Sprintf("%09d", n))
}

func GetValue() []byte {
	var str bytes.Buffer
	for i := 0; i < 12; i++ {
		str.WriteByte(alphabet[rand.Int()%26])
	}
	return []byte("test_val-" + strconv.FormatInt(time.Now().UnixNano(), 10) + str.String())
}

func BenchmarkTxn_Set(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	//err := roseDB.Txn(func(tx *Txn) error {
	//	for i := 0; i < b.N; i++ {
	//		err := tx.Set(GetKey(i), GetValue())
	//		if err != nil {
	//			return err
	//		}
	//	}
	//	return nil
	//})
	//if err != nil {
	//	b.Log("write err: ", err)
	//}

	for i := 0; i < b.N; i++ {
		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.Set(GetKey(i), GetValue())
			return err
		})
		if err != nil {
			b.Log("write err: ", err)
		}
	}
}

func BenchmarkTxn_Set1(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := roseDB.Set(GetKey(i), GetValue())
		if err != nil {
			panic(err)
		}
	}
}

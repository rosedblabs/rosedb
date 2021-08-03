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
		err := roseDB.Txn(func(tx *Txn) error {
			for i := 0; i < 300000; i++ {
				err := tx.Set(GetKey(i), GetValue())
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})
}

func TestTxn_SetNx(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		key := []byte("k1")
		val := []byte("val-1")
		err := roseDB.Txn(func(tx *Txn) error {
			res, err := tx.SetNx(key, val)
			t.Log("res = ", res)
			return err
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})
}

func TestTxn_Get(t *testing.T) {
	key := []byte("k100")
	err := roseDB.TxnView(func(tx *Txn) error {
		v, err := tx.Get(key)
		if err != nil {
			return err
		}
		t.Log(string(v), err)
		return nil
	})
	if err != nil {
		t.Log(err)
	}
}

func TestTxn_GetSet(t *testing.T) {
	key := []byte("k1")
	val := []byte("val-new-1")

	t.Run("1", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			old, err := tx.GetSet(key, val)
			if err != nil {
				return err
			}
			t.Log(string(old), err)
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})

	t.Run("2", func(t *testing.T) {
		key := []byte("k2")
		val := []byte("val-2")
		err := roseDB.Txn(func(tx *Txn) error {
			old, err := tx.GetSet(key, val)
			if err != nil {
				return err
			}
			t.Log(string(old), err)
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})

	t.Run("3", func(t *testing.T) {
		key := []byte("k3")
		val := []byte("val-3")
		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.Set(key, val)
			if err != nil {
				return err
			}

			old, err := tx.GetSet(key, []byte("val-new-33"))
			if err != nil {
				return err
			}
			t.Log(string(old), err)
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})
}

func TestTxn_SetEx(t *testing.T) {
	key := []byte("k2")
	val := []byte("val-new-new-2")
	t.Run("1", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.SetEx(key, val, 200)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})

	t.Run("2", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.SetEx([]byte("k2"), []byte("val-ex-2"), 200)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})
}

func TestTxn_Append(t *testing.T) {
	key := []byte("k5")
	val := []byte("val-555-new")

	t.Run("1", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.Append(key, val)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})

	t.Run("2", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.Append(key, []byte("  app-val"))
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})
}

func TestTxn_StrLen(t *testing.T) {
	key := []byte("k5")
	t.Run("1", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			strLen := tx.StrLen(key)
			t.Log(strLen)
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})

	t.Run("2", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			strLen := tx.StrLen([]byte("not exist"))
			t.Log(strLen)
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})
}

func TestTxn_StrExists(t *testing.T) {
	key := []byte("k5")
	t.Run("1", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			strLen := tx.StrExists(key)
			t.Log(strLen)
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})
}

func TestTxn_StrRem(t *testing.T) {
	key := []byte("k5")
	t.Run("1", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			strLen := tx.StrRem(key)
			t.Log(strLen)
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})
}

func TestTxn_LPush(t *testing.T) {
	key := []byte("my_list")

	t.Run("print data", func(t *testing.T) {
		vals, err := roseDB.LRange(key, 0, -1)
		if err != nil {
			t.Error(err)
		}
		for _, v := range vals {
			t.Log(string(v))
		}
	})

	t.Run("1", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.LPush(key, []byte("val-1"), []byte("val-2"), []byte("val-3"))
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})
}

func TestTxn_RPush(t *testing.T) {
	key := []byte("my_list")
	t.Run("1", func(t *testing.T) {
		err := roseDB.Txn(func(tx *Txn) error {
			err := tx.RPush(key, []byte("val-4"), []byte("val-5"), []byte("val-6"))
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Error("write data err ", err)
		}
	})
}

func TestTxn_HSet(t *testing.T) {
	key := []byte("my_hash")
	roseDB.Txn(func(tx *Txn) error {
		err := tx.HSet(key, []byte("f1"), []byte("val-1"))
		if err != nil {
			return err
		}
		return nil
	})
}

func TestTxn_HGet(t *testing.T) {
	roseDB.Txn(func(tx *Txn) error {
		key := []byte("my_hash")
		v := roseDB.HGet(key, []byte("f1"))
		t.Log(string(v))
		return nil
	})
}

func TestTxn_SAdd(t *testing.T) {
	key := []byte("my_set")
	err := roseDB.Txn(func(tx *Txn) error {
		err := tx.SAdd(key, []byte("set-val-1"), []byte("set-val-2"))
		return err
	})
	if err != nil {
		t.Error(err)
	}
}

func TestTxn_SIsMember(t *testing.T) {
	key := []byte("my_set")
	err := roseDB.Txn(func(tx *Txn) error {
		ok := tx.SIsMember(key, []byte("set-val-11"))
		t.Log(ok)
		return nil
	})
	if err != nil {
		t.Error(err)
	}
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
		score, err := tx.ZScore(key, []byte("zset-val-11"))
		t.Log(score)
		return err
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
		v, err := tx.Get(key)
		if err != nil {
			return err
		}
		t.Log(string(v), err)

		k3 := []byte("k3")
		f1 := []byte("f1")
		vv := tx.HGet(k3, f1)
		t.Log(string(vv))

		k4 := []byte("k4")
		v4 := []byte("set-val-4")
		ok := tx.SIsMember(k4, v4)
		t.Log(ok)

		k5 := []byte("k5")
		v5 := []byte("zset-val-5")
		score, err := tx.ZScore(k5, v5)
		if err != nil {
			return err
		}
		t.Log(score)
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

func BenchmarkTxn_Set(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	//err := db.Txn(func(tx *Txn) error {
	//	for i := 0; i < b.N; i++ {
	//		key := []byte("test-key--" + strconv.Itoa(i))
	//		err := tx.Set(key, []byte("test-val"))
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
			//key := []byte("test-key--" + strconv.Itoa(i % 10))
			err := tx.Set(GetKey(i), GetValue())
			return err
		})
		if err != nil {
			b.Log("write err: ", err)
		}
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

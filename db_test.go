package rosedb

import (
	"testing"
)

func TestOpen(t *testing.T) {
	opts := DefaultOptions("/tmp/rosedb")
	db, err := Open(opts)
	if err != nil {
		t.Error("open db err ", err)
	}

	key := []byte("my_list")
	err = db.LPush(key, []byte("LotusDB"))
	t.Log(err)

	//v, err := db.LPop(key)
	//t.Log(string(v))
	//t.Log(err)

	db.Set([]byte("set-k"), []byte("set-v"))
	db.HSet([]byte("hset-k"), []byte("hset-f"), []byte("hset-v"))
	db.SAdd([]byte("sadd-k"), []byte("sadd-v"))
	db.ZAdd([]byte("zset-k"), 1993, []byte("zset-v"))
}

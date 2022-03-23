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

	//key := []byte("name")
	//err = db.Set(key, []byte("RoseDB"))
	//t.Log(err)

	//v, err := db.Get(key)
	//t.Logf("val = *%s*", string(v))
	//t.Log(err)

	//err = db.Delete(key)
	//t.Log(err)

	hk := []byte("myhash")
	hf := []byte("myhash-field1")
	////hv := []byte("myhash-field1-val")
	////err = db.HSet(hk, hf, hv)
	////t.Log(err)
	//
	//err = db.ZAdd(hk, 1232.3324, hf)
	//t.Log(err)

	ok, score := db.ZScore(hk, hf)
	t.Log(ok, score)
}

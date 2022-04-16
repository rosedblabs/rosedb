package rosedb

import (
	"path/filepath"
	"testing"
)

func TestRoseDB_SAdd(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	//defer destroyDB(db)
	if err != nil {
		t.Error("open db err ", err)
	}

	//db.SAdd(GetKey(12), GetValue16B())
	//db.SAdd(GetKey(12), GetValue16B())
	//db.SAdd(GetKey(12), GetValue16B())

	members, err := db.SMembers(GetKey(12))
	t.Log(err)
	for _, m := range members {
		t.Log(string(m))
	}
}

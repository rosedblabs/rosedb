package rosedb

import (
	"github.com/stretchr/testify/assert"
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

	setKey := []byte("my_set")
	//for i := 0; i < 10; i++ {
	//	db.SAdd(setKey, GetKey(i))
	//}

	members, err := db.SMembers(setKey)
	assert.Nil(t, err)

	for _, mem := range members {
		t.Log(string(mem))
	}

	//pop, err := db.SPop(setKey, 3)
	//assert.Nil(t, err)
	//for _, mem := range pop {
	//	t.Log(string(mem))
	//}
}

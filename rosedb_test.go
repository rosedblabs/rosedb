package rosedb

import (
	"fmt"
	"github.com/roseduan/rosedb/storage"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func InitDb() *RoseDB {
	config := DefaultConfig()
	//config.DirPath = dbPath
	config.IdxMode = KeyOnlyMemMode
	config.RwMethod = storage.FileIO

	db, err := Open(config)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func InitDB(cfg Config) *RoseDB {
	db, err := Open(cfg)
	if err != nil {
		panic(fmt.Sprintf("open rosedb err.%+v", err))
	}
	return db
}

func DestroyDB(db *RoseDB) {
	if db == nil {
		return
	}
	err := os.RemoveAll(db.config.DirPath)
	if err != nil {
		log.Fatalf("destroy db err.%+v", err)
	}
}

func ReopenDb() *RoseDB {
	return InitDb()
}

func TestOpen(t *testing.T) {
	type args struct {
		config Config
	}

	config := DefaultConfig()
	mmapConfig := config
	mmapConfig.RwMethod = storage.MMap
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"default", args{config: DefaultConfig()}, false},
		{"mmap", args{config: mmapConfig}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Open(tt.args.config)
			defer DestroyDB(got)

			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.NotNil(t, got)
		})
	}
}

func TestOpen2(t *testing.T) {
	open := func(method storage.FileRWMethod) {
		config := DefaultConfig()
		config.RwMethod = method
		roseDB := InitDB(config)
		defer func() {
			err := roseDB.Close()
			assert.Nil(t, err)
			DestroyDB(roseDB)
		}()

		writeDataForOpen(t, roseDB)

		db, err := Open(config)
		assert.Nil(t, err)

		//t.Log(db.strIndex.idxList.Len)
		//t.Log(db.listIndex.indexes.LLen("my_list"))
		//t.Log(db.hashIndex.indexes.HLen("my_hash"))
		//t.Log(db.setIndex.indexes.SCard("my_set"))
		//t.Log(db.zsetIndex.indexes.ZCard("my_zset"))
		num := 250000
		assert.Equal(t, db.strIndex.idxList.Len, num)
		assert.Equal(t, db.listIndex.indexes.LLen("my_list"), num)
		assert.Equal(t, db.hashIndex.indexes.HLen("my_hash"), num)
		assert.Equal(t, db.setIndex.indexes.SCard("my_set"), num)
		assert.Equal(t, db.zsetIndex.indexes.ZCard("my_zset"), num)
	}

	open(storage.FileIO)
	open(storage.MMap)
}

func TestRoseDB_Close(t *testing.T) {
	closeDB := func(method storage.FileRWMethod) {
		config := DefaultConfig()
		config.RwMethod = method
		roseDB := InitDB(config)
		defer DestroyDB(roseDB)

		err := roseDB.Close()
		assert.Nil(t, err)
	}

	closeDB(storage.FileIO)
	closeDB(storage.MMap)
}

func TestRoseDB_Sync(t *testing.T) {
	closeDB := func(method storage.FileRWMethod) {
		config := DefaultConfig()
		config.RwMethod = method
		roseDB := InitDB(config)
		defer DestroyDB(roseDB)

		err := roseDB.Sync()
		assert.Nil(t, err)
	}

	closeDB(storage.FileIO)
	closeDB(storage.MMap)
}

func TestOpen3(t *testing.T) {
	config := DefaultConfig()
	config.MergeThreshold = 1
	roseDB := InitDB(config)

	var r string
	err := roseDB.Get("merge-ex-key-2", &r)
	t.Log(err, r)

	t.Log(roseDB.TTL("merge-ex-key-1"))
	t.Log(roseDB.TTL("merge-ex-key-2"))
}

func TestRoseDB_Merge(t *testing.T) {
	config := DefaultConfig()
	config.MergeThreshold = 1
	roseDB := InitDB(config)

	t.Run("list", func(t *testing.T) {
		listKey := "my_list"
		//for i := 0; i < 600000; i++ {
		//	_, err := roseDB.LPush(listKey, GetValue())
		//	assert.Nil(t, err)
		//}
		//for i := 0; i < 580000; i++ {
		//	_, err := roseDB.RPop(listKey)
		//	assert.Nil(t, err)
		//}

		//roseDB.Merge()

		l1 := roseDB.LLen(listKey)
		t.Log(l1)

		v1 := roseDB.LIndex(listKey, 0)
		v2 := roseDB.LIndex(listKey, -1)

		t.Log(string(v1))
		t.Log(string(v2))

		roseDB.LPush(listKey, "rosedb")
		v3 := roseDB.LIndex(listKey, 0)
		t.Log(string(v3))
	})

	t.Run("hash", func(t *testing.T) {
	})
}

func writeDataForOpen(t *testing.T, roseDB *RoseDB) {
	listKey := "my_list"
	hashKey := "my_hash"
	setKey := "my_set"
	zsetKey := "my_zset"

	for i := 0; i < 250000; i++ {
		err := roseDB.Set(GetKey(i), GetValue())
		assert.Nil(t, err)

		_, err = roseDB.LPush(listKey, GetValue())
		assert.Nil(t, err)

		_, err = roseDB.HSet([]byte(hashKey), GetKey(i), GetValue())
		assert.Nil(t, err)

		_, err = roseDB.SAdd([]byte(setKey), GetValue())
		assert.Nil(t, err)

		err = roseDB.ZAdd(zsetKey, float64(i+10), GetValue())
		assert.Nil(t, err)
	}
}

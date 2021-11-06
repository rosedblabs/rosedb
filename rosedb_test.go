package rosedb

import (
	"encoding/json"
	"fmt"
	"github.com/roseduan/rosedb/storage"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

var dbPath = "/tmp/rosedb_server"

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

func TestRoseDb_Save(t *testing.T) {
	config := DefaultConfig()
	config.DirPath = "/tmp/testRoseDB"
	config.BlockSize = 3
	db, err := Open(config)
	if err != nil {
		t.Fatal(err.Error())
	}
	testKey := []byte("test_key1")
	testVal := []byte("test_val1")
	e := &storage.Entry{
		Meta: &storage.Meta{
			Key:       testKey,
			Value:     testVal,
			Extra:     nil,
			KeySize:   uint32(len(testKey)),
			ValueSize: uint32(len(testVal)),
			ExtraSize: 0,
		},
		Timestamp: 0,
		TxId:      0,
	}
	err = db.store(e)
	//if err != nil {
	//	t.Fatal(err.Error())
	//}
	testKey = []byte("test_key2")
	testVal = []byte("test_val2")
	e2 := &storage.Entry{
		Meta: &storage.Meta{
			Key:       testKey,
			Value:     testVal,
			Extra:     nil,
			KeySize:   uint32(len(testKey)),
			ValueSize: uint32(len(testVal)),
			ExtraSize: 0,
		},
		Timestamp: 0,
		TxId:      0,
	}
	err = db.store(e2)
	if err != nil {
		t.Fatal(err.Error())
	}
	testKey = []byte("test_key3")
	testVal = []byte("test_val3")
	e3 := &storage.Entry{
		Meta: &storage.Meta{
			Key:       testKey,
			Value:     testVal,
			Extra:     nil,
			KeySize:   uint32(len(testKey)),
			ValueSize: uint32(len(testVal)),
			ExtraSize: 0,
		},
		Timestamp: 0,
		TxId:      0,
	}
	err = db.store(e3)
}

func TestOpen(t *testing.T) {

	opendb := func(method storage.FileRWMethod) {
		config := DefaultConfig()
		config.RwMethod = method

		config.DirPath = dbPath
		db, err := Open(config)
		if err != nil {
			t.Error("open db err: ", err)
		}

		defer db.Close()
	}

	t.Run("FileIO", func(t *testing.T) {
		opendb(storage.FileIO)
	})

	t.Run("MMap", func(t *testing.T) {
		opendb(storage.MMap)
	})
}

func Test_SaveInfo(t *testing.T) {
	config := DefaultConfig()
	config.DirPath = dbPath
	db, err := Open(config)
	if err != nil {
		panic(err)
	}
	db.saveConfig()

	var cfg Config
	bytes, _ := ioutil.ReadFile(config.DirPath + "/db.cfg")
	_ = json.Unmarshal(bytes, &cfg)
}

func TestRoseDB_Backup(t *testing.T) {
	err := roseDB.Backup("/tmp/rosedb/backup-db0")
	if err != nil {
		t.Error(err)
	}
}

func TestRoseDB_Close(t *testing.T) {
	db := InitDb()
	defer db.Close()
}

func TestRoseDB_Sync(t *testing.T) {
	db := InitDb()
	defer db.Close()

	db.Sync()
}

func TestRoseDB_Reclaim2(t *testing.T) {
	now := time.Now()
	for i := 0; i <= 2000000; i++ {
		value := GetValue()
		err := roseDB.Set(GetKey(i%500000), value)
		if err != nil {
			panic(err)
		}
		if i == 44091 {
			err := roseDB.Set("test-key", "rosedb")
			if err != nil {
				panic(err)
			}
		}

		_, err = roseDB.HSet(GetKey(100), []byte("h1"), GetValue())
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i <= 2000000; i++ {
		listKey := []byte("my-list")
		_, err := roseDB.LPush(listKey, GetValue())
		if err != nil {
			panic(err)
		}
		if i > 200 {
			_, err = roseDB.LPop(listKey)
			if err != nil {
				panic(err)
			}
		}
	}
	t.Log("time spend --- ", time.Since(now).Milliseconds())
}

func TestRoseDB_SingleMerge(t *testing.T) {
	//writeDataForMerge()

	err := roseDB.SingleMerge(0)
	assert.Nil(t, err)
}

func TestRoseDB_StartMerge(t *testing.T) {
	var err error

	//writeDataForMerge()

	//go func() {
	//	time.Sleep(4 * time.Second)
	//	fmt.Println("发送终止信号")
	//	roseDB.StopMerge()
	//}()

	now := time.Now()
	err = roseDB.StartMerge()
	if err != nil {
		panic(err)
	}
	t.Log("merge spend --- ", time.Since(now).Milliseconds())

	var r string
	err = roseDB.Get("test-key", &r)
	//assert.Equal(t, err, nil)
	t.Log(r, err)
	l := roseDB.strIndex.idxList.Len
	t.Log("string 数据量 : ", l)
}

func TestRoseDB_StopMerge(t *testing.T) {
	fmt.Println("发送终止信号")
	roseDB.StopMerge()
}

func writeDataForMerge() {
	//for i := 0; i <= 200000; i++ {
	//	listKey := []byte("my-list")
	//	_, err := roseDB.LPush(listKey, GetValue())
	//	if err != nil {
	//		panic(err)
	//	}
	//	if i > 20 {
	//		_, err = roseDB.LPop(listKey)
	//		if err != nil {
	//			panic(err)
	//		}
	//	}
	//}

	for i := 0; i <= 2000000; i++ {
		err := roseDB.Set(GetKey(i%10000), GetValue())
		if err != nil {
			panic(err)
		}
	}
}

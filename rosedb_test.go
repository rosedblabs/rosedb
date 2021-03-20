package rosedb

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"rosedb/storage"
	"testing"
)

var dbPath = "/Users/roseduan/resources/rosedb/db0"

func InitDb() *RoseDB {
	config := DefaultConfig()
	config.DirPath = dbPath
	config.IdxMode = KeyOnlyRamMode
	config.RwMethod = storage.FileIO
	config.BlockSize = 4 * 1024 * 1024

	db, err := Open(config)
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func ReopenDb() *RoseDB {
	db, err := Reopen(dbPath)
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func TestOpen(t *testing.T) {

	opendb := func(method storage.FileRWMethod) {
		config := DefaultConfig()
		config.RwMethod = method

		config.DirPath = "/Users/roseduan/resources/rosedb/db1"
		db, err := Open(config)
		if err != nil {
			t.Error("数据库打开失败 ", err)
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

func TestDifferentTypeData(t *testing.T) {

	t.Run("save", func(t *testing.T) {
		db := InitDb()
		defer db.Close()
		//str
		db.Set([]byte("str_key_001"), []byte("str_val_001"))
		db.Set([]byte("str_key_002"), []byte("str_val_002"))

		//list
		db.LPush([]byte("list_data"), []byte("list_val_001"), []byte("list_val_002"), []byte("list_val_003"))

		//hash
		db.HSet([]byte("hash_data_001"), []byte("hash_field_001"), []byte("hash_val_001"))
		db.HSet([]byte("hash_data_002"), []byte("hash_field_002"), []byte("hash_val_002"))

		//set
		db.SAdd([]byte("set_key_001"), []byte("set_val_001"), []byte("set_val_002"), []byte("set_val_003"))

		//zset
		db.ZAdd([]byte("zset_key_001"), 84.44, []byte("zset_val_001"))
		db.ZAdd([]byte("zset_key_002"), 90.23, []byte("zset_val_002"))
	})

	t.Run("get", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		t.Run("str", func(t *testing.T) {
			val1, _ := db.Get([]byte("str_key_001"))
			t.Log(string(val1))

			val2, _ := db.Get([]byte("str_key_002"))
			t.Log(string(val2))

		})

		t.Run("list", func(t *testing.T) {
			val1 := db.LIndex([]byte("list_data"), 0)
			val2 := db.LIndex([]byte("list_data"), 1)
			val3 := db.LIndex([]byte("list_data"), 2)
			t.Log(string(val1))
			t.Log(string(val2))
			t.Log(string(val3))
		})

		t.Run("hash", func(t *testing.T) {
			val1 := db.HGet([]byte("hash_data_001"), []byte("hash_field_001"))
			val2 := db.HGet([]byte("hash_data_002"), []byte("hash_field_002"))
			t.Log(string(val1))
			t.Log(string(val2))
		})

		t.Run("set", func(t *testing.T) {
			members := db.SMembers([]byte("set_key_001"))
			for _, m := range members {
				t.Log(string(m))
			}
		})

		t.Run("zset", func(t *testing.T) {
			vals1 := db.ZRange([]byte("zset_key_001"), 0, -1)
			for _, v := range vals1 {
				t.Logf("%+v ", v)
			}

			vals2 := db.ZRange([]byte("zset_key_002"), 0, -1)
			for _, v := range vals2 {
				t.Logf("%+v ", v)
			}
		})
	})
}

func Test_SaveInfo(t *testing.T) {
	config := DefaultConfig()
	config.DirPath = "/Users/roseduan/resources/rosedb"
	db, err := Open(config)

	if err != nil {
		t.Error("数据库打开失败 ", err)
	}

	db.saveConfig()

	var cfg Config
	bytes, _ := ioutil.ReadFile(config.DirPath + "/db.cfg")
	_ = json.Unmarshal(bytes, &cfg)
	t.Logf("%+v", cfg)
}

func TestReopen(t *testing.T) {
	path := "/Users/roseduan/resources/rosedb/db3"
	db, err := Reopen(path)
	if err != nil {
		t.Error("reopen db error ", err)
	}

	//test_value_227957
	key := []byte("test_key_481522")
	val, _ := db.LPop(key)
	t.Log(string(val))
}

func TestRoseDB_Reclaim(t *testing.T) {
	db := ReopenDb()
	defer db.Close()

	err := db.Reclaim()
	if err != nil {
		log.Println(err)
	}
}

func TestRoseDB_Backup(t *testing.T) {
	path := "/Users/roseduan/resources/rosedb/db0"
	db, err := Reopen(path)
	if err != nil {
		t.Error("reopen db error ", err)
	}

	err = db.Backup("/Users/roseduan/resources/backup-db0")
	if err != nil {
		t.Error(err)
	}
}

package rosedb

import (
	"encoding/json"
	"github.com/roseduan/rosedb/storage"
	"io/ioutil"
	"log"
	"testing"
)

var dbPath = "/tmp/rosedb/db0"

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

		config.DirPath = "/tmp/rosedb/db0"
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

func Test_SaveInfo(t *testing.T) {
	config := DefaultConfig()
	config.DirPath = "/tmp/rosedb/db0"
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
	path := "/tmp/rosedb/db0"
	_, _ = Reopen(path)
	//defer db.Close()

	//if err != nil {
	//	log.Println(err)
	//}
}

//
//func TestRoseDB_Reclaim(t *testing.T) {
//	db := ReopenDb()
//	defer db.Close()
//
//	db.config.ReclaimThreshold = 0
//	err := db.Reclaim()
//	if err != nil {
//		log.Println(err)
//	}
//}

func TestRoseDB_Backup(t *testing.T) {
	path := "/tmp/rosedb/db0"
	db, err := Reopen(path)
	if err != nil {
		t.Error("reopen db error ", err)
	}

	err = db.Backup("/tmp/rosedb/backup-db0")
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

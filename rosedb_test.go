package rosedb

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"testing"
)

var dbPath = "/Users/roseduan/resources/rosedb/db11"

func InitDb() *RoseDB {
	config := DefaultConfig()
	config.DirPath = dbPath

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
	config := DefaultConfig()
	config.IdxMode = KeyOnlyRamMode
	config.ReclaimThreshold = 1

	config.DirPath = "/Users/roseduan/resources/rosedb/db6"
	db, err := Open(config)
	if err != nil {
		t.Error("数据库打开失败 ", err)
	}

	defer db.Close()
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
	path := "/Users/roseduan/resources/rosedb/db0"
	db, err := Reopen(path)
	if err != nil {
		t.Error("reopen db error ", err)
	}

	t.Log(db.idxList.Len)
	db.meta.UnusedSpace = 19993333333

	e, _ := db.Get([]byte("test_key_916257"))
	t.Log(string(e))

	//db.Reclaim()

	res, _ := db.Get([]byte("my_name"))
	t.Log(string(res))

	defer db.Close()
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

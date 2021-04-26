package rosedb

import (
	"encoding/json"
	"github.com/roseduan/rosedb/storage"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var dbPath = "/tmp/rosedb/db1"

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

		config.DirPath = dbPath
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
	config.DirPath = dbPath
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
	path := dbPath
	_, _ = Reopen(path)
	//defer db.Close()

	//if err != nil {
	//	log.Println(err)
	//}
}

func TestRoseDB_Backup(t *testing.T) {
	path := dbPath
	db, err := Reopen(path)
	if err != nil {
		t.Error("reopen db error ", err)
	}

	err = db.Backup("/tmp/rosedb/backup-db0")
	if err != nil {
		t.Error(err)
	}
}

func TestOpen2(t *testing.T) {
	config := DefaultConfig()
	config.DirPath = ""

	db, _ := Open(config)
	if db != nil {
		db.Close()
	}
}

func TestReopen2(t *testing.T) {
	db, _ := Reopen("")
	if db != nil {
		db.Close()
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

func TestRoseDB_Reclaim(t *testing.T) {
	config := DefaultConfig()
	config.DirPath = "/tmp/rosedb/db-reclaim"
	config.IdxMode = KeyOnlyRamMode
	config.RwMethod = storage.FileIO
	config.BlockSize = 4 * 1024 * 1024

	db, err := Open(config)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	writeMultiLargeData(db)

	// another case
	db.config.ReclaimThreshold = 10
	db.Reclaim()

	//for test
	db.config.ReclaimThreshold = 1
	err = db.Reclaim()
	if err != nil {
		log.Println(err)
	}
}

func writeMultiLargeData(db *RoseDB) {
	keyPrefix := "test_key_"
	valPrefix := "test_value_"
	rand.Seed(time.Now().Unix())

	//str
	for i := 0; i < 50000; i++ {
		key := keyPrefix + strconv.Itoa(rand.Intn(1000))
		val := valPrefix + strconv.Itoa(rand.Intn(1000))
		err := db.Set([]byte(key), []byte(val))
		if err != nil {
			log.Println("数据写入发生错误 ", err)
		}
	}

	//list
	for i := 0; i < 50000; i++ {
		key := keyPrefix + strconv.Itoa(rand.Intn(1000))
		val := valPrefix + strconv.Itoa(rand.Intn(1000))
		if i%2 == 0 {
			db.LPush([]byte(key), []byte(val))
		} else {
			db.RPush([]byte(key), []byte(val))
		}
	}

	//hash
	for i := 0; i < 50000; i++ {
		key := keyPrefix + strconv.Itoa(rand.Intn(1000))
		field := "field-" + strconv.Itoa(rand.Intn(1000))
		val := valPrefix + strconv.Itoa(rand.Intn(1000))
		db.HSet([]byte(key), []byte(field), []byte(val))
	}

	//set
	for i := 0; i < 50000; i++ {
		key := keyPrefix + strconv.Itoa(rand.Intn(1000))
		val := valPrefix + strconv.Itoa(rand.Intn(1000))
		db.SAdd([]byte(key), [][]byte{[]byte(val)}...)
	}

	var key1 = []byte("m_set1")
	var key2 = []byte("m_set2")
	db.SAdd(key1, [][]byte{[]byte("1")}...)
	db.SAdd(key2, [][]byte{[]byte("2")}...)
	db.SMove(key1, key2, []byte("1"))

	//zset
	for i := 0; i < 50000; i++ {
		key := keyPrefix + strconv.Itoa(rand.Intn(1000))
		val := valPrefix + strconv.Itoa(rand.Intn(1000))
		db.ZAdd([]byte(key), float64(i+100), []byte(val))
	}
}

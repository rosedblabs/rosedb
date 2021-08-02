package rosedb

import (
	"encoding/json"
	"fmt"
	"github.com/roseduan/rosedb/storage"
	"io/ioutil"
	"log"
	"math/rand"
	"testing"
	"time"
)

var dbPath = "/tmp/rosedb_server"

func InitDb() *RoseDB {
	config := DefaultConfig()
	//config.DirPath = dbPath
	config.IdxMode = KeyOnlyMemMode
	config.RwMethod = storage.FileIO
	//config.BlockSize = 4 * 1024 * 1024

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
	config.IdxMode = KeyOnlyMemMode
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
	//keyPrefix := "my_list"
	//valPrefix := "test_value_"
	rand.Seed(time.Now().Unix())

	//str
	for i := 0; i < 300000; i++ {
		//key := keyPrefix + strconv.Itoa(rand.Intn(1000))
		//val := valPrefix + strconv.Itoa(rand.Intn(1000))
		err := db.Set(GetKey(i), GetValue())
		if err != nil {
			log.Println("数据写入发生错误 ", err)
		}

		if i == 233004 {
			err := db.Set([]byte("aaa"), []byte("222333"))
			if err != nil {
				log.Println("数据写入发生错误 ", err)
			}
		}
	}
	//
	//list
	//for i := 0; i < 500000; i++ {
	//	//key := keyPrefix + strconv.Itoa(rand.Intn(1000))
	//	val := valPrefix + strconv.Itoa(rand.Intn(1000))
	//	if i%2 == 0 {
	//		db.LPush([]byte(keyPrefix), []byte(val))
	//	} else {
	//		db.RPush([]byte(keyPrefix), []byte(val))
	//	}
	//}
	//
	//db.LSet([]byte(keyPrefix), 199384, []byte("I am roseduan"))
	//db.LPush([]byte("bbb"), []byte("rosedb"))

	//
	////hash
	//for i := 0; i < 2000000; i++ {
	//	key := keyPrefix + strconv.Itoa(rand.Intn(100000))
	//	field := "field-" + strconv.Itoa(rand.Intn(100000))
	//	val := valPrefix + strconv.Itoa(rand.Intn(100000))
	//	db.HSet([]byte(key), []byte(field), []byte(val))
	//}
	//
	////set
	//for i := 0; i < 2000000; i++ {
	//	key := keyPrefix + strconv.Itoa(rand.Intn(10000))
	//	val := valPrefix + strconv.Itoa(rand.Intn(10000))
	//	db.SAdd([]byte(key), [][]byte{[]byte(val)}...)
	//}
	//
	//var key1 = []byte("m_set1")
	//var key2 = []byte("m_set2")
	//db.SAdd(key1, [][]byte{[]byte("1")}...)
	//db.SAdd(key2, [][]byte{[]byte("2")}...)
	//db.SMove(key1, key2, []byte("1"))
	//
	////zset
	//for i := 0; i < 2000000; i++ {
	//	key := keyPrefix + strconv.Itoa(rand.Intn(10000))
	//	val := valPrefix + strconv.Itoa(rand.Intn(10000))
	//	db.ZAdd([]byte(key), float64(i+100), []byte(val))
	//}
}

func TestOpen4(t *testing.T) {
	config := DefaultConfig()
	config.IdxMode = KeyOnlyMemMode
	//config.BlockSize = 8 * 1024 * 1024
	config.DirPath = "/tmp/rosedb"

	start := time.Now()
	db, err := Open(config)
	t.Log("open time spend: ", time.Since(start))

	if err != nil {
		log.Fatal("open db err.", err)
	}
	defer db.Close()

	t.Log("有效的 str 数量 : ", db.strIndex.idxList.Len)

	start = time.Now()
	//writeMultiLargeData(db)
	//err = db.SingleReclaim(0)
	//if err != nil {
	//	log.Fatal("reclaim err: ", err)
	//}
	fmt.Println("time spent : ", time.Since(start).Milliseconds())

	//ok1 := db.LKeyExists([]byte("aaa"))
	//ok2 := db.LKeyExists([]byte("bbb"))
	//t.Log(ok1, ok2)

	v := db.LIndex([]byte("my_list"), 199384)
	t.Log("-----==", string(v))
}

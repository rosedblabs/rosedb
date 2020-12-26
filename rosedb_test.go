package rosedb

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var dbPath = "/Users/roseduan/resources/rosedb/db7"

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

	t.Run("Test_Add", func(t *testing.T) {
		key, value := []byte("test_key_001"), []byte("test_val_001")
		if err := db.Set(key, value); err != nil {
			t.Error("写入数据失败 ", err)
		}

		newVal := []byte("test_val_002")
		db.Set(key, newVal)
	})

	t.Run("Test_Get", func(t *testing.T) {
		key := []byte("test_key_001")
		if val, err := db.Get(key); err != nil {
			t.Error("读取数据失败 ", err)
		} else {
			t.Log("读取到的数据 ", string(val))
		}
	})

	t.Run("append", func(t *testing.T) {
		key := []byte("test_key_001")
		newVal := []byte(" is append val")
		db.Append(key, newVal)

		e, _ := db.Get(key)
		t.Log("追加后的 val ", string(e))
	})

	t.Log("unused space : ", db.meta.UnusedSpace)

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

//批量数据测试
func TestRoseDB_Add(t *testing.T) {
	config := DefaultConfig()
	config.DirPath = "/Users/roseduan/resources/rosedb/db3"

	db, err := Open(config)
	if err != nil {
		log.Fatal(err)
	}

	keyPrefix := "test_key_"
	valPrefix := "test_value_"

	rand.Seed(time.Now().Unix())

	for i := 0; i < 100000; i++ {
		key := keyPrefix + strconv.Itoa(rand.Intn(1000000))
		val := valPrefix + strconv.Itoa(rand.Intn(1000000))

		err := db.LPush([]byte(key), []byte(val))
		if err != nil {
			t.Error("数据写入发生错误 ", err)
		}
	}

	_ = db.Set([]byte(keyPrefix+"0012"), []byte(valPrefix+"0012"))
	t.Log(db.idxList.Len)

	val, _ := db.Get([]byte(keyPrefix + "0012"))
	t.Log(string(val))

	defer db.Close()
}

func BenchmarkRoseDB_Set(b *testing.B) {
	b.StartTimer()

	config := DefaultConfig()
	config.DirPath = "/Users/roseduan/resources/rosedb/db17"
	db, err := Open(config)
	if err != nil {
		log.Fatal(err)
	}

	keyPrefix := "test_key_"
	valPrefix := "test_value_"

	rand.Seed(time.Now().Unix())

	for i := 0; i < 200000; i++ {
		key := keyPrefix + strconv.Itoa(rand.Intn(100000))
		val := valPrefix + strconv.Itoa(rand.Intn(100000))

		err := db.Set([]byte(key), []byte(val))
		if err != nil {
			b.Error("数据写入发生错误 ", err)
		}
	}

	defer func() {
		db.Sync()
		db.Close()
	}()

	b.StopTimer()
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

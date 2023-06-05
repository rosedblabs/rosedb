package rosedb

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/flower-corp/rosedb/logger"
	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	t.Run("default", func(t *testing.T) {
		opts := DefaultOptions(path)
		db, err := Open(opts)
		defer destroyDB(db)
		assert.Nil(t, err)
		assert.NotNil(t, db)
	})

	t.Run("mmap", func(t *testing.T) {
		opts := DefaultOptions(path)
		opts.IoType = MMap
		db, err := Open(opts)
		defer destroyDB(db)
		assert.Nil(t, err)
		assert.NotNil(t, db)
	})
}

func TestLogFileGC(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.LogFileGCInterval = time.Second * 7
	opts.LogFileGCRatio = 0.00001
	db, err := Open(opts)
	defer destroyDB(db)
	if err != nil {
		t.Error("open db err ", err)
	}

	writeCount := 800000
	for i := 0; i < writeCount; i++ {
		err := db.Set(GetKey(i), GetValue16B())
		assert.Nil(t, err)
	}

	var deleted [][]byte
	rand.Seed(time.Now().Unix())
	for i := 0; i < 100000; i++ {
		k := rand.Intn(writeCount)
		key := GetKey(k)
		err := db.Delete(key)
		assert.Nil(t, err)
		deleted = append(deleted, key)
	}

	time.Sleep(time.Second * 12)
	for _, key := range deleted {
		_, err := db.Get(key)
		assert.Equal(t, err, ErrKeyNotFound)
	}
}

func TestRoseDB_Backup(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	defer destroyDB(db)
	if err != nil {
		t.Error("open db err ", err)
	}

	for i := 0; i < 10; i++ {
		err := db.Set(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}

	backupPath := filepath.Join("/tmp", "rosedb-backup")
	err = db.Backup(backupPath)
	assert.Nil(t, err)

	// open the backup database
	opts2 := DefaultOptions(backupPath)
	db2, err := Open(opts2)
	assert.Nil(t, err)
	defer destroyDB(db2)
	val, err := db2.Get(GetKey(4))
	assert.Nil(t, err)
	assert.NotNil(t, val)
}

func TestKeyType(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	defer destroyDB(db)
	if err != nil {
		t.Error("open db err ", err)
	}

	strKey := []byte("str-key1")
	listKey := []byte("list-key1")
	setKey := []byte("set-key1")
	hasKey := []byte("hash-key1")
	zsetKey := []byte("zset-key1")
	notExistKey := []byte("nil-key")
	db.Set(strKey, []byte("v-1"))
	db.LPush(listKey, []byte("v-2"), []byte("v-3"))
	db.SAdd(setKey, []byte("v-4"), []byte("v-5"))
	db.HSet(hasKey, []byte("field-1"), []byte("v-6"))
	db.ZAdd(zsetKey, 6.0, []byte("v-7"))
	tests := []struct {
		name    string
		db      *RoseDB
		key     []byte
		want    string
		wantErr error
	}{
		{
			"str-key", db, strKey, "string", nil,
		},
		{
			"list-key", db, listKey, "list", nil,
		},
		{
			"set-key", db, setKey, "set", nil,
		},
		{
			"hash-key", db, hasKey, "hash", nil,
		},
		{
			"zset-key", db, zsetKey, "zset", nil,
		},
		{
			"not-exist-key", db, notExistKey, "", ErrKeyNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.KeyType(tt.key)
			if (err != nil) != (tt.wantErr != nil) {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func destroyDB(db *RoseDB) {
	if db != nil {
		_ = db.Close()
		if runtime.GOOS == "windows" {
			time.Sleep(time.Millisecond * 100)
		}
		err := os.RemoveAll(db.opts.DBPath)
		if err != nil {
			logger.Errorf("destroy db err: %v", err)
		}
	}
}

const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func init() {
	rand.Seed(time.Now().Unix())
}

// GetKey length: 32 Bytes
func GetKey(n int) []byte {
	return []byte("kvstore-bench-key------" + fmt.Sprintf("%09d", n))
}

func GetValue16B() []byte {
	return GetValue(16)
}

func GetValue128B() []byte {
	return GetValue(128)
}

func GetValue4K() []byte {
	return GetValue(4096)
}

func GetValue(n int) []byte {
	var str bytes.Buffer
	for i := 0; i < n; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return str.Bytes()
}

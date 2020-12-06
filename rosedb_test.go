package rosedb

import "testing"

func TestOpen(t *testing.T) {
	config := DefaultConfig()
	config.dirPath = "/Users/roseduan/resources/rosedb"
	db, err := Open(config)
	if err != nil {
		t.Error("数据库打开失败 ", err)
	}

	t.Run("Test_Add", func(t *testing.T) {
		key, value := []byte("test_key_001"), []byte("test_val_001")
		if err := db.Add(key, value); err != nil {
			t.Error("写入数据失败 ", err)
		}
	})

	t.Run("Test_Get", func(t *testing.T) {
		key := []byte("test_key_001")
		if val, err := db.Get(key); err != nil {
			t.Error("读取数据失败 ", err)
		} else {
			t.Log("读取到的数据 ", val)
		}
	})
}

package storage

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// DBMeta 保存数据库的一些额外信息
// save some extra info of rosedb, other config may be added in the future
type DBMeta struct {
	ActiveWriteOff map[uint16]int64 `json:"active_write_off"` //当前数据文件的写偏移  write offset of current active db file
}

// LoadMeta 加载数据信息
// load db meta
func LoadMeta(path string) (m *DBMeta) {
	m = &DBMeta{ActiveWriteOff: make(map[uint16]int64)}

	file, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return
	}
	defer file.Close()

	b, _ := ioutil.ReadAll(file)
	_ = json.Unmarshal(b, m)
	return
}

// Store 存储数据信息
// store db meta
func (m *DBMeta) Store(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	b, _ := json.Marshal(m)
	_, err = file.Write(b)
	return err
}

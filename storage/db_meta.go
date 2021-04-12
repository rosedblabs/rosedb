package storage

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// DBMeta 保存数据库的一些额外信息
type DBMeta struct {
	ActiveWriteOff int64 `json:"active_write_off"` //当前数据文件的写偏移
}

func LoadMeta(path string) (m *DBMeta) {
	m = &DBMeta{}

	file, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return
	}
	defer file.Close()

	b, _ := ioutil.ReadAll(file)
	_ = json.Unmarshal(b, m)
	return
}

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

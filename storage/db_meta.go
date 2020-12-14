package storage

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

//保存数据库的一些信息
type DBMeta struct {
	ActiveWriteOff int64  `json:"active_write_off"` //当前数据文件的写偏移
	UnusedSpace    uint64 `json:"unused_space"`     //未使用可回收的磁盘空间
}

func LoadMeta(path string) (m *DBMeta) {
	m = &DBMeta{}

	file, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return
	}

	defer file.Close()

	b, err := ioutil.ReadAll(file)
	if err != nil {
		return
	}

	err = json.Unmarshal(b, m)
	if err != nil {
		return
	}

	return
}

func (m *DBMeta) Store(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	defer file.Close()

	b, err := json.Marshal(m)
	if err != nil {
		return err
	}

	_, err = file.Write(b)
	return err
}

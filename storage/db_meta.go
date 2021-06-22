package storage

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// Save some extra info of rosedb, other config may be added in the future.
type DBMeta struct {
	ActiveWriteOff   map[uint16]int64 `json:"active_write_off"`  // Write offset of current active db files.
	ReclaimableSpace map[uint32]int64 `json:"reclaimable_space"` // Reclaimable space in each db file of String, for single reclaiming.
}

// LoadMeta load db meta from file.
func LoadMeta(path string) (m *DBMeta) {
	m = &DBMeta{
		ActiveWriteOff:   make(map[uint16]int64),
		ReclaimableSpace: make(map[uint32]int64),
	}

	file, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return
	}
	defer file.Close()

	b, _ := ioutil.ReadAll(file)
	_ = json.Unmarshal(b, m)
	return
}

// Store store db meta as json.
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

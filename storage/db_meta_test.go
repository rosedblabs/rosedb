package storage

import "testing"

func TestDBMeta_Store(t *testing.T) {
	writeOff := make(map[uint16]int64)
	ReclaimableSpace := make(map[uint32]int64)
	writeOff[0] = 34
	m := &DBMeta{writeOff, ReclaimableSpace}
	if err := m.Store("/tmp/db.Meta"); err != nil {
		t.Error(err)
	}
}

func TestLoadMeta(t *testing.T) {
	_ = LoadMeta("/tmp/db.Meta")
}

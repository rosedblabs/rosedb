package storage

import "testing"

func TestDBMeta_Store(t *testing.T) {
	m := &DBMeta{43}
	if err := m.Store("/tmp/db.Meta"); err != nil {
		t.Error(err)
	}
}

func TestLoadMeta(t *testing.T) {
	_ = LoadMeta("/tmp/db.Meta")
}

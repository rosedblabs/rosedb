package storage

import "testing"

func TestDBMeta_Store(t *testing.T) {
	m := &DBMeta{43, 1232}
	if err := m.Store("/Users/roseduan/resources/rosedb/db.meta"); err != nil {
		t.Error(err)
	}
}

func TestLoad(t *testing.T) {
	m := LoadMeta("/Users/roseduan/resources/rosedb/db.meta")
	t.Logf("%+v \n", m)
	t.Log(m == nil)
}

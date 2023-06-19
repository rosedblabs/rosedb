package rosedb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_Merge(t *testing.T) {
	options := DefaultOptions
	options.DirPath = "/tmp/rosedb-test"
	db, err := Open(options)
	assert.Nil(t, err)
	//defer destroyDB(db)
	t.Log(db, err)

	// for i := 0; i < 10000; i++ {
	// 	err := db.Put(utils.GetTestKey(rand.Int()), utils.RandomValue(128))
	// 	assert.Nil(t, err)
	// 	err = db.Put(utils.GetTestKey(rand.Int()), utils.RandomValue(KB))
	// 	assert.Nil(t, err)
	// 	err = db.Put(utils.GetTestKey(rand.Int()), utils.RandomValue(5*KB))
	// 	assert.Nil(t, err)
	// }

	// err = db.Merge()
	t.Log(err)
}

func TestDB_Merge2(t *testing.T) {
	// f, err := os.Open("/tmp/rosedb-test/aa")
	// t.Log(os.IsNotExist(err))
	// t.Log(f, err)

	// err = os.RemoveAll("/tmp/rosedb-test/")
	// t.Log(err)

	err := os.Rename("/tmp/rosedb-test/aa", "/tmp/rosedb-test/aa")
	t.Log(err)
}

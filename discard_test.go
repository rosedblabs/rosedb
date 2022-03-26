package rosedb

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestDiscard_listenUpdates(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	defer os.RemoveAll(path)
	assert.Nil(t, err)

	writeCount := 600000
	for i := 0; i < writeCount; i++ {
		err := db.Set(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}
	// delete
	for i := 0; i < 300000; i++ {
		err := db.Delete(GetKey(i))
		assert.Nil(t, err)
	}

	ccl, err := db.discard.getCCL(10, 0.001)
	assert.Nil(t, err)
	assert.Equal(t, len(ccl), 1)
}

func TestDiscard_newDiscard(t *testing.T) {
	t.Run("init", func(t *testing.T) {
		path := filepath.Join("/tmp", "rosedb-discard")
		os.MkdirAll(path, os.ModePerm)
		defer os.RemoveAll(path)
		dis, err := newDiscard(path, discardFileName)
		assert.Nil(t, err)

		assert.Equal(t, len(dis.freeList), 682)
		assert.Equal(t, len(dis.location), 0)
	})

	t.Run("with-data", func(t *testing.T) {
		path := filepath.Join("/tmp", "rosedb-discard")
		os.MkdirAll(path, os.ModePerm)
		defer os.RemoveAll(path)
		dis, err := newDiscard(path, discardFileName)
		assert.Nil(t, err)

		for i := 1; i < 300; i = i * 5 {
			dis.setTotal(uint32(i), 223)
			dis.incrDiscard(uint32(i), i*10)
		}

		assert.Equal(t, len(dis.freeList), 678)
		assert.Equal(t, len(dis.location), 4)

		// reopen
		dis2, err := newDiscard(path, discardFileName)
		assert.Nil(t, err)
		assert.Equal(t, len(dis2.freeList), 678)
		assert.Equal(t, len(dis2.location), 4)
	})
}

func TestDiscard_setTotal(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb-discard")
	os.MkdirAll(path, os.ModePerm)
	defer os.RemoveAll(path)
	dis, err := newDiscard(path, discardFileName)
	assert.Nil(t, err)

	type args struct {
		fid       uint32
		totalSize int
	}
	tests := []struct {
		name string
		dis  *discard
		args args
	}{
		{
			"zero", dis, args{0, 10},
		},
		{
			"normal", dis, args{334, 123224},
		},
		{
			"set-again-1", dis, args{194, 100},
		},
		{
			"set-again-2", dis, args{194, 150},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.dis.setTotal(tt.args.fid, uint32(tt.args.totalSize))
		})
	}
}

func TestDiscard_clear(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb-discard")
	os.MkdirAll(path, os.ModePerm)
	defer os.RemoveAll(path)
	dis, err := newDiscard(path, discardFileName)
	assert.Nil(t, err)

	for i := 0; i < 682; i++ {
		dis.setTotal(uint32(i), uint32(i+100))
		dis.incrDiscard(uint32(i), i+10)
	}

	type args struct {
		fid uint32
	}
	tests := []struct {
		name string
		dis  *discard
		args args
	}{
		{
			"0", dis, args{0},
		},
		{
			"33", dis, args{33},
		},
		{
			"198", dis, args{198},
		},
		{
			"340", dis, args{340},
		},
		{
			"680", dis, args{680},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.dis.clear(tt.args.fid)
		})
	}
}

func TestDiscard_incrDiscard(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb-discard")
	os.MkdirAll(path, os.ModePerm)
	defer os.RemoveAll(path)
	dis, err := newDiscard(path, "")
	assert.Nil(t, err)

	for i := 1; i < 600; i = i * 5 {
		dis.setTotal(uint32(i-1), uint32(i+1000))
	}
	for i := 1; i < 600; i = i * 5 {
		dis.incrDiscard(uint32(i-1), i+100)
	}

	ccl, err := dis.getCCL(10, 0.0000001)
	assert.Nil(t, err)
	assert.Equal(t, len(ccl), 4)
}

func TestDiscard_getCCL(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb-discard")
	os.MkdirAll(path, os.ModePerm)
	defer os.RemoveAll(path)
	dis, err := newDiscard(path, discardFileName)
	assert.Nil(t, err)

	for i := 1; i < 2000; i = i * 5 {
		dis.setTotal(uint32(i-1), uint32(i+1000))
	}
	for i := 1; i < 2000; i = i * 5 {
		dis.incrDiscard(uint32(i-1), i+100)
	}

	t.Run("normal", func(t *testing.T) {
		ccl, err := dis.getCCL(624, 0.0000001)
		assert.Nil(t, err)
		assert.Equal(t, len(ccl), 4)
	})

	t.Run("filter-some", func(t *testing.T) {
		ccl, err := dis.getCCL(100, 0.2)
		assert.Nil(t, err)
		assert.Equal(t, len(ccl), 2)
	})
	t.Run("clear and get", func(t *testing.T) {
		dis.clear(124)
		ccl, err := dis.getCCL(100, 0.0001)
		assert.Nil(t, err)
		assert.Equal(t, len(ccl), 4)
	})
}

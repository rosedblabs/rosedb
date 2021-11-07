package storage

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestBuild(t *testing.T) {
	path := "/tmp/rosedb"
	_ = os.MkdirAll("/tmp/rosedb", os.ModePerm)
	_ = os.MkdirAll("/tmp/rosedb/"+mergeDir, os.ModePerm)
	defer os.RemoveAll(path)

	writeDataForBuild(path, FileIO, 3)
	writeDataForBuild(path, MMap, 4)

	t.Run("fileio", func(t *testing.T) {
		build, m, err := Build(path, FileIO, 1024)
		assert.Nil(t, err)
		assert.NotNil(t, build)
		assert.NotNil(t, m)

		//t.Logf("%+v\n", build)
		//t.Logf("%+v\n", m)
	})

	t.Run("mmap", func(t *testing.T) {
		build, m, err := Build(path, MMap, 1024)
		assert.Nil(t, err)
		assert.NotNil(t, build)
		assert.NotNil(t, m)

		//t.Logf("%+v\n", build)
		//t.Logf("%+v\n", m)
	})

	t.Run("with merge", func(t *testing.T) {
		writeDataForBuild(path+"/"+mergeDir, FileIO, 0)
		writeDataForBuild(path+"/"+mergeDir, MMap, 1)

		build, m, err := Build(path, FileIO, 1024)
		assert.Nil(t, err)
		assert.NotNil(t, build)
		assert.NotNil(t, m)

		build1, m1, err := Build(path, MMap, 1024)
		assert.Nil(t, err)
		assert.NotNil(t, build1)
		assert.NotNil(t, m1)

		//t.Logf("%+v\n", build1)
		//t.Logf("%+v\n", m1)
	})
}

func writeDataForBuild(path string, method FileRWMethod, fileId uint32) {
	write := func(eType uint16, fid uint32) {
		file, err := NewDBFile(path, fid, method, 1024, eType)
		if err != nil {
			panic(err)
		}
		tests := []*Entry{
			NewEntryNoExtra([]byte("key-1"), []byte("val-1"), eType, 0),
			NewEntry([]byte("key-2"), []byte("val-2"), []byte("extra-something"), eType, 0),
			NewEntryWithExpire([]byte("key-3"), []byte("val-3"), time.Now().Unix(), eType, 0),
			NewEntryWithTxn([]byte("key-4"), []byte("val-4"), []byte("extra-something"), 101, eType, 0),
		}
		for _, tt := range tests {
			err := file.Write(tt)
			if err != nil {
				panic(err)
			}
		}
	}

	write(String, fileId)
	write(String, fileId+1)

	write(List, fileId)
	write(Hash, fileId)
	write(Set, fileId)
	write(ZSet, fileId)
}

func TestDBFile_Close(t *testing.T) {
	path := "/tmp/rosedb"
	_ = os.MkdirAll("/tmp/rosedb", os.ModePerm)
	defer os.RemoveAll(path)

	file, err := NewDBFile(path, 0, FileIO, 1024, String)
	assert.Nil(t, err)

	err = file.Close(true)
	assert.Nil(t, err)

	file1, err := NewDBFile(path, 0, MMap, 1024, String)
	assert.Nil(t, err)

	err = file1.Close(true)
	assert.Nil(t, err)
}

func TestDBFile_Read(t *testing.T) {
	path := "/tmp/rosedb"
	_ = os.MkdirAll("/tmp/rosedb", os.ModePerm)
	defer os.RemoveAll(path)

	tt := func(method FileRWMethod, fileId uint32) {
		writeForRead(path, method)
		file, err := NewDBFile(path, fileId, method, 1024, String)
		if err != nil {
			panic(err)
		}
		offset := []int64{0, 44, 103, 147}
		for _, off := range offset {
			e, err := file.Read(off)
			assert.Nil(t, err)

			//t.Logf("%+v\n", e)
			//t.Logf("%+v\n", string(e.Meta.Key))
			assert.NotNil(t, e)
		}
	}

	t.Run("fileio", func(t *testing.T) {
		tt(FileIO, 0)
	})

	t.Run("mmap", func(t *testing.T) {
		tt(MMap, 0)
	})
}

func writeForRead(path string, method FileRWMethod) {
	deadline := time.Now().Add(time.Second * 100).Unix()
	tests := []*Entry{
		NewEntryNoExtra([]byte("key-1"), []byte("val-1"), String, 0),
		NewEntry([]byte("key-2"), []byte("val-2"), []byte("extra-something"), String, 0),
		NewEntryWithExpire([]byte("key-3"), []byte("val-3"), deadline, String, 0),
		NewEntryWithTxn([]byte("key-4"), []byte("val-4"), []byte("extra-something"), 101, String, 0),
	}

	file, err := NewDBFile(path, 0, method, 1024, String)
	if err != nil {
		panic(err)
	}

	for _, e := range tests {
		if err := file.Write(e); err != nil {
			panic(err)
		}
	}
}

func TestDBFile_Sync(t *testing.T) {
	path := "/tmp/rosedb"
	_ = os.MkdirAll("/tmp/rosedb", os.ModePerm)
	defer os.RemoveAll(path)

	file, err := NewDBFile(path, 0, FileIO, 1024, String)
	assert.Nil(t, err)

	err = file.Sync()
	assert.Nil(t, err)

	file1, err := NewDBFile(path, 0, MMap, 1024, String)
	assert.Nil(t, err)

	err = file1.Sync()
	assert.Nil(t, err)
}

func TestDBFile_Write(t *testing.T) {
	path := "/tmp/rosedb"
	_ = os.MkdirAll("/tmp/rosedb", os.ModePerm)
	defer os.RemoveAll(path)

	var dbfile *DBFile
	var err error

	deadline := time.Now().Add(time.Second * 100).Unix()
	tests := []*Entry{
		NewEntryNoExtra([]byte("key-1"), []byte("val-1"), String, 0),
		NewEntry([]byte("key-2"), []byte("val-2"), []byte("extra-something"), String, 0),
		NewEntryWithExpire([]byte("key-3"), []byte("val-3"), deadline, String, 0),
		NewEntryWithTxn([]byte("key-4"), []byte("val-4"), []byte("extra-something"), 101, String, 0),
	}

	t.Run("file-io", func(t *testing.T) {
		dbfile, err = NewDBFile(path, 0, FileIO, 1*1024*1024, String)
		if err != nil {
			panic(err)
		}
		for _, tt := range tests {
			err := dbfile.Write(tt)
			assert.Nil(t, err)
		}
	})

	t.Run("mmap", func(t *testing.T) {
		dbfile, err = NewDBFile(path, 1, MMap, 1024, String)
		if err != nil {
			panic(err)
		}
		for _, tt := range tests {
			err := dbfile.Write(tt)
			assert.Nil(t, err)
		}
	})
}

func TestNewDBFile(t *testing.T) {
	path := "/tmp/rosedb"
	_ = os.MkdirAll("/tmp/rosedb", os.ModePerm)
	defer os.RemoveAll(path)

	type args struct {
		path      string
		fileId    uint32
		method    FileRWMethod
		blockSize int64
		eType     uint16
	}

	tests := []struct {
		name    string
		args    args
		want    *DBFile
		wantErr bool
	}{
		{"f-string", args{"/tmp/rosedb", 0, FileIO, 1 * 1024 * 1024, String}, nil, false},
		{"f-list", args{"/tmp/rosedb", 0, FileIO, 1 * 1024 * 1024, List}, nil, false},
		{"f-hash", args{"/tmp/rosedb", 0, FileIO, 1 * 1024 * 1024, Hash}, nil, false},
		{"f-set", args{"/tmp/rosedb", 0, FileIO, 1 * 1024 * 1024, Set}, nil, false},
		{"f-zset", args{"/tmp/rosedb", 0, FileIO, 1 * 1024 * 1024, ZSet}, nil, false},

		{"m-string", args{"/tmp/rosedb", 1, MMap, 1 * 1024 * 1024, String}, nil, false},
		{"m-list", args{"/tmp/rosedb", 2, MMap, 2 * 1024 * 1024, List}, nil, false},
		{"m-hash", args{"/tmp/rosedb", 3, MMap, 3 * 1024 * 1024, Hash}, nil, false},
		{"m-set", args{"/tmp/rosedb", 4, MMap, 4 * 1024 * 1024, Set}, nil, false},
		{"m-zset", args{"/tmp/rosedb", 5, MMap, 5 * 1024 * 1024, ZSet}, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewDBFile(tt.args.path, tt.args.fileId, tt.args.method, tt.args.blockSize, tt.args.eType)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewDBFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.NotNil(t, got)
		})
	}
}

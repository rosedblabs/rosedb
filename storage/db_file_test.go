package storage

import (
	"log"
	"testing"
)

const (
	path1            = "/Users/roseduan/resources/rosedb"
	fileId1          = 0
	path2            = "/Users/roseduan/resources/rosedb"
	fileId2          = 1
	defaultBlockSize = 8 * 1024 * 1024
)

func TestNewDBFile(t *testing.T) {

	newOne := func(method FileRWMethod) {
		file, err := NewDBFile(path1, fileId1, method, defaultBlockSize)
		if err != nil {
			t.Error("new db file error ", err)
		}

		t.Logf("%+v \n", file)
		t.Log(file.File == nil)
	}

	t.Run("new db file file io", func(t *testing.T) {
		newOne(FileIO)
	})

	t.Run("new db file mmap", func(t *testing.T) {
		newOne(MMap)
	})
}

func TestBuild(t *testing.T) {
	path := "/Users/roseduan/resources/rosedb/db2/"
	files, u, err := Build(path, FileIO, defaultBlockSize)
	if err != nil {
		log.Fatal(err)
	}

	t.Log(files)
	t.Log(u)
}

func TestDBFile_Write_FileIO(t *testing.T) {
	df, err := NewDBFile(path1, fileId1, FileIO, defaultBlockSize)

	if err != nil {
		t.Error(err)
	}

	entry1 := &Entry{
		Meta: &Meta{
			Key:   []byte("test001"),
			Value: []byte("test001"),
		},
	}
	entry1.Meta.KeySize = uint32(len(entry1.Meta.Key))
	entry1.Meta.ValueSize = uint32(len(entry1.Meta.Value))

	entry2 := &Entry{
		Meta: &Meta{
			Key:   []byte("test_key_002"),
			Value: []byte("test_val_002"),
		},
	}

	entry2.Meta.KeySize = uint32(len(entry2.Meta.Key))
	entry2.Meta.ValueSize = uint32(len(entry2.Meta.Value))

	err = df.Write(entry1)

	t.Log(df.Offset)

	err = df.Write(entry2)

	t.Log(df.Offset)

	defer func() {
		err = df.Close(true)
	}()

	if err != nil {
		t.Error("写入数据错误 : ", err)
	}
}

func TestDBFile_Read_FileIO(t *testing.T) {
	df, _ := NewDBFile(path1, fileId1, FileIO, defaultBlockSize)

	readEntry := func(offset int64) *Entry {
		if e, err := df.Read(offset); err != nil {
			t.Error("read db File error ", err)
		} else {
			return e
		}
		return nil
	}

	e1 := readEntry(0)
	t.Log(e1)
	t.Log(string(e1.Meta.Key), e1.Meta.KeySize, string(e1.Meta.Value), e1.Meta.ValueSize, e1.crc32)
	e2 := readEntry(30)
	t.Log(e2)
	t.Log(string(e2.Meta.Key), e2.Meta.KeySize, string(e2.Meta.Value), e2.Meta.ValueSize, e2.crc32)

	defer df.Close(false)
}

var df, _ = NewDBFile(path2, fileId2, MMap, defaultBlockSize)

func TestDBFile_Write_MMap(t *testing.T) {
	writeEntry := func(key, value []byte) {
		defer df.Sync()
		e := &Entry{
			Meta: &Meta{
				Key:   key,
				Value: value,
			},
		}

		e.Meta.KeySize = uint32(len(e.Meta.Key))
		e.Meta.ValueSize = uint32(len(e.Meta.Value))

		if err := df.Write(e); err != nil {
			t.Error("数据写入错误", err)
		}

		t.Log("Offset = ", df.Offset)
	}

	writeEntry([]byte("mmap_key_001"), []byte("mmap_val_001"))
	writeEntry([]byte("mmap_key_002"), []byte("mmap_val_002"))
}

func TestDBFile_Read_MMap(t *testing.T) {
	readEntry := func(offset int64) {
		if e, err := df.Read(offset); err != nil {
			t.Error("数据读取失败", err)
		} else {
			t.Log(e)
			t.Log(string(e.Meta.Key), e.Meta.KeySize, string(e.Meta.Value), e.Meta.ValueSize, e.crc32)
		}
	}

	readEntry(0)
	readEntry(40)
}

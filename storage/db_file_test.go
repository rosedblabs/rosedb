package storage

import "testing"

const (
	path1            = "/Users/roseduan/resources/rosedb"
	fileId1          = 0
	path2            = "/Users/roseduan/resources/rosedb"
	fileId2          = 1
	defaultBlockSize = 8 * 1024 * 1024
)

func TestDBFile_Write_FileIO(t *testing.T) {
	df, err := NewDBFile(path1, fileId1, FileIO, defaultBlockSize)

	if err != nil {
		t.Error(err)
	}

	entry := &Entry{
		Key:   []byte("test001"),
		Value: []byte("test001"),
	}
	entry.keySize = uint32(len(entry.Key))
	entry.valueSize = uint32(len(entry.Value))

	entry2 := &Entry{
		Key:   []byte("test_key_002"),
		Value: []byte("test_val_002"),
	}
	entry2.keySize = uint32(len(entry2.Key))
	entry2.valueSize = uint32(len(entry2.Value))

	err = df.Write(entry)

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

	readEntry := func(offset, n int64) *Entry {
		if e, err := df.Read(offset, n); err != nil {
			t.Error("read db file error ", err)
		} else {
			return e
		}
		return nil
	}

	e1 := readEntry(0, 26)
	t.Log(e1)
	t.Log(string(e1.Key), e1.keySize, string(e1.Value), e1.valueSize, e1.crc32)
	e2 := readEntry(26, 62-26)
	t.Log(e2)
	t.Log(string(e2.Key), e2.keySize, string(e2.Value), e2.valueSize, e2.crc32)

	defer df.Close(false)
}

var df, _ = NewDBFile(path2, fileId2, MMap, defaultBlockSize)

func TestDBFile_Write_MMap(t *testing.T) {
	writeEntry := func(key, value []byte) {
		defer df.Sync()
		e := &Entry{
			Key:   key,
			Value: value,
		}

		e.keySize = uint32(len(e.Key))
		e.valueSize = uint32(len(e.Value))

		if err := df.Write(e); err != nil {
			t.Error("数据写入错误", err)
		}

		t.Log("Offset = ", df.Offset)
	}

	writeEntry([]byte("mmap_key_001"), []byte("mmap_val_001"))
	writeEntry([]byte("mmap_key_002"), []byte("mmap_val_002"))
}

func TestDBFile_Read_MMap(t *testing.T) {
	readEntry := func(offset, n int64) {
		if e, err := df.Read(offset, n); err != nil {
			t.Error("数据读取失败", err)
		} else {
			t.Log(e)
			t.Log(string(e.Key), e.keySize, string(e.Value), e.valueSize, e.crc32)
		}
	}

	readEntry(0, 36)
	readEntry(36, 72-36)
}

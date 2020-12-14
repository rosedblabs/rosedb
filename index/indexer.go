package index

import (
	"encoding/binary"
	"io"
	"os"
	"rosedb/ds/skiplist"
)

const (
	indexerHeaderSize = 4*4 + 8
)

//数据索引定义
type Indexer struct {
	Key       []byte
	Value     []byte
	FileId    uint32 //存储数据的文件id
	EntrySize uint32 //数据条目(Entry)的大小
	Offset    int64  //Entry数据的查询起始位置
	KeySize   uint32
	ValueSize uint32
}

func (i *Indexer) Size() uint32 {
	return i.KeySize + i.ValueSize + indexerHeaderSize
}

//加载索引信息
func Build(t *skiplist.SkipList, path string) error {
	file, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}

	defer file.Close()

	var offset int64 = 0
	for {

		buf := make([]byte, indexerHeaderSize)
		if _, err := file.ReadAt(buf, offset); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		ks := binary.BigEndian.Uint32(buf[16:20])
		vs := binary.BigEndian.Uint32(buf[20:24])
		idx := &Indexer{
			FileId:    binary.BigEndian.Uint32(buf[:4]),
			EntrySize: binary.BigEndian.Uint32(buf[4:8]),
			Offset:    int64(binary.BigEndian.Uint64(buf[8:16])),
			KeySize:   ks,
			ValueSize: vs,
		}

		keyVal := make([]byte, ks+vs)
		if _, err = file.ReadAt(keyVal, indexerHeaderSize+offset); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		idx.Key, idx.Value = keyVal[:ks], keyVal[ks:ks+vs]
		t.Put(idx.Key, idx)

		offset += int64(idx.Size())
	}

	return nil
}

//保存索引信息
func Store(t *skiplist.SkipList, path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	defer file.Close()

	if t.Size > 0 {
		var offset int64 = 0
		handleFunc := func(e *skiplist.Element) bool {
			item := e.Value().(*Indexer)
			if item != nil {
				b := item.encode()
				if n, err := file.WriteAt(b, offset); err != nil {
					return false
				} else {
					offset += int64(n)
				}
			}
			return true
		}

		t.Foreach(handleFunc)
	}

	if err := file.Sync(); err != nil {
		return err
	}

	return nil
}

func (i *Indexer) encode() []byte {
	buf := make([]byte, i.Size())

	ks, vs := len(i.Key), len(i.Value)
	binary.BigEndian.PutUint32(buf[0:4], i.FileId)
	binary.BigEndian.PutUint32(buf[4:8], i.EntrySize)
	binary.BigEndian.PutUint64(buf[8:16], uint64(i.Offset))
	binary.BigEndian.PutUint32(buf[16:20], i.KeySize)
	binary.BigEndian.PutUint32(buf[20:24], i.ValueSize)

	copy(buf[indexerHeaderSize:indexerHeaderSize+ks], i.Key)
	copy(buf[indexerHeaderSize+ks:indexerHeaderSize+ks+vs], i.Value)

	return buf
}

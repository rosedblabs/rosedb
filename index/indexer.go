package index

import (
	"encoding/binary"
	"io"
	"os"
	"rosedb/storage"
)

const (
	indexerHeaderSize = 4*5 + 8
)

//数据索引定义
type Indexer struct {
	Meta      *storage.Meta //元数据信息
	FileId    uint32        //存储数据的文件id
	EntrySize uint32        //数据条目(Entry)的大小
	Offset    int64         //Entry数据的查询起始位置
}

func (i *Indexer) Size() uint32 {
	return i.Meta.KeySize + i.Meta.ValueSize + i.Meta.ExtraSize + indexerHeaderSize
}

//加载索引信息
func Build(t *SkipList, path string) error {
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
		es := binary.BigEndian.Uint32(buf[24:28])
		idx := &Indexer{
			FileId:    binary.BigEndian.Uint32(buf[:4]),
			EntrySize: binary.BigEndian.Uint32(buf[4:8]),
			Offset:    int64(binary.BigEndian.Uint64(buf[8:16])),
			Meta: &storage.Meta{
				KeySize:   ks,
				ValueSize: vs,
				ExtraSize: es,
			},
		}

		val := make([]byte, ks+vs+es)
		if _, err = file.ReadAt(val, indexerHeaderSize+offset); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		idx.Meta.Key, idx.Meta.Value = val[:ks], val[ks:ks+vs]
		if es > 0 {
			idx.Meta.Extra = val[ks+vs : ks+vs+es]
		}

		t.Put(idx.Meta.Key, idx)

		offset += int64(idx.Size())
	}

	return nil
}

//保存索引信息
func Store(t *SkipList, path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	defer file.Close()

	if t.Len > 0 {
		var offset int64 = 0
		handleFunc := func(e *Element) bool {
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

	ks, vs, es := len(i.Meta.Key), len(i.Meta.Value), len(i.Meta.Extra)
	binary.BigEndian.PutUint32(buf[0:4], i.FileId)
	binary.BigEndian.PutUint32(buf[4:8], i.EntrySize)
	binary.BigEndian.PutUint64(buf[8:16], uint64(i.Offset))
	binary.BigEndian.PutUint32(buf[16:20], i.Meta.KeySize)
	binary.BigEndian.PutUint32(buf[20:24], i.Meta.ValueSize)
	binary.BigEndian.PutUint32(buf[24:28], i.Meta.ExtraSize)

	copy(buf[indexerHeaderSize:indexerHeaderSize+ks], i.Meta.Key)
	copy(buf[indexerHeaderSize+ks:indexerHeaderSize+ks+vs], i.Meta.Value)
	if es > 0 {
		copy(buf[indexerHeaderSize+ks+vs:indexerHeaderSize+ks+vs+es], i.Meta.Extra)
	}

	return buf
}

package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var (
	ErrInvalidEntry = errors.New("storage/entry: invalid entry")
	ErrInvalidCrc   = errors.New("storage/entry: invalid crc")
)

const (
	//keySize, valueSize, crc32 均为 uint32 类型，各占 4 字节
	//4 + 4 + 4 = 12
	entryHeaderSize = 12
)

type Entry struct {
	Key       []byte
	Value     []byte
	keySize   uint32
	valueSize uint32
	crc32     uint32
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:       key,
		Value:     value,
		keySize:   uint32(len(key)),
		valueSize: uint32(len(value)),
	}
}

func (e *Entry) Size() uint32 {
	return entryHeaderSize + e.keySize + e.valueSize
}

//对Entry进行编码，返回字节数组
func (e *Entry) Encode() ([]byte, error) {
	if e == nil || e.keySize == 0 {
		return nil, ErrInvalidEntry
	}

	ks, vs := e.keySize, e.valueSize
	buf := make([]byte, e.Size())

	binary.BigEndian.PutUint32(buf[4:8], ks)
	binary.BigEndian.PutUint32(buf[8:12], vs)
	copy(buf[entryHeaderSize:entryHeaderSize+ks], e.Key)
	copy(buf[entryHeaderSize+ks:(entryHeaderSize+ks+vs)], e.Value)

	crc := crc32.ChecksumIEEE(e.Value)
	binary.BigEndian.PutUint32(buf[0:4], crc)

	return buf, nil
}

//解码字节数组，返回Entry
func Decode(buf []byte) (*Entry, error) {
	ks := binary.BigEndian.Uint32(buf[4:8])
	vs := binary.BigEndian.Uint32(buf[8:12])
	key := buf[entryHeaderSize : entryHeaderSize+ks]
	value := buf[entryHeaderSize+ks : (entryHeaderSize + ks + vs)]
	crc := binary.BigEndian.Uint32(buf[0:4])

	checkCrc := crc32.ChecksumIEEE(value)
	if checkCrc != crc {
		return nil, ErrInvalidCrc
	}

	return &Entry{
		keySize:   ks,
		valueSize: vs,
		Key:       key,
		Value:     value,
		crc32:     crc,
	}, nil
}

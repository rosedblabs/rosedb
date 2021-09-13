package utils

import (
	"bytes"
	"encoding/binary"
	"github.com/vmihailenco/msgpack/v5"
)

// EncodeKey returns key in bytes.
func EncodeKey(key interface{}) (res []byte, err error) {
	switch key.(type) {
	case []byte:
		return key.([]byte), nil
	case bool, float32, float64, complex64, complex128, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		buf := new(bytes.Buffer)
		err = binary.Write(buf, binary.BigEndian, key)
		return buf.Bytes(), err
	case int:
		val := uint64(key.(int))
		p := make([]byte, 8)
		p[0] = byte(val >> 56)
		p[1] = byte(val >> 48)
		p[2] = byte(val >> 40)
		p[3] = byte(val >> 32)
		p[4] = byte(val >> 24)
		p[5] = byte(val >> 16)
		p[6] = byte(val >> 8)
		p[7] = byte(val)
		return p, err
	case string:
		return []byte(key.(string)), nil
	default:
		res, err = msgpack.Marshal(key)
		return
	}
}

// EncodeValue returns value in bytes.
func EncodeValue(value interface{}) (res []byte, err error) {
	switch value.(type) {
	case []byte:
		return value.([]byte), nil
	case string:
		return []byte(value.(string)), err
	default:
		res, err = msgpack.Marshal(value)
		return
	}
}

// DecodeValue decode value to dest.
func DecodeValue(value []byte, dest interface{}) (err error) {
	switch dest.(type) {
	case *[]byte:
		*dest.(*[]byte) = value
	case *string:
		*dest.(*string) = string(value)
	default:
		err = msgpack.Unmarshal(value, dest)
		return
	}
	return
}

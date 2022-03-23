package list

import (
	"encoding/binary"
	"github.com/flower-corp/rosedb/logger"
	goart "github.com/plar/go-adaptive-radix-tree"
	"math"
)

const (
	initialSeq  = math.MaxUint32 / 2
	listMetaKey = "!|list|meta|!"
)

type List struct {
	records map[string]goart.Tree
}

type meta struct {
	headSeq uint32
	tailSeq uint32
}

func New() *List {
	return &List{records: make(map[string]goart.Tree)}
}

func (lis *List) LPush(key, value []byte) uint32 {
	lisKey := string(key)
	metaKey := lis.encodeMetaKey(key)
	if lis.records[lisKey] == nil {
		tree := goart.New()
		tree.Insert(metaKey, &meta{headSeq: initialSeq, tailSeq: initialSeq + 1})
		lis.records[lisKey] = tree
	}

	metaInfo := lis.getMeta(key)
	seq := metaInfo.headSeq
	encKey := EncodeKey(key, seq)
	lis.records[lisKey].Insert(encKey, value)

	// update meta
	metaInfo.headSeq--
	lis.records[lisKey].Insert(metaKey, metaInfo)
	return seq
}

func (lis *List) LPop(key []byte) ([]byte, uint32) {
	lisKey := string(key)
	if lis.records[lisKey] == nil {
		return nil, 0
	}

	metaKey := lis.encodeMetaKey(key)
	metaInfo := lis.getMeta(key)
	size := metaInfo.tailSeq - metaInfo.headSeq - 1
	if size <= 0 {
		// reset meta
		lis.records[lisKey].Insert(metaKey, &meta{
			headSeq: initialSeq,
			tailSeq: initialSeq + 1,
		})
		return nil, 0
	}

	seq := metaInfo.headSeq + 1
	encKey := EncodeKey(key, seq)
	value, _ := lis.records[lisKey].Delete(encKey)
	val, _ := value.([]byte)

	// update meta
	metaInfo.headSeq++
	lis.records[lisKey].Insert(metaKey, metaInfo)
	return val, seq
}

func (lis *List) RPush(key, value []byte) uint32 {
	lisKey := string(key)
	metaKey := lis.encodeMetaKey(key)
	if lis.records[lisKey] == nil {
		tree := goart.New()
		tree.Insert(metaKey, &meta{headSeq: initialSeq, tailSeq: initialSeq + 1})
		lis.records[lisKey] = tree
	}

	metaInfo := lis.getMeta(key)
	seq := metaInfo.tailSeq
	encKey := EncodeKey(key, seq)
	lis.records[lisKey].Insert(encKey, value)

	// update meta
	metaInfo.tailSeq++
	lis.records[lisKey].Insert(metaKey, metaInfo)
	return seq
}

func (lis *List) RPop(key []byte) ([]byte, uint32) {
	lisKey := string(key)
	if lis.records[lisKey] == nil {
		return nil, 0
	}

	metaKey := lis.encodeMetaKey(key)
	metaInfo := lis.getMeta(key)
	size := metaInfo.tailSeq - metaInfo.headSeq - 1
	if size <= 0 {
		// reset meta
		lis.records[lisKey].Insert(metaKey, &meta{
			headSeq: initialSeq,
			tailSeq: initialSeq + 1,
		})
		return nil, 0
	}

	seq := metaInfo.tailSeq - 1
	encKey := EncodeKey(key, seq)
	value, _ := lis.records[lisKey].Delete(encKey)
	val, _ := value.([]byte)

	// update meta
	metaInfo.tailSeq--
	lis.records[lisKey].Insert(metaKey, metaInfo)
	return val, seq
}

func (lis *List) getMeta(key []byte) *meta {
	metaKey := lis.encodeMetaKey(key)
	// get meta info
	metaRaw, found := lis.records[string(key)].Search(metaKey)
	if !found {
		logger.Fatalf("")
	}

	metaInfo, ok := metaRaw.(*meta)
	if !ok {
		logger.Fatalf("")
	}
	return metaInfo
}

func (lis *List) encodeMetaKey(key []byte) []byte {
	buf := make([]byte, len(key)+len(listMetaKey))
	copy(buf[:len(key)], key)
	copy(buf[len(key):], listMetaKey)
	return buf
}

func EncodeKey(key []byte, seq uint32) []byte {
	header := make([]byte, binary.MaxVarintLen32)
	var index int
	index += binary.PutVarint(header[index:], int64(seq))

	buf := make([]byte, len(key)+index)
	copy(buf[:index], header)
	copy(buf[index:], key)
	return buf
}

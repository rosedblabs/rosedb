package list

import (
	"encoding/binary"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	goart "github.com/plar/go-adaptive-radix-tree"
	"math"
)

const (
	initialSeq = math.MaxUint32 / 2
)

type Command uint8

const (
	LPush Command = iota
	RPush
	LPop
	RPop
	LSet
)

type List struct {
	records map[string]goart.Tree
	metas   map[string]*meta
}

type meta struct {
	headSeq uint32
	tailSeq uint32
}

func New() *List {
	return &List{
		records: make(map[string]goart.Tree),
		metas:   make(map[string]*meta),
	}
}

func (lis *List) LPush(key, value []byte) {
	lis.push(key, value, true)
}

func (lis *List) RPush(key, value []byte) {
	lis.push(key, value, false)
}

func (lis *List) push(key, value []byte, isLeft bool) {
	listKey := string(key)
	if lis.records[listKey] == nil {
		lis.records[listKey] = goart.New()
		lis.metas[listKey] = &meta{headSeq: initialSeq, tailSeq: initialSeq + 1}
	}

	metaInfo := lis.getMeta(key)
	seq := metaInfo.headSeq
	if !isLeft {
		seq = metaInfo.tailSeq
	}
	encKey := EncodeKey(key, seq)
	lis.records[listKey].Insert(encKey, value)

	// update meta
	if isLeft {
		metaInfo.headSeq--
	} else {
		metaInfo.tailSeq++
	}
}

func (lis *List) LPop(key []byte) []byte {
	return lis.pop(key, true)
}

func (lis *List) RPop(key []byte) []byte {
	return lis.pop(key, false)
}

func (lis *List) pop(key []byte, isLeft bool) []byte {
	listKey := string(key)
	if lis.records[listKey] == nil {
		return nil
	}

	metaInfo := lis.getMeta(key)
	size := metaInfo.tailSeq - metaInfo.headSeq - 1
	if size <= 0 {
		// reset meta
		lis.metas[listKey] = &meta{headSeq: initialSeq, tailSeq: initialSeq + 1}
		return nil
	}

	seq := metaInfo.headSeq + 1
	if !isLeft {
		seq = metaInfo.tailSeq - 1
	}
	encKey := EncodeKey(key, seq)
	value, _ := lis.records[listKey].Delete(encKey)
	var val []byte
	if value != nil {
		val, _ = value.([]byte)
	}

	// update meta
	if isLeft {
		metaInfo.headSeq++
	} else {
		metaInfo.tailSeq--
	}
	return val
}

func (lis *List) LIndex(key []byte, index int) []byte {
	listKey := string(key)
	if _, ok := lis.records[listKey]; !ok {
		return nil
	}

	metaInfo := lis.getMeta(key)
	size := metaInfo.tailSeq - metaInfo.headSeq - 1
	newIndex, ok := lis.validIndex(listKey, index, size)
	if !ok {
		return nil
	}

	encKey := EncodeKey(key, metaInfo.headSeq+uint32(newIndex)+1)
	value, _ := lis.records[listKey].Search(encKey)
	if value != nil {
		val, _ := value.([]byte)
		return val
	}
	return nil
}

func (lis *List) LSet(key []byte, index int, value []byte) bool {
	listKey := string(key)
	if _, ok := lis.records[listKey]; !ok {
		return false
	}

	metaInfo := lis.getMeta(key)
	size := metaInfo.tailSeq - metaInfo.headSeq - 1
	newIndex, ok := lis.validIndex(listKey, index, size)
	if !ok {
		return false
	}

	encKey := EncodeKey(key, metaInfo.headSeq+uint32(newIndex)+1)
	lis.records[listKey].Insert(encKey, value)
	return true
}

func (lis *List) LLen(key []byte) uint32 {
	listKey := string(key)
	if _, ok := lis.records[listKey]; !ok {
		return 0
	}
	metaInfo := lis.getMeta(key)
	size := metaInfo.tailSeq - metaInfo.headSeq - 1
	return size
}

// check if the index is valid and returns the new index.
func (lis *List) validIndex(key string, index int, size uint32) (int, bool) {
	item := lis.records[key]
	if item == nil || size <= 0 {
		return 0, false
	}

	if index < 0 {
		index += int(size)
	}
	return index, index >= 0 && index < int(size)
}

func (lis *List) getMeta(key []byte) *meta {
	metaInfo, ok := lis.metas[string(key)]
	if !ok {
		logger.Fatalf("fail to find meta info")
	}
	return metaInfo
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

func DecodeKey(buf []byte) ([]byte, uint32) {
	var index int
	seq, i := binary.Varint(buf[index:])
	index += i
	return buf[index:], uint32(seq)
}

func EncodeCommandKey(key []byte, cmd Command) []byte {
	buf := make([]byte, len(key)+1)
	buf[0] = byte(cmd)
	copy(buf[1:], key)
	return buf
}

func DecodeCommandKey(buf []byte) ([]byte, Command) {
	return buf[1:], Command(buf[0])
}

func (lis *List) IterateAndSend(chn chan *logfile.LogEntry) {
	for _, tree := range lis.records {
		iter := tree.Iterator()
		for iter.HasNext() {
			node, _ := iter.Next()
			if node == nil {
				continue
			}
			key, _ := DecodeKey(node.Key())
			value, _ := node.Value().([]byte)
			encKey := EncodeCommandKey(key, RPush)
			chn <- &logfile.LogEntry{Key: encKey, Value: value}
		}
	}
}

package rosedb

import (
	"bytes"
	"encoding/binary"
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"math"
)

// LPush insert all the specified values at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
func (db *RoseDB) LPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}
	for _, val := range values {
		if err := db.pushInternal(key, val, true); err != nil {
			return err
		}
	}
	return nil
}

// LPushX insert specified values at the head of the list stored at key,
// only if key already exists and holds a list.
// In contrary to LPUSH, no operation will be performed when key does not yet exist.
func (db *RoseDB) LPushX(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}

	for _, val := range values {
		if err := db.pushInternal(key, val, true); err != nil {
			return err
		}
	}
	return nil
}

// RPush insert all the specified values at the tail of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operation.
func (db *RoseDB) RPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}
	for _, val := range values {
		if err := db.pushInternal(key, val, false); err != nil {
			return err
		}
	}
	return nil
}

// RPushX insert specified values at the tail of the list stored at key,
// only if key already exists and holds a list.
// In contrary to RPUSH, no operation will be performed when key does not yet exist.
func (db *RoseDB) RPushX(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}
	for _, val := range values {
		if err := db.pushInternal(key, val, false); err != nil {
			return err
		}
	}
	return nil
}

// LPop removes and returns the first elements of the list stored at key.
func (db *RoseDB) LPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popInternal(key, true)
}

// RPop Removes and returns the last elements of the list stored at key.
func (db *RoseDB) RPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popInternal(key, false)
}

// LMove atomically returns and removes the first/last element of the list stored at source,
// and pushes the element at the first/last element of the list stored at destination.
func (db *RoseDB) LMove(srcKey, dstKey []byte, srcIsLeft, dstIsLeft bool) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	popValue, err := db.popInternal(srcKey, srcIsLeft)
	if err != nil {
		return nil, err
	}
	if popValue == nil {
		return nil, nil
	}

	if db.listIndex.trees[string(dstKey)] == nil {
		db.listIndex.trees[string(dstKey)] = art.NewART()
	}
	if err = db.pushInternal(dstKey, popValue, dstIsLeft); err != nil {
		return nil, err
	}

	return popValue, nil
}

// LLen returns the length of the list stored at key.
// If key does not exist, it is interpreted as an empty list and 0 is returned.
func (db *RoseDB) LLen(key []byte) int {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.listIndex.trees[string(key)] == nil {
		return 0
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return 0
	}
	return int(tailSeq - headSeq - 1)
}

// LIndex returns the element at index in the list stored at key.
// If index is out of range, it returns nil.
func (db *RoseDB) LIndex(key []byte, index int) ([]byte, error) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.listIndex.trees[string(key)] == nil {
		return nil, nil
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}

	seq, err := db.listSequence(headSeq, tailSeq, index)
	if err != nil {
		return nil, err
	}

	if seq >= tailSeq || seq <= headSeq {
		return nil, ErrWrongIndex
	}

	encKey := db.encodeListKey(key, seq)
	val, err := db.getVal(idxTree, encKey, List)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// LSet Sets the list element at index to element.
func (db *RoseDB) LSet(key []byte, index int, value []byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return err
	}

	seq, err := db.listSequence(headSeq, tailSeq, index)
	if err != nil {
		return err
	}

	if seq >= tailSeq || seq <= headSeq {
		return ErrWrongIndex
	}

	encKey := db.encodeListKey(key, seq)
	ent := &logfile.LogEntry{Key: encKey, Value: value}
	valuePos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}
	if err = db.updateIndexTree(idxTree, ent, valuePos, true, List); err != nil {
		return err
	}
	return nil
}

// LRange returns the specified elements of the list stored at key.
// The offsets start and stop are zero-based indexes, with 0 being the first element
// of the list (the head of the list), 1 being the next element and so on.
// These offsets can also be negative numbers indicating offsets starting at the end of the list.
// For example, -1 is the last element of the list, -2 the penultimate, and so on.
// If start is larger than the end of the list, an empty list is returned.
// If stop is larger than the actual end of the list, Redis will treat it like the last element of the list.
func (db *RoseDB) LRange(key []byte, start, end int) (values [][]byte, err error) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.listIndex.trees[string(key)] == nil {
		return nil, ErrKeyNotFound
	}

	idxTree := db.listIndex.trees[string(key)]
	// get List DataType meta info
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}

	var startSeq, endSeq uint32

	// logical address to physical address
	startSeq, err = db.listSequence(headSeq, tailSeq, start)
	if err != nil {
		return nil, err
	}
	endSeq, err = db.listSequence(headSeq, tailSeq, end)
	if err != nil {
		return nil, err
	}
	// normalize startSeq
	if startSeq <= headSeq {
		startSeq = headSeq + 1
	}

	// normalize endSeq
	if endSeq >= tailSeq {
		endSeq = tailSeq - 1
	}

	if startSeq >= tailSeq || endSeq <= headSeq || startSeq > endSeq {
		return nil, ErrWrongIndex
	}

	// the endSeq value is included
	for seq := startSeq; seq < endSeq+1; seq++ {
		encKey := db.encodeListKey(key, seq)
		val, err := db.getVal(idxTree, encKey, List)

		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

func (db *RoseDB) encodeListKey(key []byte, seq uint32) []byte {
	buf := make([]byte, len(key)+4)
	binary.LittleEndian.PutUint32(buf[:4], seq)
	copy(buf[4:], key[:])
	return buf
}

func (db *RoseDB) decodeListKey(buf []byte) ([]byte, uint32) {
	seq := binary.LittleEndian.Uint32(buf[:4])
	key := make([]byte, len(buf[4:]))
	copy(key[:], buf[4:])
	return key, seq
}

func (db *RoseDB) listMeta(idxTree *art.AdaptiveRadixTree, key []byte) (uint32, uint32, error) {
	val, err := db.getVal(idxTree, key, List)
	if err != nil && err != ErrKeyNotFound {
		return 0, 0, err
	}

	var headSeq uint32 = initialListSeq
	var tailSeq uint32 = initialListSeq + 1
	if len(val) != 0 {
		headSeq = binary.LittleEndian.Uint32(val[:4])
		tailSeq = binary.LittleEndian.Uint32(val[4:8])
	}
	return headSeq, tailSeq, nil
}

func (db *RoseDB) saveListMeta(idxTree *art.AdaptiveRadixTree, key []byte, headSeq, tailSeq uint32) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], headSeq)
	binary.LittleEndian.PutUint32(buf[4:8], tailSeq)
	ent := &logfile.LogEntry{Key: key, Value: buf, Type: logfile.TypeListMeta}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}
	err = db.updateIndexTree(idxTree, ent, pos, true, List)
	return err
}

func (db *RoseDB) pushInternal(key []byte, val []byte, isLeft bool) error {
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return err
	}
	var seq = headSeq
	if !isLeft {
		seq = tailSeq
	}
	encKey := db.encodeListKey(key, seq)
	ent := &logfile.LogEntry{Key: encKey, Value: val}
	valuePos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}

	if err = db.updateIndexTree(idxTree, ent, valuePos, true, List); err != nil {
		return err
	}

	if isLeft {
		headSeq--
	} else {
		tailSeq++
	}
	err = db.saveListMeta(idxTree, key, headSeq, tailSeq)
	return err
}

func (db *RoseDB) popInternal(key []byte, isLeft bool) ([]byte, error) {
	if db.listIndex.trees[string(key)] == nil {
		return nil, nil
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}
	if tailSeq-headSeq-1 <= 0 {
		return nil, nil
	}

	var seq = headSeq + 1
	if !isLeft {
		seq = tailSeq - 1
	}
	encKey := db.encodeListKey(key, seq)
	val, err := db.getVal(idxTree, encKey, List)
	if err != nil {
		return nil, err
	}

	ent := &logfile.LogEntry{Key: encKey, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return nil, err
	}
	oldVal, updated := idxTree.Delete(encKey)
	if isLeft {
		headSeq++
	} else {
		tailSeq--
	}
	if err = db.saveListMeta(idxTree, key, headSeq, tailSeq); err != nil {
		return nil, err
	}
	// send discard
	db.sendDiscard(oldVal, updated, List)
	_, entrySize := logfile.EncodeEntry(ent)
	node := &indexNode{fid: pos.fid, entrySize: entrySize}
	select {
	case db.discards[List].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	if tailSeq-headSeq-1 == 0 {
		// reset meta
		if headSeq != initialListSeq || tailSeq != initialListSeq+1 {
			headSeq = initialListSeq
			tailSeq = initialListSeq + 1
			_ = db.saveListMeta(idxTree, key, headSeq, tailSeq)
		}
		delete(db.listIndex.trees, string(key))
	}
	return val, nil
}

// listSequence just convert logical index to physical seq.
// whether physical seq is legal or not, just convert it
func (db *RoseDB) listSequence(headSeq, tailSeq uint32, index int) (uint32, error) {
	var seq uint32

	if index >= 0 {
		seq = headSeq + uint32(index) + 1
	} else {
		seq = tailSeq - uint32(-index)
	}
	return seq, nil
}

// LRem removes the first count occurrences of elements equal to element from the list stored at key.
// The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to element moving from head to tail.
// count < 0: Remove elements equal to element moving from tail to head.
// count = 0: Remove all elements equal to element.
// Note that this method will rewrite the values, so it maybe very slow.
func (db *RoseDB) LRem(key []byte, count int, value []byte) (int, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if count == 0 {
		count = math.MaxUint32
	}
	var discardCount int
	idxTree := db.listIndex.trees[string(key)]
	if idxTree == nil {
		return discardCount, nil
	}
	// get List DataType meta info
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return discardCount, err
	}

	reserveSeq, discardSeq, reserveValueSeq := make([]uint32, 0), make([]uint32, 0), make([][]byte, 0)
	classifyData := func(key []byte, seq uint32) error {
		encKey := db.encodeListKey(key, seq)
		val, err := db.getVal(idxTree, encKey, List)
		if err != nil {
			return err
		}
		if bytes.Equal(value, val) {
			discardSeq = append(discardSeq, seq)
			discardCount++
		} else {
			reserveSeq = append(reserveSeq, seq)
			temp := make([]byte, len(val))
			copy(temp, val)
			reserveValueSeq = append(reserveValueSeq, temp)
		}
		return nil
	}

	addReserveData := func(key []byte, value []byte, isLeft bool) error {
		if db.listIndex.trees[string(key)] == nil {
			db.listIndex.trees[string(key)] = art.NewART()
		}
		if err := db.pushInternal(key, value, isLeft); err != nil {
			return err
		}
		return nil
	}

	if count > 0 {
		// record discard data and reserve data
		for seq := headSeq + 1; seq < tailSeq; seq++ {
			if err := classifyData(key, seq); err != nil {
				return discardCount, err
			}
			if discardCount == count {
				break
			}
		}

		discardSeqLen := len(discardSeq)
		if discardSeqLen > 0 {
			// delete discard data
			for seq := headSeq + 1; seq <= discardSeq[discardSeqLen-1]; seq++ {
				if _, err := db.popInternal(key, true); err != nil {
					return discardCount, err
				}
			}
			// add reserve data
			for i := len(reserveSeq) - 1; i >= 0; i-- {
				if reserveSeq[i] < discardSeq[discardSeqLen-1] {
					if err := addReserveData(key, reserveValueSeq[i], true); err != nil {
						return discardCount, err
					}
				}
			}
		}
	} else {
		count = -count
		// record discard data and reserve data
		for seq := tailSeq - 1; seq > headSeq; seq-- {
			if err := classifyData(key, seq); err != nil {
				return discardCount, err
			}
			if discardCount == count {
				break
			}
		}

		discardSeqLen := len(discardSeq)
		if discardSeqLen > 0 {
			// delete discard data
			for seq := tailSeq - 1; seq >= discardSeq[discardSeqLen-1]; seq-- {
				if _, err := db.popInternal(key, false); err != nil {
					return discardCount, err
				}
			}
			// add reserve data
			for i := len(reserveSeq) - 1; i >= 0; i-- {
				if reserveSeq[i] > discardSeq[discardSeqLen-1] {
					if err := addReserveData(key, reserveValueSeq[i], false); err != nil {
						return discardCount, err
					}
				}
			}
		}
	}
	return discardCount, nil
}

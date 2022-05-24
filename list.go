package rosedb

import (
	"encoding/binary"
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
)

// LPush insert all the specified values at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
func (db *RoseDB) LPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}
	db.listIndex.idxTree = db.listIndex.trees[string(key)]
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

	db.listIndex.idxTree = db.listIndex.trees[string(key)]
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
	db.listIndex.idxTree = db.listIndex.trees[string(key)]
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
	db.listIndex.idxTree = db.listIndex.trees[string(key)]
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

// LMove Atomically returns and removes the first/last element of the list stored at source,
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
	db.listIndex.idxTree = db.listIndex.trees[string(dstKey)]
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
	db.listIndex.idxTree = db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(key)
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
	db.listIndex.idxTree = db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(key)
	if err != nil {
		return nil, err
	}

	var seq uint32
	if index >= 0 {
		seq = headSeq + uint32(index) + 1
		// out of range
		if seq >= tailSeq {
			return nil, nil
		}
	} else {
		seq = tailSeq - uint32(-index)
		// out of range
		if seq <= headSeq {
			return nil, nil
		}
	}
	encKey := db.encodeListKey(key, seq)
	val, err := db.getVal(encKey, List)
	if err != nil {
		return nil, err
	}
	return val, nil
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

func (db *RoseDB) listMeta(key []byte) (uint32, uint32, error) {
	val, err := db.getVal(key, List)
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

func (db *RoseDB) saveListMeta(key []byte, headSeq, tailSeq uint32) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], headSeq)
	binary.LittleEndian.PutUint32(buf[4:8], tailSeq)
	ent := &logfile.LogEntry{Key: key, Value: buf, Type: logfile.TypeListMeta}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}
	err = db.updateIndexTree(ent, pos, true, List)
	return err
}

func (db *RoseDB) pushInternal(key []byte, val []byte, isLeft bool) error {
	headSeq, tailSeq, err := db.listMeta(key)
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
	if err = db.updateIndexTree(ent, valuePos, true, List); err != nil {
		return err
	}

	if isLeft {
		headSeq--
	} else {
		tailSeq++
	}
	err = db.saveListMeta(key, headSeq, tailSeq)
	return err
}

func (db *RoseDB) popInternal(key []byte, isLeft bool) ([]byte, error) {
	if db.listIndex.trees[string(key)] == nil {
		return nil, nil
	}
	db.listIndex.idxTree = db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(key)
	if err != nil {
		return nil, err
	}
	size := tailSeq - headSeq - 1
	if size <= 0 {
		// reset meta
		if headSeq != initialListSeq || tailSeq != initialListSeq+1 {
			headSeq = initialListSeq
			tailSeq = initialListSeq + 1
			_ = db.saveListMeta(key, headSeq, tailSeq)
		}
		return nil, nil
	}

	var seq = headSeq + 1
	if !isLeft {
		seq = tailSeq - 1
	}
	encKey := db.encodeListKey(key, seq)
	val, err := db.getVal(encKey, List)
	if err != nil {
		return nil, err
	}

	ent := &logfile.LogEntry{Key: encKey, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return nil, err
	}
	oldVal, updated := db.listIndex.idxTree.Delete(encKey)
	if isLeft {
		headSeq++
	} else {
		tailSeq--
	}
	if err = db.saveListMeta(key, headSeq, tailSeq); err != nil {
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
	return val, nil
}

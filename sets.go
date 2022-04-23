package rosedb

import (
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"github.com/flower-corp/rosedb/util"
)

// SAdd add the specified members to the set stored at key.
// Specified members that are already a member of this set are ignored.
// If key does not exist, a new set is created before adding the specified members.
func (db *RoseDB) SAdd(key []byte, members ...[]byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		db.setIndex.trees[string(key)] = art.NewART()
	}
	db.setIndex.idxTree = db.setIndex.trees[string(key)]
	for _, mem := range members {
		if len(mem) == 0 {
			continue
		}
		if err := db.setIndex.murhash.Write(mem); err != nil {
			return err
		}
		sum := db.setIndex.murhash.EncodeSum128()
		db.setIndex.murhash.Reset()

		ent := &logfile.LogEntry{Key: key, Value: mem}
		valuePos, err := db.writeLogEntry(ent, Set)
		if err != nil {
			return err
		}
		entry := &logfile.LogEntry{Key: sum, Value: mem}
		_, size := logfile.EncodeEntry(ent)
		valuePos.entrySize = size
		if err := db.updateIndexTree(entry, valuePos, true, Set); err != nil {
			return err
		}
	}
	return nil
}

// SPop removes and returns one or more random members from the set value store at key.
func (db *RoseDB) SPop(key []byte, count uint) ([][]byte, error) {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()
	if db.setIndex.trees[string(key)] == nil {
		return nil, nil
	}
	db.setIndex.idxTree = db.setIndex.trees[string(key)]

	var values [][]byte
	iter := db.setIndex.idxTree.Iterator()
	for iter.HasNext() && count > 0 {
		count--
		node, _ := iter.Next()
		if node == nil {
			continue
		}
		val, err := db.getVal(node.Key(), Set)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	for _, val := range values {
		if err := db.sremInternal(key, val); err != nil {
			return nil, err
		}
	}
	return values, nil
}

// SRem remove the specified members from the set stored at key.
// Specified members that are not a member of this set are ignored.
// If key does not exist, it is treated as an empty set and this command returns 0.
func (db *RoseDB) SRem(key []byte, members ...[]byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		return nil
	}
	db.setIndex.idxTree = db.setIndex.trees[string(key)]
	for _, mem := range members {
		if err := db.sremInternal(key, mem); err != nil {
			return err
		}
	}
	return nil
}

// SIsMember returns if member is a member of the set stored at key.
func (db *RoseDB) SIsMember(key, member []byte) bool {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.setIndex.trees[string(key)] == nil {
		return false
	}
	db.setIndex.idxTree = db.setIndex.trees[string(key)]
	if err := db.setIndex.murhash.Write(member); err != nil {
		return false
	}
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()
	node := db.setIndex.idxTree.Get(sum)
	return node != nil
}

// SMembers returns all the members of the set value stored at key.
func (db *RoseDB) SMembers(key []byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	return db.sMembers(key)
}

// SCard returns the set cardinality (number of elements) of the set stored at key.
func (db *RoseDB) SCard(key []byte) int {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	if db.setIndex.trees[string(key)] == nil {
		return 0
	}
	return db.setIndex.trees[string(key)].Size()
}

// SDiff returns the members of the set difference between the first set and
// all the successive sets. Returns error if no key is passed as a parameter.
func (db *RoseDB) SDiff(keys ...[]byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}
	if len(keys) == 1 {
		return db.sMembers(keys[0])
	}

	firstSet, err := db.sMembers(keys[0])
	if err != nil {
		return nil, err
	}
	successiveSet := make(map[uint64]struct{})
	for _, key := range keys[1:] {
		members, err := db.sMembers(key)
		if err != nil {
			return nil, err
		}
		for _, k := range members {
			h := util.MemHash(k)
			if _, ok := successiveSet[h]; !ok {
				successiveSet[h] = struct{}{}
			}
		}
	}
	if len(successiveSet) == 0 {
		return firstSet, nil
	}
	res := make([][]byte, 0)
	for _, k := range firstSet {
		h := util.MemHash(k)
		if _, ok := successiveSet[h]; !ok {
			res = append(res, k)
		}
	}
	return res, nil
}

func (db *RoseDB) sremInternal(key []byte, member []byte) error {
	db.setIndex.idxTree = db.setIndex.trees[string(key)]
	if err := db.setIndex.murhash.Write(member); err != nil {
		return err
	}
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()

	val, updated := db.setIndex.idxTree.Delete(sum)
	if !updated {
		return nil
	}
	entry := &logfile.LogEntry{Key: key, Value: sum, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, Set)
	if err != nil {
		return err
	}

	db.sendDiscard(val, updated, Set)
	// The deleted entry itself is also invalid.
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discards[Set].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	return nil
}

// sMembers is a helper method to get all members of the given set key.
func (db *RoseDB) sMembers(key []byte) ([][]byte, error) {
	if db.setIndex.trees[string(key)] == nil {
		return nil, nil
	}

	var values [][]byte
	db.setIndex.idxTree = db.setIndex.trees[string(key)]
	iterator := db.setIndex.idxTree.Iterator()
	for iterator.HasNext() {
		node, _ := iterator.Next()
		if node == nil {
			continue
		}
		val, err := db.getVal(node.Key(), Set)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

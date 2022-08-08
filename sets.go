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
	idxTree := db.setIndex.trees[string(key)]
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
		if err := db.updateIndexTree(idxTree, entry, valuePos, true, Set); err != nil {
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
	idxTree := db.setIndex.trees[string(key)]

	var values [][]byte
	iter := idxTree.Iterator()
	for iter.HasNext() && count > 0 {
		count--
		node, _ := iter.Next()
		if node == nil {
			continue
		}
		val, err := db.getVal(idxTree, node.Key(), Set)
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
	idxTree := db.setIndex.trees[string(key)]
	if err := db.setIndex.murhash.Write(member); err != nil {
		return false
	}
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()
	node := idxTree.Get(sum)
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
		for _, value := range members {
			h := util.MemHash(value)
			if _, ok := successiveSet[h]; !ok {
				successiveSet[h] = struct{}{}
			}
		}
	}
	if len(successiveSet) == 0 {
		return firstSet, nil
	}
	res := make([][]byte, 0)
	for _, value := range firstSet {
		h := util.MemHash(value)
		if _, ok := successiveSet[h]; !ok {
			res = append(res, value)
		}
	}
	return res, nil
}

// SDiffStore is equal to SDiff, but instead of returning the resulting set, it is stored in first param.
func (db *RoseDB) SDiffStore(keys ...[]byte) (int, error) {
	destination := keys[0]
	diff, err := db.SDiff(keys[1:]...)
	if err != nil {
		return -1, err
	}
	if err := db.sStore(destination, diff); err != nil {
		return -1, err
	}
	return db.SCard(destination), nil
}

// SUnion returns the members of the set resulting from the union of all the given sets.
func (db *RoseDB) SUnion(keys ...[]byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}
	if len(keys) == 1 {
		return db.sMembers(keys[0])
	}

	set := make(map[uint64]struct{})
	unionSet := make([][]byte, 0)
	for _, key := range keys {
		values, err := db.sMembers(key)
		if err != nil {
			return nil, err
		}
		for _, val := range values {
			h := util.MemHash(val)
			if _, ok := set[h]; !ok {
				set[h] = struct{}{}
				unionSet = append(unionSet, val)
			}
		}
	}
	return unionSet, nil
}

//SUnionStore Store the union result in first param
func (db *RoseDB) SUnionStore(keys ...[]byte) (int, error) {
	destination := keys[0]
	union, err := db.SUnion(keys[1:]...)
	if err != nil {
		return -1, err
	}
	if err := db.sStore(destination, union); err != nil {
		return -1, err
	}
	return db.SCard(destination), nil
}

func (db *RoseDB) sremInternal(key []byte, member []byte) error {
	idxTree := db.setIndex.trees[string(key)]
	if err := db.setIndex.murhash.Write(member); err != nil {
		return err
	}
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()

	val, updated := idxTree.Delete(sum)
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
	idxTree := db.setIndex.trees[string(key)]
	iterator := idxTree.Iterator()
	for iterator.HasNext() {
		node, _ := iterator.Next()
		if node == nil {
			continue
		}
		val, err := db.getVal(idxTree, node.Key(), Set)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

// SInter returns the members of the set resulting from the inter of all the given sets.
func (db *RoseDB) SInter(keys ...[]byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}
	if len(keys) == 1 {
		return db.sMembers(keys[0])
	}
	num := len(keys)
	set := make(map[uint64]int)
	interSet := make([][]byte, 0)
	for _, key := range keys {
		values, err := db.sMembers(key)
		if err != nil {
			return nil, err
		}
		for _, val := range values {
			h := util.MemHash(val)
			set[h]++
			if set[h] == num {
				interSet = append(interSet, val)
			}
		}
	}
	return interSet, nil
}

//SInterStore Store the inter result in first param
func (db *RoseDB) SInterStore(keys ...[]byte) (int, error) {
	destination := keys[0]
	inter, err := db.SInter(keys[1:]...)
	if err != nil {
		return -1, err
	}
	if err := db.sStore(destination, inter); err != nil {
		return -1, err
	}
	return db.SCard(destination), nil
}

//sStore store vals in the set the destination points to
//sStore is called in SInterStore SUnionStore SDiffStore
func (db *RoseDB) sStore(destination []byte, vals [][]byte) error {
	for _, val := range vals {
		if isMember := db.SIsMember(destination, val); !isMember {
			if err := db.SAdd(destination, val); err != nil {
				return err
			}
		}
	}
	return nil
}

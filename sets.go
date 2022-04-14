package rosedb

import (
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
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
		ent := &logfile.LogEntry{Key: key, Value: mem}
		valuePos, err := db.writeLogEntry(ent, Set)
		if err != nil {
			return err
		}
		if err := db.updateIndexTree(ent, valuePos, true, Set); err != nil {
			return err
		}
	}
	return nil
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
		val, updated := db.setIndex.idxTree.Delete(mem)
		if !updated {
			continue
		}
		entry := &logfile.LogEntry{Key: key, Value: mem, Type: logfile.TypeDelete}
		pos, err := db.writeLogEntry(entry, Set)
		if err != nil {
			return err
		}

		db.sendDiscard(val, updated)
		// The deleted entry itself is also invalid.
		_, size := logfile.EncodeEntry(entry)
		node := &indexNode{fid: pos.fid, entrySize: size}
		select {
		case db.discard.valChan <- node:
		default:
			logger.Warn("send to discard chan fail")
		}
	}
	return nil
}

// SMembers returns all the members of the set value stored at key.
func (db *RoseDB) SMembers(key []byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

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
		values = append(values, node.Key())
	}
	return values, nil
}

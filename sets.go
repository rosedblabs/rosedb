package rosedb

import (
	"github.com/flower-corp/rosedb/logfile"
)

// SAdd add the specified members to the set stored at key.
// Specified members that are already a member of this set are ignored.
// If key does not exist, a new set is created before adding the specified members.
func (db *RoseDB) SAdd(key []byte, members ...[]byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	for _, mem := range members {
		entry := &logfile.LogEntry{Key: key, Value: mem}
		if _, err := db.writeLogEntry(entry, Set); err != nil {
			return err
		}
		db.setIndex.indexes.SAdd(string(key), mem)
	}
	return nil
}

// SRem remove the specified members from the set stored at key.
// Specified members that are not a member of this set are ignored.
// If key does not exist, it is treated as an empty set and this command returns 0.
func (db *RoseDB) SRem(key []byte, members ...[]byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	for _, mem := range members {
		if ok := db.setIndex.indexes.SRem(string(key), mem); ok {
			entry := &logfile.LogEntry{Key: key, Value: mem, Type: logfile.TypeDelete}
			if _, err := db.writeLogEntry(entry, Set); err != nil {
				return err
			}
		}
	}
	return nil
}

// SPop removes and returns one or more random members from the set value store at key.
func (db *RoseDB) SPop(key []byte, count int) ([][]byte, error) {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	values := db.setIndex.indexes.SPop(string(key), count)
	for _, val := range values {
		entry := &logfile.LogEntry{Key: key, Value: val, Type: logfile.TypeDelete}
		if _, err := db.writeLogEntry(entry, Set); err != nil {
			return nil, err
		}
	}
	return values, nil
}

// SIsMember returns if member is a member of the set stored at key.
func (db *RoseDB) SIsMember(key, member []byte) bool {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	return db.setIndex.indexes.SIsMember(string(key), member)
}

// SMove move member from the set at source to the set at destination.
func (db *RoseDB) SMove(src []byte, dst []byte, member []byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	srcEntry := &logfile.LogEntry{Key: src, Value: member, Type: logfile.TypeDelete}
	if _, err := db.writeLogEntry(srcEntry, Set); err != nil {
		return err
	}
	dstEntry := &logfile.LogEntry{Key: dst, Value: member}
	if _, err := db.writeLogEntry(dstEntry, Set); err != nil {
		return err
	}
	db.setIndex.indexes.SMove(string(src), string(dst), member)
	return nil
}

// SCard returns the set cardinality (number of elements) of the set stored at key.
func (db *RoseDB) SCard(key []byte) int {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	return db.setIndex.indexes.SCard(string(key))
}

// SMembers returns all the members of the set value stored at key.
func (db *RoseDB) SMembers(key []byte) (values [][]byte) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	return db.setIndex.indexes.SMembers(string(key))
}

func (db *RoseDB) iterateSetsAndSend(chn chan *logfile.LogEntry) {
	db.setIndex.indexes.IterateAndSend(chn)
	close(chn)
}

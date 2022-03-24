package rosedb

import "github.com/flower-corp/rosedb/logfile"

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

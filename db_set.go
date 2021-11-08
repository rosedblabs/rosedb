package rosedb

import (
	"sync"
	"time"

	"github.com/roseduan/rosedb/ds/set"
	"github.com/roseduan/rosedb/storage"
	"github.com/roseduan/rosedb/utils"
)

// SetIdx the set idx
type SetIdx struct {
	mu      *sync.RWMutex
	indexes *set.Set
}

// newSetIdx create new set index.
func newSetIdx() *SetIdx {
	return &SetIdx{indexes: set.New(), mu: new(sync.RWMutex)}
}

// SAdd add the specified members to the set stored at key.
// Specified members that are already a member of this set are ignored.
// If key does not exist, a new set is created before adding the specified members.
func (db *RoseDB) SAdd(key interface{}, members ...interface{}) (res int, err error) {

	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return -1, err
	}

	var encMembers [][]byte

	for i := 0; i < len(members); i++ {
		eval, err := utils.EncodeValue(members[i])

		if err != nil {
			return -1, err
		}

		if err = db.checkKeyValue(encKey, eval); err != nil {
			return -1, err
		}

		encMembers = append(encMembers, eval)
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	for _, m := range encMembers {
		exist := db.setIndex.indexes.SIsMember(string(encKey), m)
		if !exist {
			e := storage.NewEntryNoExtra(encKey, m, Set, SetSAdd)
			if err = db.store(e); err != nil {
				return
			}
			res = db.setIndex.indexes.SAdd(string(encKey), m)
		}
	}
	return
}

// SPop removes and returns one or more random members from the set value store at key.
func (db *RoseDB) SPop(key interface{}, count int) (values [][]byte, err error) {

	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return nil, err
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(encKey, Set) {
		return nil, ErrKeyExpired
	}

	values = db.setIndex.indexes.SPop(string(encKey), count)
	for _, v := range values {
		e := storage.NewEntryNoExtra(encKey, v, Set, SetSRem)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

// SIsMember returns if member is a member of the set stored at key.
func (db *RoseDB) SIsMember(key, member interface{}) bool {

	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return false
	}

	if err = db.checkKeyValue(encKey, encMember); err != nil {
		return false
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(encKey, Set) {
		return false
	}
	return db.setIndex.indexes.SIsMember(string(encKey), encMember)
}

// SRandMember returns a random element from the set value stored at key.
// count > 0: if count less than set`s card, returns an array containing count different elements. if count greater than set`s card, the entire set will be returned.
// count < 0: the command is allowed to return the same element multiple times, and in this case, the number of returned elements is the absolute value of the specified count.
func (db *RoseDB) SRandMember(key interface{}, count int) [][]byte {

	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return nil
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(encKey, Set) {
		return nil
	}
	return db.setIndex.indexes.SRandMember(string(encKey), count)
}

// SRem remove the specified members from the set stored at key.
// Specified members that are not a member of this set are ignored.
// If key does not exist, it is treated as an empty set and this command returns 0.
func (db *RoseDB) SRem(key interface{}, members ...interface{}) (res int, err error) {

	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return -1, err
	}

	var encMembers [][]byte

	for i := 0; i < len(members); i++ {
		eval, err := utils.EncodeValue(members[i])

		if err != nil {
			return -1, err
		}

		if err = db.checkKeyValue(encKey, eval); err != nil {
			return -1, err
		}

		encMembers = append(encMembers, eval)
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(encKey, Set) {
		return
	}

	for _, m := range encMembers {
		if ok := db.setIndex.indexes.SRem(string(encKey), m); ok {
			e := storage.NewEntryNoExtra(encKey, m, Set, SetSRem)
			if err = db.store(e); err != nil {
				return
			}
			res++
		}
	}
	return
}

// SMove move member from the set at source to the set at destination.
func (db *RoseDB) SMove(src []byte, dst []byte, member interface{}) error {

	eval, err := utils.EncodeValue(member)

	if err != nil {
		return err
	}

	if err = db.checkKeyValue(nil, eval); err != nil {
		return err
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(src, Set) {
		return ErrKeyExpired
	}
	if db.checkExpired(dst, Set) {
		return ErrKeyExpired
	}

	if ok := db.setIndex.indexes.SMove(string(src), string(dst), eval); ok {
		e := storage.NewEntry(src, eval, dst, Set, SetSMove)
		if err := db.store(e); err != nil {
			return err
		}
	}
	return nil
}

// SCard returns the set cardinality (number of elements) of the set stored at key.
func (db *RoseDB) SCard(key interface{}) int {

	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return 0
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return 0
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(encKey, Set) {
		return 0
	}
	return db.setIndex.indexes.SCard(string(encKey))
}

// SMembers returns all the members of the set value stored at key.
func (db *RoseDB) SMembers(key interface{}) (val [][]byte) {
	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return nil
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(encKey, Set) {
		return
	}
	return db.setIndex.indexes.SMembers(string(encKey))
}

// SUnion returns the members of the set resulting from the union of all the given sets.
func (db *RoseDB) SUnion(keys ...interface{}) (val [][]byte) {

	if keys == nil || len(keys) == 0 {
		return
	}

	var encKeys [][]byte
	for i := 0; i < len(keys); i++ {
		enckey, err := utils.EncodeKey(keys[i])

		if err != nil {
			return nil
		}

		if err := db.checkKeyValue(enckey, nil); err != nil {
			return nil
		}

		encKeys = append(encKeys, enckey)
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	var validKeys []string
	for _, k := range encKeys {
		if db.checkExpired(k, Set) {
			continue
		}
		validKeys = append(validKeys, string(k))
	}

	return db.setIndex.indexes.SUnion(validKeys...)
}

// SDiff returns the members of the set resulting from the difference between the first set and all the successive sets.
func (db *RoseDB) SDiff(keys ...interface{}) (val [][]byte) {
	if keys == nil || len(keys) == 0 {
		return
	}

	var encKeys [][]byte
	for i := 0; i < len(keys); i++ {
		enckey, err := utils.EncodeKey(keys[i])

		if err != nil {
			return nil
		}

		if err := db.checkKeyValue(enckey, nil); err != nil {
			return nil
		}

		encKeys = append(encKeys, enckey)
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	var validKeys []string
	for _, k := range encKeys {
		if db.checkExpired(k, Set) {
			continue
		}
		validKeys = append(validKeys, string(k))
	}

	return db.setIndex.indexes.SDiff(validKeys...)
}

// SKeyExists returns if the key exists.
func (db *RoseDB) SKeyExists(key interface{}) (ok bool) {

	enckey, err := utils.EncodeKey(key)

	if err != nil {
		return
	}

	if err := db.checkKeyValue(enckey, nil); err != nil {
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(enckey, Set) {
		return
	}

	ok = db.setIndex.indexes.SKeyExists(string(enckey))
	return
}

// SClear clear the specified key in set.
func (db *RoseDB) SClear(key interface{}) (err error) {
	if !db.SKeyExists(key) {
		return ErrKeyNotExist
	}

	enckey, err := utils.EncodeKey(key)

	if err != nil {
		return
	}

	if err := db.checkKeyValue(enckey, nil); err != nil {
		return err
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(enckey, nil, Set, SetSClear)
	if err = db.store(e); err != nil {
		return
	}
	db.setIndex.indexes.SClear(string(enckey))
	return
}

// SExpire set expired time for the key in set.
func (db *RoseDB) SExpire(key interface{}, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}
	if !db.SKeyExists(key) {
		return ErrKeyNotExist
	}

	enckey, err := utils.EncodeKey(key)

	if err != nil {
		return
	}

	if err := db.checkKeyValue(enckey, nil); err != nil {
		return err
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(enckey, nil, deadline, Set, SetSExpire)
	if err = db.store(e); err != nil {
		return
	}
	db.expires[Set][string(enckey)] = deadline
	return
}

// STTL return time to live for the key in set.
func (db *RoseDB) STTL(key interface{}) (ttl int64) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	enckey, err := utils.EncodeKey(key)

	if err != nil {
		return
	}

	if err := db.checkKeyValue(enckey, nil); err != nil {
		return
	}

	if db.checkExpired(enckey, Set) {
		return
	}

	deadline, exist := db.expires[Set][string(enckey)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}

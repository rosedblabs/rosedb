package rosedb

import (
	"bytes"
	"github.com/roseduan/rosedb/ds/list"
	"github.com/roseduan/rosedb/storage"
	"github.com/roseduan/rosedb/utils"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ListIdx the list index.
type ListIdx struct {
	mu      *sync.RWMutex
	indexes *list.List
}

func newListIdx() *ListIdx {
	return &ListIdx{
		indexes: list.New(), mu: new(sync.RWMutex),
	}
}

// LPush insert all the specified values at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
func (db *RoseDB) LPush(key interface{}, values ...interface{}) (res int, err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return -1, err
	}
	var encVals [][]byte
	for i := 0; i < len(values); i++ {
		eval, err := utils.EncodeValue(values[i])
		if err != nil {
			return -1, err
		}
		if err := db.checkKeyValue(encKey, eval); err != nil {
			return -1, err
		}
		encVals = append(encVals, eval)
	}
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	for _, val := range encVals {
		e := storage.NewEntryNoExtra(encKey, val, List, ListLPush)
		if err = db.store(e); err != nil {
			return
		}

		res = db.listIndex.indexes.LPush(string(encKey), val)
	}
	return
}

// RPush insert all the specified values at the tail of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operation.
func (db *RoseDB) RPush(key interface{}, values ...interface{}) (res int, err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return -1, err
	}
	var encVals [][]byte
	for i := 0; i < len(values); i++ {
		eval, err := utils.EncodeValue(values[i])
		if err != nil {
			return -1, err
		}
		if err := db.checkKeyValue(encKey, eval); err != nil {
			return -1, err
		}
		encVals = append(encVals, eval)
	}
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	for _, val := range encVals {
		e := storage.NewEntryNoExtra(encKey, val, List, ListRPush)
		if err = db.store(e); err != nil {
			return
		}

		res = db.listIndex.indexes.RPush(string(encKey), val)
	}
	return
}

// LPop removes and returns the first elements of the list stored at key.
func (db *RoseDB) LPop(key interface{}) ([]byte, error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil, err
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil, err
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(encKey, List) {
		return nil, ErrKeyExpired
	}

	val := db.listIndex.indexes.LPop(string(encKey))
	if val != nil {
		e := storage.NewEntryNoExtra(encKey, val, List, ListLPop)
		if err := db.store(e); err != nil {
			return nil, err
		}
	}
	return val, nil
}

// Removes and returns the last elements of the list stored at key.
func (db *RoseDB) RPop(key interface{}) ([]byte, error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil, err
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil, err
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(encKey, List) {
		return nil, ErrKeyExpired
	}

	val := db.listIndex.indexes.RPop(string(encKey))
	if val != nil {
		e := storage.NewEntryNoExtra(encKey, val, List, ListRPop)
		if err := db.store(e); err != nil {
			return nil, err
		}
	}
	return val, nil
}

// LIndex returns the element at index index in the list stored at key.
// The index is zero-based, so 0 means the first element, 1 the second element and so on.
// Negative indices can be used to designate elements starting at the tail of the list. Here, -1 means the last element, -2 means the penultimate and so forth.
func (db *RoseDB) LIndex(key interface{}, idx int) []byte {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	return db.listIndex.indexes.LIndex(string(encKey), idx)
}

// LRem removes the first count occurrences of elements equal to element from the list stored at key.
// The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to element moving from head to tail.
// count < 0: Remove elements equal to element moving from tail to head.
// count = 0: Remove all elements equal to element.
func (db *RoseDB) LRem(key, value interface{}, count int) (int, error) {
	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return 0, nil
	}
	if err := db.checkKeyValue(encKey, encVal); err != nil {
		return 0, nil
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(encKey, List) {
		return 0, ErrKeyExpired
	}

	res := db.listIndex.indexes.LRem(string(encKey), encVal, count)
	if res > 0 {
		c := strconv.Itoa(count)
		e := storage.NewEntry(encKey, encVal, []byte(c), List, ListLRem)
		if err := db.store(e); err != nil {
			return res, err
		}
	}
	return res, nil
}

// LInsert inserts element in the list stored at key either before or after the reference value pivot.
func (db *RoseDB) LInsert(key string, option list.InsertOption, pivot, val interface{}) (count int, err error) {
	encVal, err := utils.EncodeValue(val)
	if err != nil {
		return
	}
	envPivot, err := utils.EncodeValue( pivot)
	if err != nil {
		return
	}
	if err = db.checkKeyValue([]byte(key), encVal); err != nil {
		return
	}

	if strings.Contains(string(envPivot), ExtraSeparator) {
		return 0, ErrExtraContainsSeparator
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	count = db.listIndex.indexes.LInsert(key, option, envPivot, encVal)
	if count != -1 {
		var buf bytes.Buffer
		buf.Write(envPivot)
		buf.Write([]byte(ExtraSeparator))
		opt := strconv.Itoa(int(option))
		buf.Write([]byte(opt))

		e := storage.NewEntry([]byte(key), encVal, buf.Bytes(), List, ListLInsert)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

// LSet sets the list element at index to element.
// returns whether is successful.
func (db *RoseDB) LSet(key interface{}, idx int, val interface{}) (ok bool, err error) {
	encKey, encVal, err := db.encode(key, val)
	if err != nil {
		return false, err
	}
	if err := db.checkKeyValue(encKey, encVal); err != nil {
		return false, err
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if ok = db.listIndex.indexes.LSet(string(encKey), idx, encVal); ok {
		i := strconv.Itoa(idx)
		e := storage.NewEntry(encKey, encVal, []byte(i), List, ListLSet)
		if err := db.store(e); err != nil {
			return false, err
		}
	}
	return
}

// LTrim trim an existing list so that it will contain only the specified range of elements specified.
// Both start and stop are zero-based indexes, where 0 is the first element of the list (the head), 1 the next element and so on.
func (db *RoseDB) LTrim(key interface{}, start, end int) error {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return err
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(encKey, List) {
		return ErrKeyExpired
	}

	if res := db.listIndex.indexes.LTrim(string(encKey), start, end); res {
		var buf bytes.Buffer
		buf.Write([]byte(strconv.Itoa(start)))
		buf.Write([]byte(ExtraSeparator))
		buf.Write([]byte(strconv.Itoa(end)))

		e := storage.NewEntry(encKey, nil, buf.Bytes(), List, ListLTrim)
		if err := db.store(e); err != nil {
			return err
		}
	}
	return nil
}

// LRange returns the specified elements of the list stored at key.
// The offsets start and stop are zero-based indexes, with 0 being the first element of the list (the head of the list), 1 being the next element and so on.
// These offsets can also be negative numbers indicating offsets starting at the end of the list.
// For example, -1 is the last element of the list, -2 the penultimate, and so on.
func (db *RoseDB) LRange(key interface{}, start, end int) ([][]byte, error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil, err
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil, err
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	return db.listIndex.indexes.LRange(string(encKey), start, end), nil
}

// LLen returns the length of the list stored at key.
// If key does not exist, it is interpreted as an empty list and 0 is returned.
func (db *RoseDB) LLen(key interface{}) int {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return 0
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return 0
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	return db.listIndex.indexes.LLen(string(encKey))
}

// LKeyExists check if the key of a List exists.
func (db *RoseDB) LKeyExists(key interface{}) (ok bool) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return false
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.checkExpired(encKey, List) {
		return false
	}

	ok = db.listIndex.indexes.LKeyExists(string(encKey))
	return
}

// LValExists check if the val exists in a specified List stored at key.
func (db *RoseDB) LValExists(key interface{}, val interface{}) (ok bool) {
	encKey, encVal, err := db.encode(key, val)
	if err != nil {
		return false
	}
	if err := db.checkKeyValue(encKey, encVal); err != nil {
		return
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.checkExpired(encKey, List) {
		return false
	}

	ok = db.listIndex.indexes.LValExists(string(encKey), encVal)
	return
}

// LClear clear a specified key.
func (db *RoseDB) LClear(key interface{}) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}
	if err = db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	if !db.LKeyExists(encKey) {
		return ErrKeyNotExist
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(encKey, nil, List, ListLClear)
	if err = db.store(e); err != nil {
		return err
	}

	db.listIndex.indexes.LClear(string(encKey))
	delete(db.expires[List], string(encKey))
	return
}

// LExpire set expired time for a specified key of List.
func (db *RoseDB) LExpire(key interface{}, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}
	if !db.LKeyExists(encKey) {
		return ErrKeyNotExist
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(encKey, nil, deadline, List, ListLExpire)
	if err = db.store(e); err != nil {
		return err
	}

	db.expires[List][string(encKey)] = deadline
	return
}

// LTTL return time to live.
func (db *RoseDB) LTTL(key interface{}) (ttl int64) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}
	if db.checkExpired(encKey, List) {
		return
	}

	deadline, exist := db.expires[List][string(encKey)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}

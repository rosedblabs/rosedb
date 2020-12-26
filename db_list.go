package rosedb

import (
	"bytes"
	"log"
	"rosedb/ds/list"
	"rosedb/storage"
	"strconv"
	"strings"
)

//---------列表相关操作接口-----------

func (db *RoseDB) LPush(key []byte, values ...[]byte) error {
	if err := db.checkKeyValue(key, values...); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	for _, val := range values {
		e := storage.NewEntryNoExtra(key, val, List, ListLPush)
		idx, err := db.store(e)
		if err != nil {
			return err
		}

		if err := db.buildIndex(e, idx); err != nil {
			return err
		}
	}

	return nil
}

func (db *RoseDB) RPush(key []byte, values ...[]byte) error {
	if err := db.checkKeyValue(key, values...); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	for _, val := range values {
		e := storage.NewEntryNoExtra(key, val, List, ListRPush)
		idx, err := db.store(e)
		if err != nil {
			return err
		}

		if err := db.buildIndex(e, idx); err != nil {
			return err
		}
	}

	return nil
}

func (db *RoseDB) LPop(key []byte) ([]byte, error) {

	db.mu.Lock()
	defer db.mu.Unlock()

	val := db.listIndex.LPop(string(key))

	e := storage.NewEntryNoExtra(key, val, List, ListLPop)
	if _, err := db.store(e); err != nil {
		log.Println("error occurred when store ListLPop data")
	}

	return val, nil
}

func (db *RoseDB) RPop(key []byte) ([]byte, error) {

	db.mu.Lock()
	defer db.mu.Unlock()

	val := db.listIndex.RPop(string(key))

	e := storage.NewEntryNoExtra(key, val, List, ListRPop)
	if _, err := db.store(e); err != nil {
		log.Println("error occurred when store ListRPop data")
	}

	return val, nil
}

func (db *RoseDB) LIndex(key []byte, idx int) []byte {

	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.listIndex.LIndex(string(key), idx)
}

func (db *RoseDB) LRem(key, value []byte, count int) (int, error) {

	db.mu.Lock()
	defer db.mu.Unlock()

	res := db.listIndex.LRem(string(key), value, count)

	if res > 0 {
		c := strconv.Itoa(count)
		e := storage.NewEntry(key, value, []byte(c), List, ListLRem)
		_, err := db.store(e)
		if err != nil {
			return 0, err
		}
	}

	return res, nil
}

func (db *RoseDB) LInsert(key string, option list.InsertOption, pivot, val []byte) error {

	if err := db.checkKeyValue([]byte(key), val); err != nil {
		return err
	}

	if strings.Contains(string(pivot), ExtraSeparator) {
		return ErrExtraContainsSeparator
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	res := db.listIndex.LInsert(key, option, pivot, val)
	if res != -1 {
		var buf bytes.Buffer
		buf.Write(pivot)
		buf.Write([]byte(ExtraSeparator))
		opt := strconv.Itoa(int(option))
		buf.Write([]byte(opt))

		e := storage.NewEntry([]byte(key), val, buf.Bytes(), List, ListLInsert)
		if _, err := db.store(e); err != nil {
			return err
		}
	}

	return nil
}

func (db *RoseDB) LSet(key []byte, idx int, val []byte) error {

	if err := db.checkKeyValue(key, val); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if res := db.listIndex.LSet(string(key), idx, val); res {
		i := strconv.Itoa(idx)
		e := storage.NewEntry(key, val, []byte(i), List, ListLSet)
		if _, err := db.store(e); err != nil {
			return err
		}
	}

	return nil
}

func (db *RoseDB) LTrim(key []byte, start, end int) error {

	db.mu.Lock()
	defer db.mu.Unlock()

	if res := db.listIndex.LTrim(string(key), start, end); res {
		var buf bytes.Buffer
		buf.Write([]byte(strconv.Itoa(start)))
		buf.Write([]byte(ExtraSeparator))
		buf.Write([]byte(strconv.Itoa(end)))

		e := storage.NewEntry(key, nil, buf.Bytes(), List, ListLTrim)
		if _, err := db.store(e); err != nil {
			return err
		}
	}

	return nil
}

func (db *RoseDB) LRange(key []byte, start, end int) ([][]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if err := db.checkKeyValue(key, nil); err != nil {
		return nil, err
	}

	return db.listIndex.LRange(string(key), start, end), nil
}

func (db *RoseDB) LLen(key []byte) int {

	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.listIndex.LLen(string(key))
}

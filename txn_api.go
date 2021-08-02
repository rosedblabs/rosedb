package rosedb

import (
	"bytes"
	"encoding/binary"
	"github.com/roseduan/rosedb/index"
	"github.com/roseduan/rosedb/storage"
	"github.com/roseduan/rosedb/utils"
	"time"
)

// Set see db_str.go:Set
func (tx *Txn) Set(key, value []byte) (err error) {
	if err = tx.db.checkKeyValue(key, value); err != nil {
		return
	}
	e := storage.NewEntryWithTxn(key, value, nil, String, StringSet, tx.id)
	tx.putEntry(e)
	return
}

// SetNx see db_str.go:SetNx
func (tx *Txn) SetNx(key, value []byte) (res uint32, err error) {
	if err = tx.db.checkKeyValue(key, value); err != nil {
		return
	}
	if _, ok := tx.strEntries[string(key)]; ok {
		return
	}

	exist := tx.db.strIndex.idxList.Exist(key)
	if exist && !tx.db.checkExpired(key, String) {
		return
	}
	if err = tx.Set(key, value); err == nil {
		res = 1
	}
	return
}

// SetEx see db_str.go:SetEx
func (tx *Txn) SetEx(key, value []byte, duration int64) (err error) {
	if err = tx.db.checkKeyValue(key, value); err != nil {
		return
	}
	if duration <= 0 {
		return ErrInvalidTTL
	}

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithTxn(key, value, nil, String, StringExpire, tx.id)
	e.Timestamp = uint64(deadline)
	tx.putEntry(e)
	return
}

// Get see db_str.go:Get
func (tx *Txn) Get(key []byte) (val []byte, err error) {
	if e, ok := tx.strEntries[string(key)]; ok {
		if e.GetMark() == StringExpire && e.Timestamp < uint64(time.Now().Unix()) {
			return
		}
		val = e.Meta.Value
	} else {
		val, err = tx.db.getVal(key)
	}
	return
}

// GetSet see db_str.go:GetSet
func (tx *Txn) GetSet(key, val []byte) (res []byte, err error) {
	res, err = tx.Get(key)
	if err != nil && err != ErrKeyNotExist {
		return
	}
	err = tx.Set(key, val)
	return
}

// Append see db_str.go:Append
func (tx *Txn) Append(key, value []byte) (err error) {
	if e, ok := tx.strEntries[string(key)]; ok {
		e.Meta.Value = append(e.Meta.Value, value...)
		return
	}

	existVal, err := tx.Get(key)
	if err != nil && err != ErrKeyNotExist && err != ErrKeyExpired {
		return err
	}

	if len(existVal) > 0 {
		existVal = append(existVal, value...)
	} else {
		existVal = value
	}
	err = tx.Set(key, existVal)
	return
}

// StrLen see db_str.go:StrLen
func (tx *Txn) StrLen(key []byte) int {
	if e, ok := tx.strEntries[string(key)]; ok {
		return len(e.Meta.Value)
	}

	e := tx.db.strIndex.idxList.Get(key)
	if e != nil {
		if tx.db.checkExpired(key, String) {
			return 0
		}
		idx := e.Value().(*index.Indexer)
		return int(idx.Meta.ValueSize)
	}
	return 0
}

// StrExists see db_str.go:StrExists
func (tx *Txn) StrExists(key []byte) bool {
	if _, ok := tx.strEntries[string(key)]; ok {
		return true
	}

	exist := tx.db.strIndex.idxList.Exist(key)
	if exist && !tx.db.checkExpired(key, String) {
		return true
	}
	return false
}

// StrRem see db_str.go:StrRem
func (tx *Txn) StrRem(key []byte) (err error) {
	if err = tx.db.checkKeyValue(key, nil); err != nil {
		return
	}
	if _, ok := tx.strEntries[string(key)]; ok {
		delete(tx.strEntries, string(key))
		return
	}

	e := storage.NewEntryWithTxn(key, nil, nil, String, StringRem, tx.id)
	tx.putEntry(e)
	return
}

// LPush see db_list.go:LPush
func (tx *Txn) LPush(key []byte, values ...[]byte) (err error) {
	if err = tx.db.checkKeyValue(key, values...); err != nil {
		return
	}
	for _, v := range values {
		e := storage.NewEntryWithTxn(key, v, nil, List, ListLPush, tx.id)
		tx.putEntry(e)
	}
	return
}

// RPush see db_list.go:RPush
func (tx *Txn) RPush(key []byte, values ...[]byte) (err error) {
	if err = tx.db.checkKeyValue(key, values...); err != nil {
		return
	}

	for _, v := range values {
		e := storage.NewEntryWithTxn(key, v, nil, List, ListRPush, tx.id)
		tx.putEntry(e)
	}
	return
}

// HSet see db_hash.go:HSet
func (tx *Txn) HSet(key []byte, field []byte, value []byte) (err error) {
	if err = tx.db.checkKeyValue(key, value); err != nil {
		return
	}
	if bytes.Compare(tx.HGet(key, field), value) == 0 {
		return
	}

	e := storage.NewEntryWithTxn(key, value, field, Hash, HashHSet, tx.id)
	tx.putEntry(e)

	encKey := tx.encodeKey(key, field, Hash)
	tx.keysMap[encKey] = len(tx.writeEntries) - 1
	return
}

// HGet see db_hash.go:HGet
func (tx *Txn) HGet(key, field []byte) (res []byte) {
	encKey := tx.encodeKey(key, field, Hash)
	if idx, ok := tx.keysMap[encKey]; ok {
		entry := tx.writeEntries[idx]
		return entry.Meta.Value
	}

	if tx.db.checkExpired(key, Hash) {
		return
	}
	res = tx.db.hashIndex.indexes.HGet(string(key), string(field))
	return
}

// SAdd see db_set.go:SAdd
func (tx *Txn) SAdd(key []byte, members ...[]byte) (err error) {
	if err = tx.db.checkKeyValue(key, members...); err != nil {
		return
	}
	for _, mem := range members {
		if !tx.SIsMember(key, mem) {
			e := storage.NewEntryWithTxn(key, mem, nil, Set, SetSAdd, tx.id)
			tx.putEntry(e)

			encKey := tx.encodeKey(key, mem, Set)
			tx.keysMap[encKey] = len(tx.keysMap) - 1
		}
	}
	return
}

// SIsMember see db_set.go:SIsMember
func (tx *Txn) SIsMember(key, member []byte) (ok bool) {
	encKey := tx.encodeKey(key, member, Set)
	if idx, exist := tx.keysMap[encKey]; exist {
		entry := tx.writeEntries[idx]
		if bytes.Compare(entry.Meta.Value, member) == 0 {
			ok = true
			return
		}
	}
	if tx.db.checkExpired(key, Set) {
		return false
	}

	ok = tx.db.setIndex.indexes.SIsMember(string(key), member)
	return
}

// ZScore see db_zset.go/ZAdd
func (tx *Txn) ZAdd(key []byte, score float64, member []byte) (err error) {
	oldScore, err := tx.ZScore(key, member)
	if err != nil {
		return err
	}
	if oldScore == score {
		return
	}

	extra := []byte(utils.Float64ToStr(score))
	e := storage.NewEntryWithTxn(key, member, extra, ZSet, ZSetZAdd, tx.id)
	tx.putEntry(e)

	encKey := tx.encodeKey(key, member, ZSet)
	tx.keysMap[encKey] = len(tx.writeEntries) - 1
	return
}

// ZScore see db_zset.go/ZScore
func (tx *Txn) ZScore(key, member []byte) (score float64, err error) {
	encKey := tx.encodeKey(key, member, ZSet)
	if idx, ok := tx.keysMap[encKey]; ok {
		entry := tx.writeEntries[idx]
		score, err = utils.StrToFloat64(string(entry.Meta.Extra))
		if err != nil {
			return
		}
	}
	if tx.db.checkExpired(key, ZSet) {
		return 0, ErrKeyExpired
	}

	score = tx.db.zsetIndex.indexes.ZScore(string(key), string(member))
	return
}

func (tx *Txn) encodeKey(key, extra []byte, dType DataType) string {
	keyLen, extraLen := len(key), len(extra)
	buf := make([]byte, keyLen+extraLen+2)

	binary.BigEndian.PutUint16(buf[:2], dType)
	copy(buf[2:keyLen+2], key)
	if extraLen > 0 {
		copy(buf[keyLen:keyLen+extraLen+2], extra)
	}
	return string(buf)
}

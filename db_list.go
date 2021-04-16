package rosedb

import (
	"bytes"
	"log"
	"rosedb/ds/list"
	"rosedb/storage"
	"strconv"
	"strings"
	"sync"
)

// ListIdx the list idx
type ListIdx struct {
	mu      sync.RWMutex
	indexes *list.List
}

func newListIdx() *ListIdx {
	return &ListIdx{indexes: list.New()}
}

// LPush 在列表的头部添加元素，返回添加后的列表长度
func (db *RoseDB) LPush(key []byte, values ...[]byte) (res int, err error) {
	if err = db.checkKeyValue(key, values...); err != nil {
		return
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	for _, val := range values {
		e := storage.NewEntryNoExtra(key, val, List, ListLPush)
		if err = db.store(e); err != nil {
			return
		}

		res = db.listIndex.indexes.LPush(string(key), val)
	}
	return
}

// RPush 在列表的尾部添加元素，返回添加后的列表长度
func (db *RoseDB) RPush(key []byte, values ...[]byte) (res int, err error) {
	if err = db.checkKeyValue(key, values...); err != nil {
		return
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	for _, val := range values {
		e := storage.NewEntryNoExtra(key, val, List, ListRPush)
		if err = db.store(e); err != nil {
			return
		}

		res = db.listIndex.indexes.RPush(string(key), val)
	}

	return
}

// LPop 取出列表头部的元素
func (db *RoseDB) LPop(key []byte) ([]byte, error) {

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	val := db.listIndex.indexes.LPop(string(key))

	if val != nil {
		e := storage.NewEntryNoExtra(key, val, List, ListLPop)
		if err := db.store(e); err != nil {
			log.Println("error occurred when store ListLPop data")
		}
	}

	return val, nil
}

// RPop 取出列表尾部的元素
func (db *RoseDB) RPop(key []byte) ([]byte, error) {

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	val := db.listIndex.indexes.RPop(string(key))

	if val != nil {
		e := storage.NewEntryNoExtra(key, val, List, ListRPop)
		if err := db.store(e); err != nil {
			log.Println("error occurred when store ListRPop data")
		}
	}

	return val, nil
}

// LIndex 返回列表在index处的值，如果不存在则返回nil
func (db *RoseDB) LIndex(key []byte, idx int) []byte {

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	return db.listIndex.indexes.LIndex(string(key), idx)
}

// LRem 根据参数 count 的值，移除列表中与参数 value 相等的元素
//count > 0 : 从表头开始向表尾搜索，移除与 value 相等的元素，数量为 count
//count < 0 : 从表尾开始向表头搜索，移除与 value 相等的元素，数量为 count 的绝对值
//count = 0 : 移除列表中所有与 value 相等的值
//返回成功删除的元素个数
func (db *RoseDB) LRem(key, value []byte, count int) (int, error) {

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	res := db.listIndex.indexes.LRem(string(key), value, count)

	if res > 0 {
		c := strconv.Itoa(count)
		e := storage.NewEntry(key, value, []byte(c), List, ListLRem)
		if err := db.store(e); err != nil {
			return res, err
		}
	}

	return res, nil
}

// LInsert 将值 val 插入到列表 key 当中，位于值 pivot 之前或之后
//如果命令执行成功，返回插入操作完成之后，列表的长度。 如果没有找到 pivot ，返回 -1
func (db *RoseDB) LInsert(key string, option list.InsertOption, pivot, val []byte) (count int, err error) {

	if err = db.checkKeyValue([]byte(key), val); err != nil {
		return
	}

	if strings.Contains(string(pivot), ExtraSeparator) {
		return 0, ErrExtraContainsSeparator
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	count = db.listIndex.indexes.LInsert(key, option, pivot, val)
	if count != -1 {
		var buf bytes.Buffer
		buf.Write(pivot)
		buf.Write([]byte(ExtraSeparator))
		opt := strconv.Itoa(int(option))
		buf.Write([]byte(opt))

		e := storage.NewEntry([]byte(key), val, buf.Bytes(), List, ListLInsert)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

// LSet 将列表 key 下标为 index 的元素的值设置为 val
//bool返回值表示操作是否成功
func (db *RoseDB) LSet(key []byte, idx int, val []byte) (bool, error) {

	if err := db.checkKeyValue(key, val); err != nil {
		return false, err
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	i := strconv.Itoa(idx)
	e := storage.NewEntry(key, val, []byte(i), List, ListLSet)
	if err := db.store(e); err != nil {
		return false, err
	}

	res := db.listIndex.indexes.LSet(string(key), idx, val)
	return res, nil
}

// LTrim 对一个列表进行修剪(trim)，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除
func (db *RoseDB) LTrim(key []byte, start, end int) error {

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if res := db.listIndex.indexes.LTrim(string(key), start, end); res {
		var buf bytes.Buffer
		buf.Write([]byte(strconv.Itoa(start)))
		buf.Write([]byte(ExtraSeparator))
		buf.Write([]byte(strconv.Itoa(end)))

		e := storage.NewEntry(key, nil, buf.Bytes(), List, ListLTrim)
		if err := db.store(e); err != nil {
			return err
		}
	}

	return nil
}

// LRange 返回列表 key 中指定区间内的元素，区间以偏移量 start 和 end 指定
//如果 start 下标比列表的最大下标(len-1)还要大，那么 LRange 返回一个空列表
//如果 end 下标比 len 还要大，则将 end 的值设置为 len - 1
func (db *RoseDB) LRange(key []byte, start, end int) ([][]byte, error) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if err := db.checkKeyValue(key, nil); err != nil {
		return nil, err
	}

	return db.listIndex.indexes.LRange(string(key), start, end), nil
}

// LLen 返回指定key的列表中的元素个数
func (db *RoseDB) LLen(key []byte) int {

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	return db.listIndex.indexes.LLen(string(key))
}

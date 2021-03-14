package rosedb

import (
	"bytes"
	"log"
	"rosedb/index"
	"rosedb/storage"
	"strings"
	"time"
)

//---------字符串相关操作接口-----------

//将字符串值 value 关联到 key
//如果 key 已经持有其他值，SET 就覆写旧值
func (db *RoseDB) Set(key, value []byte) error {
	if err := db.doSet(key, value); err != nil {
		return err
	}

	//清除过期时间
	db.Persist(key)
	return nil
}

//SetNx 是SET if Not Exists(如果不存在，则 SET)的简写
//只在键 key 不存在的情况下， 将键 key 的值设置为 value
//若键 key 已经存在， 则 SetNx 命令不做任何动作
func (db *RoseDB) SetNx(key, value []byte) error {
	if exist := db.StrExists(key); exist {
		return nil
	}

	return db.Set(key, value)
}

func (db *RoseDB) Get(key []byte) ([]byte, error) {
	keySize := uint32(len(key))
	if keySize == 0 {
		return nil, ErrEmptyKey
	}

	node := db.idxList.Get(key)
	if node == nil {
		return nil, ErrKeyNotExist
	}

	idx := node.Value().(*index.Indexer)
	if idx == nil {
		return nil, ErrNilIndexer
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	//判断是否过期
	if db.expireIfNeeded(key) {
		return nil, ErrKeyExpired
	}
	//如果key和value均在内存中，则取内存中的value
	if db.config.IdxMode == KeyValueRamMode {
		return idx.Meta.Value, nil
	}

	//如果只有key在内存中，那么需要从db file中获取value
	if db.config.IdxMode == KeyOnlyRamMode {
		df := db.activeFile
		if idx.FileId != db.activeFileId {
			df = db.archFiles[idx.FileId]
		}

		if e, err := df.Read(idx.Offset); err != nil {
			return nil, err
		} else {
			return e.Meta.Value, nil
		}
	}
	return nil, ErrKeyNotExist
}

//将键 key 的值设为 value ， 并返回键 key 在被设置之前的旧值。
func (db *RoseDB) GetSet(key, val []byte) (res []byte, err error) {
	if res, err = db.Get(key); err != nil {
		return
	}
	if err = db.Set(key, val); err != nil {
		return
	}
	return
}

//如果key存在，则将value追加至原来的value末尾
//如果key不存在，则相当于Set方法
func (db *RoseDB) Append(key, value []byte) error {
	if err := db.checkKeyValue(key, value); err != nil {
		return err
	}
	e, err := db.Get(key)
	if err != nil && err != ErrKeyNotExist {
		return err
	}
	if db.expireIfNeeded(key) {
		return ErrKeyExpired
	}

	appendExist := false
	if e != nil {
		appendExist = true
		e = append(e, value...)
	} else {
		e = value
	}

	if err := db.doSet(key, e); err != nil {
		return err
	}
	if !appendExist {
		db.Persist(key)
	}
	return nil
}

//返回key存储的字符串值的长度
func (db *RoseDB) StrLen(key []byte) int {
	if err := db.checkKeyValue(key, nil); err != nil {
		return 0
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	e := db.idxList.Get(key)
	if e != nil {
		if db.expireIfNeeded(key) {
			return 0
		}
		idx := e.Value().(*index.Indexer)
		return int(idx.Meta.ValueSize)
	}

	return 0
}

//判断key是否存在
func (db *RoseDB) StrExists(key []byte) bool {
	if err := db.checkKeyValue(key, nil); err != nil {
		return false
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	exist := db.idxList.Exist(key)
	if exist && !db.expireIfNeeded(key) {
		return true
	}
	return false
}

//删除key及其数据
func (db *RoseDB) StrRem(key []byte) error {
	if err := db.checkKeyValue(key, nil); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if ele := db.idxList.Remove(key); ele != nil {
		delete(db.expires, string(key))
		e := storage.NewEntryNoExtra(key, nil, String, StringRem)
		if err := db.store(e); err != nil {
			return err
		}
	}

	return nil
}

//根据前缀查找所有匹配的 key 对应的 value
//参数 limit 和 offset 控制取数据的范围，类似关系型数据库中的分页操作
//如果 limit 为负数，则返回所有满足条件的结果
func (db *RoseDB) PrefixScan(prefix string, limit, offset int) (val [][]byte, err error) {
	if limit == 0 {
		return
	}
	if offset < 0 {
		offset = 0
	}
	if err = db.checkKeyValue([]byte(prefix), nil); err != nil {
		return
	}

	db.mu.RLock()
	defer db.mu.RUnlock()
	e := db.idxList.FindPrefix([]byte(prefix))
	if limit > 0 {
		for i := 0; i < offset && e != nil && strings.HasPrefix(string(e.Key()), prefix); i++ {
			e = e.Next()
		}
	}
	for e != nil && strings.HasPrefix(string(e.Key()), prefix) && limit != 0 {
		item := e.Value().(*index.Indexer)
		var value []byte

		if db.config.IdxMode == KeyOnlyRamMode {
			value, err = db.Get(e.Key())
			if err != nil {
				return
			}
		} else {
			if item != nil {
				value = item.Meta.Value
			}
		}

		expired := db.expireIfNeeded(e.Key())
		if !expired {
			val = append(val, value)
			e = e.Next()
		}
		if limit > 0 && !expired {
			limit--
		}
	}
	return
}

//范围扫描，查找 key 从 start 到 end 之间的数据
func (db *RoseDB) RangeScan(start, end []byte) (val [][]byte, err error) {
	node := db.idxList.Get(start)
	if node == nil {
		return nil, ErrKeyNotExist
	}

	db.mu.RLock()
	defer db.mu.RUnlock()
	for bytes.Compare(node.Key(), end) <= 0 {
		if db.expireIfNeeded(node.Key()) {
			node = node.Next()
			continue
		}

		var value []byte
		if db.config.IdxMode == KeyOnlyRamMode {
			value, err = db.Get(node.Key())
			if err != nil {
				return nil, err
			}
		} else {
			value = node.Value().(*index.Indexer).Meta.Value
		}

		val = append(val, value)
		node = node.Next()
	}

	return
}

//设置key的过期时间
func (db *RoseDB) Expire(key []byte, seconds uint32) (err error) {
	if exist := db.StrExists(key); !exist {
		return ErrKeyNotExist
	}
	if seconds <= 0 {
		return ErrInvalidTtl
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	deadline := uint32(time.Now().Unix()) + seconds
	db.expires[string(key)] = deadline
	return
}

//清除key的过期时间
func (db *RoseDB) Persist(key []byte) {
	db.mu.Lock()
	defer db.mu.Unlock()

	delete(db.expires, string(key))
}

//获取key的过期时间
func (db *RoseDB) TTL(key []byte) (ttl uint32) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.expireIfNeeded(key) {
		return
	}
	deadline, exist := db.expires[string(key)]
	if !exist {
		return
	}

	now := uint32(time.Now().Unix())
	if deadline > now {
		ttl = deadline - now
	}
	return
}

//检查key是否过期并删除相应的值
func (db *RoseDB) expireIfNeeded(key []byte) (expired bool) {
	deadline := db.expires[string(key)]
	if deadline <= 0 {
		return
	}

	if time.Now().Unix() > int64(deadline) {
		expired = true
		//删除过期字典对应的key
		delete(db.expires, string(key))

		//删除索引及数据
		if ele := db.idxList.Remove(key); ele != nil {
			e := storage.NewEntryNoExtra(key, nil, String, StringRem)
			if err := db.store(e); err != nil {
				log.Printf("remove expired key err [%+v] [%+v]\n", key, err)
			}
		}
	}
	return
}

func (db *RoseDB) doSet(key, value []byte) (err error) {
	if err = db.checkKeyValue(key, value); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	e := storage.NewEntryNoExtra(key, value, String, StringSet)
	if err := db.store(e); err != nil {
		return err
	}

	//数据索引
	idx := &index.Indexer{
		Meta: &storage.Meta{
			KeySize: uint32(len(e.Meta.Key)),
			Key:     e.Meta.Key,
		},
		FileId:    db.activeFileId,
		EntrySize: e.Size(),
		Offset:    db.activeFile.Offset - int64(e.Size()),
	}

	if err = db.buildIndex(e, idx); err != nil {
		return err
	}
	return
}

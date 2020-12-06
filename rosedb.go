package rosedb

import (
	"errors"
	"os"
	"rosedb/ds/skiplist"
	"rosedb/index"
	"rosedb/storage"
	"rosedb/utils"
	"sync"
)

var (
	ErrEmptyKey = errors.New("rosedb: the key is empty")

	ErrKeyTooLarge = errors.New("rosedb: key exceeded the max length")

	ErrValueTooLarge = errors.New("rosedb: value exceeded the max length")

	ErrKeyNotExist = errors.New("rosedb: key not exist")

	ErrNilIndexer = errors.New("rosedb: indexer is nil")
)

type RoseDB struct {
	config       *Config
	activeFile   *storage.DBFile
	activeFileId uint8
	idxList      *skiplist.SkipList
	mu           sync.RWMutex
}

//打开一个数据库实例
func Open(config *Config) (*RoseDB, error) {
	if config == nil {
		config = DefaultConfig()
	}

	//如果目录不存在则创建
	if !utils.Exist(config.dirPath) {
		if err := os.MkdirAll(config.dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	return &RoseDB{
		config:       config,
		activeFileId: 0,
		idxList:      skiplist.New(),
	}, nil
}

func (db *RoseDB) Add(key, value []byte) error {
	keySize := uint32(len(key))
	if keySize == 0 {
		return ErrEmptyKey
	}

	config := db.config
	if keySize > config.MaxKeySize {
		return ErrKeyTooLarge
	}

	valueSize := uint32(len(value))
	if valueSize > config.MaxValueSize {
		return ErrValueTooLarge
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	//初始化数据文件
	if db.activeFile == nil {
		if dbFile, err := storage.NewDBFile(config.dirPath, db.activeFileId, config.rwMethod, config.blockSize); err != nil {
			return err
		} else {
			db.activeFile = dbFile
		}
	}

	e := storage.NewEntry(key, value)
	//如果数据文件空间不够，则关闭该文件，并新打开一个文件
	if db.activeFile.Offset+int64(e.Size()) > config.blockSize {
		if err := db.activeFile.Close(true); err != nil {
			return err
		}

		activeFileId := db.activeFileId + 1
		if dbFile, err := storage.NewDBFile(config.dirPath, activeFileId, config.rwMethod, config.blockSize); err != nil {
			return err
		} else {
			db.activeFile = dbFile
			db.activeFileId = activeFileId
		}
	}

	//写入数据至文件中
	if err := db.activeFile.Write(e); err != nil {
		return err
	}

	//数据持久化
	if config.Sync {
		if err := db.activeFile.Sync(); err != nil {
			return err
		}
	}

	//存储索引至内存中
	item := &index.Indexer{
		Key:    key,
		Size:   e.Size(),
		Offset: db.activeFile.Offset,
		FileId: db.activeFileId,
	}

	if config.idxMode == KeyValueRamMode {
		item.Value = value
	}

	db.idxList.Add(item)
	return nil
}

func (db *RoseDB) Get(key []byte) ([]byte, error) {
	keySize := uint32(len(key))
	if keySize == 0 {
		return nil, ErrEmptyKey
	}

	node := db.idxList.Find(key)
	if node == nil {
		return nil, ErrKeyNotExist
	}

	item := node.Value.(*index.Indexer)
	if item == nil {
		return nil, ErrNilIndexer
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	//如果key和value均在内存中，则取内存中的value
	if db.config.idxMode == KeyValueRamMode {
		return item.Value, nil
	}

	//如果只有key在内存中，那么需要从db file中获取value
	if db.config.idxMode == KeyOnlyRamMode {
		if e, err := db.activeFile.Read(item.Offset, int64(item.Size)); err != nil {
			return nil, err
		} else {
			return e.Value, nil
		}
	}

	return nil, ErrKeyNotExist
}

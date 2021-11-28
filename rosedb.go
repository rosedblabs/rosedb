package rosedb

import (
	"encoding/json"
	"errors"
	"github.com/roseduan/rosedb/cache"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/roseduan/rosedb/index"
	"github.com/roseduan/rosedb/storage"
	"github.com/roseduan/rosedb/utils"
)

var (
	// ErrEmptyKey the key is empty
	ErrEmptyKey = errors.New("rosedb: the key is empty")

	// ErrKeyNotExist key not exist
	ErrKeyNotExist = errors.New("rosedb: key not exist")

	// ErrKeyTooLarge the key too large
	ErrKeyTooLarge = errors.New("rosedb: key exceeded the max length")

	// ErrValueTooLarge the value too large
	ErrValueTooLarge = errors.New("rosedb: value exceeded the max length")

	// ErrNilIndexer the indexer is nil
	ErrNilIndexer = errors.New("rosedb: indexer is nil")

	// ErrMergeUnreached not ready to reclaim
	ErrMergeUnreached = errors.New("rosedb: unused space not reach the threshold")

	// ErrExtraContainsSeparator extra contains separator
	ErrExtraContainsSeparator = errors.New("rosedb: extra contains separator \\0")

	// ErrInvalidTTL ttl is invalid
	ErrInvalidTTL = errors.New("rosedb: invalid ttl")

	// ErrKeyExpired the key is expired
	ErrKeyExpired = errors.New("rosedb: key is expired")

	// ErrDBisMerging merge and single merge can`t execute at the same time.
	ErrDBisMerging = errors.New("rosedb: can`t do reclaim and single reclaim at the same time")

	// ErrDBIsClosed db can`t be used after closed.
	ErrDBIsClosed = errors.New("rosedb: db is closed, reopen it")

	// ErrTxIsFinished tx is finished.
	ErrTxIsFinished = errors.New("rosedb: transaction is finished, create a new one")

	// ErrActiveFileIsNil active file is nil.
	ErrActiveFileIsNil = errors.New("rosedb: active file is nil")

	// ErrWrongNumberOfArgs wrong number of arguments
	ErrWrongNumberOfArgs = errors.New("rosedb: wrong number of arguments")
)

const (

	// The path for saving rosedb config file.
	configSaveFile = string(os.PathSeparator) + "DB.CFG"

	// The path for saving rosedb transaction meta info.
	dbTxMetaSaveFile = string(os.PathSeparator) + "DB.TX.META"

	// rosedb reclaim path, a temporary dir, will be removed after reclaim.
	mergePath = string(os.PathSeparator) + "rosedb_merge"

	// Separator of the extra info, some commands can`t contains it.
	ExtraSeparator = "\\0"

	// DataStructureNum the num of different data structures, there are five now(string, list, hash, set, zset).
	DataStructureNum = 5
)

type (
	// RoseDB the rosedb struct, represents a db instance.
	RoseDB struct {
		// Current active files of different data types, stored like this: map[DataType]*storage.DBFile.
		activeFile *sync.Map
		archFiles  ArchivedFiles // The archived files.
		strIndex   *StrIdx       // String indexes(a skip list).
		listIndex  *ListIdx      // List indexes.
		hashIndex  *HashIdx      // Hash indexes.
		setIndex   *SetIdx       // Set indexes.
		zsetIndex  *ZsetIdx      // Sorted set indexes.
		config     Config        // Config info of rosedb.
		mu         sync.RWMutex  // mutex.
		expires    Expires       // Expired directory.
		lockMgr    *LockMgr      // lockMgr controls isolation of read and write.
		closed     uint32
		closeSig   *closeSignal
		cache      *cache.LruCache // lru cache for db_str.
		txnMgr     *TxnManager
		txnCh      chan *writeBuffer
	}

	// ArchivedFiles define the archived files, which mean these files can only be read.
	// and will never be opened for writing.
	ArchivedFiles map[DataType]map[uint32]*storage.DBFile

	// Expires saves the expire info of different keys.
	Expires map[DataType]map[string]int64

	closeSignal struct {
		chn chan struct{}
	}
)

// Open a rosedb instance. You must call Close after using it.
func Open(config Config) (*RoseDB, error) {
	// create the dir path if not exists.
	if !utils.Exist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// load the db files from disk.
	archFiles, activeFileIds, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	// set active files for writing.
	activeFiles := new(sync.Map)
	for dataType, fileId := range activeFileIds {
		file, err := storage.NewDBFile(config.DirPath, fileId, config.RwMethod, config.BlockSize, dataType)
		if err != nil {
			return nil, err
		}
		activeFiles.Store(dataType, file)
	}

	db := &RoseDB{
		activeFile: activeFiles,
		archFiles:  archFiles,
		config:     config,
		strIndex:   newStrIdx(),
		listIndex:  newListIdx(),
		hashIndex:  newHashIdx(),
		setIndex:   newSetIdx(),
		zsetIndex:  newZsetIdx(),
		expires:    make(Expires),
		cache:      cache.NewLruCache(config.CacheCapacity),
	}
	for i := 0; i < DataStructureNum; i++ {
		db.expires[uint16(i)] = make(map[string]int64)
	}
	db.lockMgr = newLockMgr(db)

	// load indexes from db files.
	if err := db.loadIdxFromFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Close db and save relative configs.
func (db *RoseDB) Close() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err = db.saveConfig(); err != nil {
		return err
	}

	// close and sync the active file.
	db.activeFile.Range(func(key, value interface{}) bool {
		if dbFile, ok := value.(*storage.DBFile); ok {
			if err = dbFile.Close(true); err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return
	}

	// close the archived files.
	for _, archFile := range db.archFiles {
		for _, file := range archFile {
			if err = file.Sync(); err != nil {
				return err
			}
		}
	}

	atomic.StoreUint32(&db.closed, 1)
	if db.closeSig.chn != nil {
		close(db.closeSig.chn)
	}
	return
}

func (db *RoseDB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) == 1
}

// Persist the db files.
func (db *RoseDB) Sync() (err error) {
	if db == nil || db.activeFile == nil {
		return nil
	}

	db.activeFile.Range(func(key, value interface{}) bool {
		if dbFile, ok := value.(*storage.DBFile); ok {
			if err = dbFile.Sync(); err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return
	}
	return
}

// Backup copy the database directory for backup.
func (db *RoseDB) Backup(dir string) (err error) {
	if utils.Exist(db.config.DirPath) {
		err = utils.CopyDir(db.config.DirPath, dir)
	}
	return
}

func (db *RoseDB) checkKeyValue(key []byte, value ...[]byte) error {
	keySize := uint32(len(key))
	if keySize == 0 {
		return ErrEmptyKey
	}

	config := db.config
	if keySize > config.MaxKeySize {
		return ErrKeyTooLarge
	}

	for _, v := range value {
		if uint32(len(v)) > config.MaxValueSize {
			return ErrValueTooLarge
		}
	}

	return nil
}

// save config before closing db.
func (db *RoseDB) saveConfig() (err error) {
	path := db.config.DirPath + configSaveFile
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)

	b, err := json.Marshal(db.config)
	_, err = file.Write(b)
	err = file.Close()

	return
}

// build the indexes for different data structures.
func (db *RoseDB) buildIndex(entry *storage.Entry, idx *index.Indexer) (err error) {
	if db.config.IdxMode == KeyValueMemMode && entry.GetType() == String {
		idx.Meta.Value = entry.Meta.Value
		idx.Meta.ValueSize = uint32(len(entry.Meta.Value))
	}

	switch entry.GetType() {
	case storage.String:
		db.buildStringIndex(idx, entry)
	case storage.List:
		db.buildListIndex(entry)
	case storage.Hash:
		db.buildHashIndex(entry)
	case storage.Set:
		db.buildSetIndex(entry)
	case storage.ZSet:
		db.buildZsetIndex(entry)
	}
	return
}

// write entry to db file.
func (db *RoseDB) store(e *storage.Entry) error {
	// sync the db file if file size is not enough, and open a new db file.
	config := db.config
	activeFile, err := db.getActiveFile(e.GetType())
	if err != nil {
		return err
	}

	if activeFile.Offset+int64(e.Size()) > config.BlockSize {
		if err := activeFile.Sync(); err != nil {
			return err
		}

		// save the old db file as arched file.
		activeFileId := activeFile.Id
		db.archFiles[e.GetType()][activeFileId] = activeFile

		newDbFile, err := storage.NewDBFile(config.DirPath, activeFileId+1, config.RwMethod, config.BlockSize, e.GetType())
		if err != nil {
			return err
		}
		activeFile = newDbFile
	}

	// write entry to db file.
	if err := activeFile.Write(e); err != nil {
		return err
	}
	db.activeFile.Store(e.GetType(), activeFile)

	// persist db file according to the config.
	if config.Sync {
		if err := activeFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Check whether key is expired and delete it if needed.
func (db *RoseDB) checkExpired(key []byte, dType DataType) (expired bool) {
	deadline, exist := db.expires[dType][string(key)]
	if !exist {
		return
	}

	if time.Now().Unix() > deadline {
		expired = true

		var e *storage.Entry
		switch dType {
		case String:
			e = storage.NewEntryNoExtra(key, nil, String, StringRem)
			db.strIndex.idxList.Remove(key)
		case List:
			e = storage.NewEntryNoExtra(key, nil, List, ListLClear)
			db.listIndex.indexes.LClear(string(key))
		case Hash:
			e = storage.NewEntryNoExtra(key, nil, Hash, HashHClear)
			db.hashIndex.indexes.HClear(string(key))
		case Set:
			e = storage.NewEntryNoExtra(key, nil, Set, SetSClear)
			db.setIndex.indexes.SClear(string(key))
		case ZSet:
			e = storage.NewEntryNoExtra(key, nil, ZSet, ZSetZClear)
			db.zsetIndex.indexes.ZClear(string(key))
		}
		if err := db.store(e); err != nil {
			log.Println("checkExpired: store entry err: ", err)
			return
		}
		// delete the expire info stored at key.
		delete(db.expires[dType], string(key))
	}
	return
}

func (db *RoseDB) getActiveFile(dType DataType) (file *storage.DBFile, err error) {
	value, ok := db.activeFile.Load(dType)
	if !ok || value == nil {
		return nil, ErrActiveFileIsNil
	}

	var typeOk bool
	if file, typeOk = value.(*storage.DBFile); !typeOk {
		return nil, ErrActiveFileIsNil
	}
	return
}

func (db *RoseDB) encode(key, value interface{}) (encKey, encVal []byte, err error) {
	if encKey, err = utils.EncodeKey(key); err != nil {
		return
	}
	if encVal, err = utils.EncodeValue(value); err != nil {
		return
	}
	return
}

func (db *RoseDB) sendTxnChn(buf *writeBuffer) {
	if buf == nil || len(buf.entries) == 0 {
		return
	}
	db.txnCh <- buf
}

func (db *RoseDB) writeTxnBuffer() {
	for {
		select {
		case <-db.txnCh:
			// todo
		default:
		}
	}
}

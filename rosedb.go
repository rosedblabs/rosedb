package rosedb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/roseduan/rosedb/cache"
	"io"
	"log"
	"os"
	"sort"
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
		archFiles  storage.ArchivedFiles // The archived files.
		strIndex   *StrIdx               // String indexes(a skip list).
		listIndex  *ListIdx              // List indexes.
		hashIndex  *HashIdx              // Hash indexes.
		setIndex   *SetIdx               // Set indexes.
		zsetIndex  *ZsetIdx              // Sorted set indexes.
		config     Config                // Config info of rosedb.
		mu         sync.RWMutex          // mutex.
		expires    Expires               // Expired directory.
		lockMgr    *LockMgr              // lockMgr controls isolation of read and write.
		txnMeta    *TxnMeta              // Txn meta info used in transaction.
		closed     uint32
		cache      *cache.LruCache // lru cache for db_str.
		isMerging  bool
	}

	// Expires saves the expire info of different keys.
	Expires map[DataType]map[string]int64
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

	// load txn meta info for transaction.
	txnMeta, err := LoadTxnMeta(config.DirPath + dbTxMetaSaveFile)
	if err != nil {
		return nil, err
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
		txnMeta:    txnMeta,
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

// Merge will reorganize the db file`s data in disk, removes the useless data in disk.
// For List, Hash, Set and ZSet, we dump the data in memory directly to db file, so it will block read and write for a while.
// For String, we choose a different way to do the same thing: load all data in db files in order, and compare the newest value in memory, find the valid data, rewrite them to new db files.
func (db *RoseDB) Merge() (err error) {
	if db.isMerging {
		return ErrDBisMerging
	}

	// create a temporary directory for storing the new db files.
	mergePath := db.config.DirPath + mergePath
	if !utils.Exist(mergePath) {
		if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
			return err
		}
	}
	defer os.RemoveAll(mergePath)

	db.mu.Lock()
	defer func() {
		db.isMerging = false
		db.mu.Unlock()
	}()
	db.isMerging = true

	wg := new(sync.WaitGroup)
	wg.Add(DataStructureNum)
	go db.dump(wg)
	go db.mergeString(wg)
	wg.Wait()
	return
}

func (db *RoseDB) dump(wg *sync.WaitGroup) {
	path := db.config.DirPath + mergePath
	for i := List; i < DataStructureNum; i++ {
		switch i {
		case List:
			go db.dumpInternal(wg, path, List)
		case Hash:
			go db.dumpInternal(wg, path, Hash)
		case Set:
			go db.dumpInternal(wg, path, Set)
		case ZSet:
			go db.dumpInternal(wg, path, ZSet)
		}
	}
	return
}

func (db *RoseDB) dumpInternal(wg *sync.WaitGroup, path string, eType DataType) {
	defer wg.Done()

	cfg := db.config
	if len(db.archFiles[eType])+1 < cfg.MergeThreshold {
		return
	}

	unLockFunc := db.lockMgr.Lock(eType)
	defer unLockFunc()

	var mergeFiles []*storage.DBFile
	// create and store the first db file.
	file, err := storage.NewDBFile(path, 0, cfg.RwMethod, cfg.BlockSize, eType)
	if err != nil {
		log.Printf("[dumpInternal]create new db file err.[%+v]\n", err)
		return
	}
	mergeFiles = append(mergeFiles, file)

	dumpStoreFn := func(e *storage.Entry) (err error) {
		if err = db.dumpStore(&mergeFiles, path, e); err != nil {
			log.Printf("[dumpInternal]store entry err.[%+v]\n", err)
		}
		return
	}

	// dump all values and delete original db files if there are no errors.
	var dumpErr error
	switch eType {
	case List:
		dumpErr = db.listIndex.indexes.DumpIterate(dumpStoreFn)
	case Hash:
		dumpErr = db.hashIndex.indexes.DumpIterate(dumpStoreFn)
	case Set:
		dumpErr = db.setIndex.indexes.DumpIterate(dumpStoreFn)
	case ZSet:
		dumpErr = db.zsetIndex.indexes.DumpIterate(dumpStoreFn)
	}
	if dumpErr != nil {
		return
	}

	for _, f := range db.archFiles[eType] {
		f.Close(false)
		os.Remove(f.File.Name())
	}

	value, ok := db.activeFile.Load(eType)
	if ok && value != nil {
		activeFile, _ := value.(*storage.DBFile)

		if activeFile != nil {
			activeFile.Close(true)
			os.Remove(activeFile.File.Name())
		}
	}

	// copy the temporary files as new db files.
	for _, f := range mergeFiles {
		name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[eType], f.Id)
		os.Rename(path+name, cfg.DirPath+name)
	}

	// reload db files.
	if err = db.loadDBFiles(eType); err != nil {
		log.Printf("[dumpInternal]load db files err.[%+v]\n", err)
		return
	}
}

func (db *RoseDB) dumpStore(mergeFiles *[]*storage.DBFile, mergePath string, e *storage.Entry) (err error) {
	var df *storage.DBFile
	df = (*mergeFiles)[len(*mergeFiles)-1]
	cfg := db.config

	if df.Offset+int64(e.Size()) > cfg.BlockSize {
		if err = df.Sync(); err != nil {
			return
		}
		df, err = storage.NewDBFile(mergePath, df.Id+1, cfg.RwMethod, cfg.BlockSize, e.GetType())
		if err != nil {
			return
		}
		*mergeFiles = append(*mergeFiles, df)
	}

	if err = df.Write(e); err != nil {
		return
	}
	return
}

func (db *RoseDB) mergeString(wg *sync.WaitGroup) {
	defer wg.Done()

	if len(db.archFiles[0]) < db.config.MergeThreshold {
		return
	}

	cfg := db.config
	path := db.config.DirPath + mergePath
	mergedFiles, _, err := storage.BuildType(path, cfg.RwMethod, cfg.BlockSize, String)
	if err != nil {
		log.Printf("[mergeString]build db file err.[%+v]", err)
		return
	}

	archFiles := mergedFiles[String]
	if archFiles == nil {
		archFiles = make(map[uint32]*storage.DBFile)
	}

	var (
		df        *storage.DBFile
		maxFileId uint32
		fileIds   []int
	)
	for fid := range archFiles {
		if fid > maxFileId {
			maxFileId = fid
		}
	}

	// skip the merged files.
	for fid := range db.archFiles[String] {
		if _, exist := archFiles[fid]; !exist {
			fileIds = append(fileIds, int(fid))
		}
	}

	// must merge db files in order.
	sort.Ints(fileIds)
	for _, fid := range fileIds {
		dbFile := db.archFiles[String][uint32(fid)]
		validEntries, err := dbFile.FindValidEntries(db.validEntry)
		if err != nil && err != io.EOF {
			log.Printf(fmt.Sprintf("find valid entries err.[%+v]", err))
			return
		}

		// rewrite valid entries.
		for _, ent := range validEntries {
			if df == nil || int64(ent.Size())+df.Offset > cfg.BlockSize {
				df, err = storage.NewDBFile(path, maxFileId, cfg.RwMethod, cfg.BlockSize, String)
				if err != nil {
					log.Printf(fmt.Sprintf("create db file err.[%+v]", err))
					return
				}
				db.archFiles[String][maxFileId] = df
				archFiles[maxFileId] = df
				maxFileId += 1
			}

			if err = df.Write(ent); err != nil {
				log.Printf(fmt.Sprintf("rewrite entry err.[%+v]", err))
				return
			}

			// update index.
			item := db.strIndex.idxList.Get(ent.Meta.Key)
			if item != nil {
				idx, _ := item.Value().(*index.Indexer)
				if idx != nil {
					idx.Offset = df.Offset - int64(ent.Size())
					idx.FileId = df.Id
					db.strIndex.idxList.Put(idx.Meta.Key, idx)
				}
			}
		}
		// delete older db file.
		_ = dbFile.Close(false)
		_ = os.Remove(dbFile.File.Name())
	}

	for _, file := range archFiles {
		name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[String], file.Id)
		os.Rename(path+name, cfg.DirPath+name)
	}

	// reload db files.
	if err = db.loadDBFiles(String); err != nil {
		log.Printf("load db files err.[%+v]", err)
		return
	}
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
func (db *RoseDB) buildIndex(entry *storage.Entry, idx *index.Indexer, isOpen bool) (err error) {
	if db.config.IdxMode == KeyValueMemMode && entry.GetType() == String {
		idx.Meta.Value = entry.Meta.Value
		idx.Meta.ValueSize = uint32(len(entry.Meta.Value))
	}

	// uncommitted entry is invalid.
	if entry.TxId != 0 && isOpen {
		if entry.TxId > db.txnMeta.MaxTxId {
			db.txnMeta.MaxTxId = entry.TxId
		}
		if _, ok := db.txnMeta.CommittedTxIds[entry.TxId]; !ok {
			return
		}
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

func (db *RoseDB) validEntry(e *storage.Entry, offset int64, fileId uint32) bool {
	if e == nil {
		return false
	}

	deadline, exist := db.expires[String][string(e.Meta.Key)]
	now := time.Now().Unix()
	if exist && deadline > now {
		return true
	}

	if e.GetMark() == StringSet || e.GetMark() == StringPersist {
		node := db.strIndex.idxList.Get(e.Meta.Key)
		if node == nil {
			return false
		}
		indexer, _ := node.Value().(*index.Indexer)
		if indexer != nil && bytes.Compare(indexer.Meta.Key, e.Meta.Key) == 0 {
			if indexer.FileId == fileId && indexer.Offset == offset {
				return true
			}
		}
	}
	return false
}

func (db *RoseDB) loadDBFiles(eType DataType) error {
	cfg := db.config
	archivedFiles, activeIds, err := storage.BuildType(cfg.DirPath, cfg.RwMethod, cfg.BlockSize, eType)
	if err != nil {
		return err
	}
	db.archFiles[eType] = archivedFiles[eType]

	activeFile, err := storage.NewDBFile(cfg.DirPath, activeIds[eType], cfg.RwMethod, cfg.BlockSize, eType)
	if err != nil {
		return err
	}
	db.activeFile.Store(eType, activeFile)
	return nil
}

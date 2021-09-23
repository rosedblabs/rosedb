package rosedb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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

	// ErrCfgNotExist the config is not exist
	ErrCfgNotExist = errors.New("rosedb: the config file not exist")

	// ErrReclaimUnreached not ready to reclaim
	ErrReclaimUnreached = errors.New("rosedb: unused space not reach the threshold")

	// ErrExtraContainsSeparator extra contains separator
	ErrExtraContainsSeparator = errors.New("rosedb: extra contains separator \\0")

	// ErrInvalidTTL ttl is invalid
	ErrInvalidTTL = errors.New("rosedb: invalid ttl")

	// ErrKeyExpired the key is expired
	ErrKeyExpired = errors.New("rosedb: key is expired")

	// ErrDBisReclaiming reclaim and single reclaim can`t execute at the same time.
	ErrDBisReclaiming = errors.New("rosedb: can`t do reclaim and single reclaim at the same time")

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
		activeFile         *sync.Map
		archFiles          ArchivedFiles // The archived files.
		strIndex           *StrIdx       // String indexes(a skip list).
		listIndex          *ListIdx      // List indexes.
		hashIndex          *HashIdx      // Hash indexes.
		setIndex           *SetIdx       // Set indexes.
		zsetIndex          *ZsetIdx      // Sorted set indexes.
		config             Config        // Config info of rosedb.
		mu                 sync.RWMutex  // mutex.
		expires            Expires       // Expired directory.
		isReclaiming       bool          // Indicates whether the db is reclaiming, see StartMerge.
		isSingleReclaiming bool          // Indicates whether the db is in single reclaiming, see SingleMerge.
		lockMgr            *LockMgr      // lockMgr controls isolation of read and write.
		txnMeta            *TxnMeta      // Txn meta info used in transaction.
		closed             uint32
		mergeChn           chan struct{} // mergeChn used for sending stop signal to merge func.
	}

	// ArchivedFiles define the archived files, which mean these files can only be read.
	// and will never be opened for writing.
	ArchivedFiles map[DataType]map[uint32]*storage.DBFile

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
	}
	for i := 0; i < DataStructureNum; i++ {
		db.expires[uint16(i)] = make(map[string]int64)
	}
	db.lockMgr = newLockMgr(db)

	// load indexes from db files.
	if err := db.loadIdxFromFiles(); err != nil {
		return nil, err
	}

	// handle db merge.
	go func() {
		timer := time.NewTimer(config.MergeCheckInterval)
		defer timer.Stop()

		for {
			select {
			case <-timer.C:
				timer.Reset(config.MergeCheckInterval)
				err := db.StartMerge()
				if err != nil && err != ErrDBisReclaiming && err != ErrReclaimUnreached {
					log.Println("rosedb: merge err: ", err)
					return
				}
			default:
			}
		}
	}()

	return db, nil
}

// Reopen the db according to the specific config path.
func Reopen(path string) (*RoseDB, error) {
	if exist := utils.Exist(path + configSaveFile); !exist {
		return nil, ErrCfgNotExist
	}

	var config Config

	b, err := ioutil.ReadFile(path + configSaveFile)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &config); err != nil {
		return nil, err
	}
	return Open(config)
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

// StartMerge reclaim db files`s redundant space in disk.
// StartMerge operation will read all archived files, iterate all entries and find the valid.
// Then rewrite the valid entries to new db files.
// So the time required for reclaim operation depend on the number of entries, you`d better execute it in low peak period.
func (db *RoseDB) StartMerge() (err error) {
	// if single reclaiming is in progress, the reclaim operation can`t be executed.
	if db.isSingleReclaiming || db.isReclaiming {
		return ErrDBisReclaiming
	}
	var mergeTypes int
	for _, archFiles := range db.archFiles {
		if len(archFiles) >= db.config.MergeThreshold {
			mergeTypes++
		}
	}
	if mergeTypes == 0 {
		return ErrReclaimUnreached
	}

	// create a temporary directory for storing the new db files.
	mergePath := db.config.DirPath + mergePath
	if !utils.Exist(mergePath) {
		if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
			return err
		}
	}

	db.mu.Lock()
	defer func() {
		db.isReclaiming = false
		db.mu.Unlock()
	}()
	db.isReclaiming = true

	// processing the different types of files in different goroutines.
	newArchivedFiles := sync.Map{}
	reclaimedTypes := sync.Map{}

	mergedFiles, _, err := storage.Build(mergePath, db.config.RwMethod, db.config.BlockSize)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	wg.Add(DataStructureNum)
	for i := 0; i < DataStructureNum; i++ {
		go func(dType uint16) {
			defer wg.Done()

			if len(db.archFiles[dType]) < db.config.MergeThreshold {
				newArchivedFiles.Store(dType, db.archFiles[dType])
				return
			}

			var (
				df      *storage.DBFile
				fileId  uint32
				fileIds []int
			)
			archFiles := mergedFiles[dType]
			if archFiles == nil {
				archFiles = make(map[uint32]*storage.DBFile)
			}

			// find the greatest file id in merged files.
			for id := range archFiles {
				if id > fileId {
					fileId = id
				}
			}

			for _, file := range db.archFiles[dType] {
				// skip the merged files.
				if _, exist := archFiles[file.Id]; !exist {
					fileIds = append(fileIds, int(file.Id))
				}
			}
			sort.Ints(fileIds)

			for _, fid := range fileIds {
				select {
				case <-db.mergeChn:
					log.Printf("receive a stop signal, merge stop, data type:%d\n", dType)
					return
				default:
					file := db.archFiles[dType][uint32(fid)]
					var offset int64 = 0
					var reclaimEntries []*storage.Entry

					// read all entries in db file, and find the valid entry.
					for {
						if e, err := file.Read(offset); err == nil {
							if db.validEntry(e, offset, file.Id) {
								reclaimEntries = append(reclaimEntries, e)
							}
							offset += int64(e.Size())
						} else {
							if err == io.EOF {
								break
							}
							log.Fatalf("err occurred when read the entry: %+v", err)
							return
						}
					}

					// rewrite the valid entries to new db file.
					for _, entry := range reclaimEntries {
						if df == nil || int64(entry.Size())+df.Offset > db.config.BlockSize {
							df, err = storage.NewDBFile(mergePath, fileId, db.config.RwMethod, db.config.BlockSize, dType)
							if err != nil {
								log.Fatalf("err occurred when create new db file: %+v", err)
								return
							}
							archFiles[fileId] = df
							// todo: atomic update
							db.archFiles[dType][fileId] = df
							fileId += 1
						}

						if err = df.Write(entry); err != nil {
							log.Fatalf("err occurred when write the entry: %+v", err)
							return
						}

						// Since the str types value will be read from db file, so should update the index info.
						// todo: atomic update
						if dType == String {
							item := db.strIndex.idxList.Get(entry.Meta.Key)
							idx := item.Value().(*index.Indexer)
							idx.Offset = df.Offset - int64(entry.Size())
							idx.FileId = df.Id
							db.strIndex.idxList.Put(idx.Meta.Key, idx)
						}
					}

					// delete the original db file.
					if err = file.Close(false); err != nil {
						log.Println("close old db file err: ", err)
						return
					}
					if err = os.Remove(file.File.Name()); err != nil {
						log.Println("remove old db file err: ", err)
						return
					}
				}
			}
			reclaimedTypes.Store(dType, struct{}{})
			newArchivedFiles.Store(dType, archFiles)
		}(uint16(i))
	}
	wg.Wait()

	var mergedCount int
	reclaimedTypes.Range(func(key, value interface{}) bool {
		mergedCount++
		return true
	})
	if mergedCount < mergeTypes {
		log.Printf("rosedb: merge stopped(total:%d, finished:%d), it will continue in next interval.\n", mergeTypes, mergedCount)
		return
	}

	dbArchivedFiles := make(ArchivedFiles)
	for i := 0; i < DataStructureNum; i++ {
		dType := uint16(i)
		value, ok := newArchivedFiles.Load(dType)
		if !ok {
			log.Printf("one type of data(%d) is missed after reclaiming.", dType)
			return
		}
		dbArchivedFiles[dType] = value.(map[uint32]*storage.DBFile)
	}

	// copy the temporary reclaim directory as new db files.
	for dataType, files := range dbArchivedFiles {
		if _, exist := reclaimedTypes.Load(dataType); exist {
			for _, f := range files {
				name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[dataType], f.Id)
				os.Rename(mergePath+name, db.config.DirPath+name)
			}
		}
	}
	if err = os.RemoveAll(mergePath); err != nil {
		return
	}

	db.archFiles = dbArchivedFiles

	// remove the txn meta file and create a new one.
	if err = db.txnMeta.txnFile.File.Close(); err != nil {
		log.Println("close txn file err: ", err)
		return
	}
	if err = os.Remove(db.config.DirPath + dbTxMetaSaveFile); err == nil {
		var txnMeta *TxnMeta
		activeTxIds := db.txnMeta.ActiveTxIds
		txnMeta, err = LoadTxnMeta(db.config.DirPath + dbTxMetaSaveFile)
		if err != nil {
			return err
		}

		db.txnMeta = txnMeta
		// write active tx ids.
		activeTxIds.Range(func(key, value interface{}) bool {
			if txId, ok := key.(uint64); ok {
				if err = db.MarkCommit(txId); err != nil {
					return false
				}
			}
			return true
		})
	}
	return
}

// StopMerge send a stop signal to merge process.
// Then the merge operation will quit.
func (db *RoseDB) StopMerge() {
	if db.mergeChn == nil {
		db.mergeChn = make(chan struct{}, DataStructureNum)
	}

	go func() {
		for i := 0; i < DataStructureNum; i++ {
			db.mergeChn <- struct{}{}
		}
	}()
}

// SingleMerge reclaim a single db file`s space according to the param fileId.
// File id is the non-zero part of a db file`s name prefix, such as 000000000.data.str (fileId is 0), 000000101.data.str (fileId is 101), etc.
// Only support String type now.
func (db *RoseDB) SingleMerge(fileId uint32) (err error) {
	// if reclaim operation is in progress, single reclaim can`t be executed.
	if db.isReclaiming {
		return ErrDBisReclaiming
	}

	// create a temporary directory for storing the new db files.
	mergePath := db.config.DirPath + mergePath
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(mergePath)

	db.mu.Lock()
	defer func() {
		db.isSingleReclaiming = false
		db.mu.Unlock()
	}()

	db.isSingleReclaiming = true
	var exist bool
	for _, file := range db.archFiles[String] {
		if file.Id == fileId {
			exist = true
			break
		}
	}
	if !exist {
		return
	}

	file := db.archFiles[String][fileId]
	// not reached the threshold.
	var (
		readOff      int64
		validEntries []*storage.Entry
	)
	// read and find all valid entries.
	for {
		entry, err := file.Read(readOff)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if db.validEntry(entry, readOff, fileId) {
			validEntries = append(validEntries, entry)
		}
		readOff += int64(entry.Size())
	}

	// remove redundant db file, update reclaimable space and archived files.
	if len(validEntries) == 0 {
		os.Remove(file.File.Name())
		delete(db.archFiles[String], fileId)
		return
	}

	// rewrite the valid entry.
	df, err := storage.NewDBFile(mergePath, fileId, db.config.RwMethod, db.config.BlockSize, String)
	if err != nil {
		return err
	}
	for _, e := range validEntries {
		if err := df.Write(e); err != nil {
			return err
		}

		// update the String index.
		item := db.strIndex.idxList.Get(e.Meta.Key)
		idx := item.Value().(*index.Indexer)
		idx.Offset = df.Offset - int64(e.Size())
		idx.FileId = fileId
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
	}

	// delete old db file.
	os.Remove(file.File.Name())
	// copy the temporary file as new archived file.
	name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[String], fileId)
	os.Rename(mergePath+name, db.config.DirPath+name)

	// update the archived file.
	db.archFiles[String][fileId] = df
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
func (db *RoseDB) buildIndex(entry *storage.Entry, idx *index.Indexer, isOpen bool) (err error) {
	if db.config.IdxMode == KeyValueMemMode && entry.GetType() == String {
		idx.Meta.Value = entry.Meta.Value
		idx.Meta.ValueSize = uint32(len(entry.Meta.Value))
	}
	// uncommitted entry is invalid.
	if entry.TxId != 0 && isOpen {
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

// validEntry check whether entry is valid(contains add and update types of operations).
// expired entry will be filtered.
func (db *RoseDB) validEntry(e *storage.Entry, offset int64, fileId uint32) bool {
	if e == nil {
		return false
	}

	// uncommitted entry is invalid.
	if e.TxId != 0 {
		if _, ok := db.txnMeta.CommittedTxIds[e.TxId]; !ok {
			return false
		}
		e.TxId = 0
	}

	mark := e.GetMark()
	switch e.GetType() {
	case String:
		deadline, exist := db.expires[String][string(e.Meta.Key)]
		now := time.Now().Unix()

		if mark == StringExpire {
			if exist && deadline > now {
				return true
			}
		}
		if mark == StringSet || mark == StringPersist {
			// check expired.
			if exist && deadline <= now {
				return false
			}

			// check the data position.
			node := db.strIndex.idxList.Get(e.Meta.Key)
			if node == nil {
				return false
			}
			indexer := node.Value().(*index.Indexer)
			if bytes.Compare(indexer.Meta.Key, e.Meta.Key) == 0 {
				if indexer != nil && indexer.FileId == fileId && indexer.Offset == offset {
					return true
				}
			}
		}
	case List:
		if mark == ListLExpire {
			deadline, exist := db.expires[List][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == ListLPush || mark == ListRPush || mark == ListLInsert || mark == ListLSet {
			if db.LValExists(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
	case Hash:
		if mark == HashHExpire {
			deadline, exist := db.expires[Hash][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == HashHSet {
			if val := db.HGet(e.Meta.Key, e.Meta.Extra); string(val) == string(e.Meta.Value) {
				return true
			}
		}
	case Set:
		if mark == SetSExpire {
			deadline, exist := db.expires[Set][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == SetSMove {
			if db.SIsMember(e.Meta.Extra, e.Meta.Value) {
				return true
			}
		}
		if mark == SetSAdd {
			if db.SIsMember(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
	case ZSet:
		if mark == ZSetZExpire {
			deadline, exist := db.expires[ZSet][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == ZSetZAdd {
			if val, err := utils.StrToFloat64(string(e.Meta.Extra)); err == nil {
				ok, score := db.ZScore(e.Meta.Key, e.Meta.Value)
				if ok && score == val {
					return true
				}
			}
		}
	}
	return false
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

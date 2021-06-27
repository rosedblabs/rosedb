package rosedb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/roseduan/rosedb/index"
	"github.com/roseduan/rosedb/storage"
	"github.com/roseduan/rosedb/utils"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"sync"
	"time"
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
)

const (

	// The path for saving rosedb config file.
	configSaveFile = string(os.PathSeparator) + "DB.CFG"

	// The path for saving rosedb meta info.
	dbMetaSaveFile = string(os.PathSeparator) + "DB.META"

	// rosedb reclaim path, a temporary dir, will be removed after reclaim.
	reclaimPath = string(os.PathSeparator) + "rosedb_reclaim"

	// Separator of the extra info, some commands can`t contains it.
	ExtraSeparator = "\\0"

	// DataStructureNum the num of different data structures, there are five now(string, list, hash, set, zset).
	DataStructureNum = 5
)

type (
	// RoseDB the rosedb struct, represents a db instance.
	RoseDB struct {
		activeFile         ActiveFiles     // Current active files.
		activeFileIds      ActiveFileIds   // Current active file ids.
		archFiles          ArchivedFiles   // The archived files.
		strIndex           *StrIdx         // String indexes(a skip list).
		listIndex          *ListIdx        // List indexes.
		hashIndex          *HashIdx        // Hash indexes.
		setIndex           *SetIdx         // Set indexes.
		zsetIndex          *ZsetIdx        // Sorted set indexes.
		config             Config          // Config info of rosedb.
		mu                 sync.RWMutex    // mutex.
		meta               *storage.DBMeta // Meta info for rosedb.
		expires            Expires         // Expired directory.
		isReclaiming       bool
		isSingleReclaiming bool
	}

	// ActiveFiles current active files for different data types.
	ActiveFiles map[DataType]*storage.DBFile

	// ActiveFileIds current active files id for different data types.
	ActiveFileIds map[DataType]uint32

	// ArchivedFiles define the archived files, which mean these files can only be read.
	// and will never be opened for writing.
	ArchivedFiles map[DataType]map[uint32]*storage.DBFile

	// Expires saves the expire info of different keys.
	Expires map[DataType]map[string]int64
)

// Open a rosedb instance.
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
	activeFiles := make(ActiveFiles)
	for dataType, fileId := range activeFileIds {
		file, err := storage.NewDBFile(config.DirPath, fileId, config.RwMethod, config.BlockSize, dataType)
		if err != nil {
			return nil, err
		}
		activeFiles[dataType] = file
	}

	// load db meta info, only active file`s write offset right now.
	meta := storage.LoadMeta(config.DirPath + dbMetaSaveFile)
	for dataType, file := range activeFiles {
		file.Offset = meta.ActiveWriteOff[dataType]
	}

	db := &RoseDB{
		activeFile:    activeFiles,
		activeFileIds: activeFileIds,
		archFiles:     archFiles,
		config:        config,
		strIndex:      newStrIdx(),
		meta:          meta,
		listIndex:     newListIdx(),
		hashIndex:     newHashIdx(),
		setIndex:      newSetIdx(),
		zsetIndex:     newZsetIdx(),
		expires:       make(Expires),
	}
	for i := 0; i < DataStructureNum; i++ {
		db.expires[uint16(i)] = make(map[string]int64)
	}

	// load indexes from db files.
	if err := db.loadIdxFromFiles(); err != nil {
		return nil, err
	}

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
func (db *RoseDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.saveConfig(); err != nil {
		return err
	}
	if err := db.saveMeta(); err != nil {
		return err
	}

	// close and sync the active file.
	for _, file := range db.activeFile {
		if err := file.Close(true); err != nil {
			return err
		}
	}

	// close the archived files.
	for _, archFile := range db.archFiles {
		for _, file := range archFile {
			if err := file.Sync(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Persist the db files.
func (db *RoseDB) Sync() error {
	if db == nil || db.activeFile == nil {
		return nil
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, file := range db.activeFile {
		if err := file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Reclaim reclaim db files`s redundant space in disk.
// Reclaim operation will read all archived files, iterate all entries and find the valid.
// Then rewrite the valid entries to new db files.
// So the time required for reclaim operation depend on the number of entries, you`d better execute it in low peak period.
func (db *RoseDB) Reclaim() (err error) {
	// if single reclaiming is in progress, the reclaim operation can`t be executed.
	if db.isSingleReclaiming {
		return ErrDBisReclaiming
	}
	var reclaimable bool
	for _, archFiles := range db.archFiles {
		if len(archFiles) >= db.config.ReclaimThreshold {
			reclaimable = true
			break
		}
	}
	if !reclaimable {
		return ErrReclaimUnreached
	}

	// create a temporary directory for storing the new db files.
	reclaimPath := db.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(reclaimPath)

	db.mu.Lock()
	defer func() {
		db.isReclaiming = false
		db.mu.Unlock()
	}()
	db.isReclaiming = true

	// processing the different types of files in different goroutines.
	newArchivedFiles := sync.Map{}
	reclaimedTypes := sync.Map{}
	wg := sync.WaitGroup{}
	wg.Add(DataStructureNum)
	for i := 0; i < DataStructureNum; i++ {
		go func(dType uint16) {
			defer func() {
				wg.Done()
			}()

			if len(db.archFiles[dType]) < db.config.ReclaimThreshold {
				newArchivedFiles.Store(dType, db.archFiles[dType])
				return
			}

			var (
				df        *storage.DBFile
				fileId    uint32
				archFiles = make(map[uint32]*storage.DBFile)
				fileIds   []int
			)

			for _, file := range db.archFiles[dType] {
				fileIds = append(fileIds, int(file.Id))
			}
			sort.Ints(fileIds)

			for _, fid := range fileIds {
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
						df, err = storage.NewDBFile(reclaimPath, fileId, db.config.RwMethod, db.config.BlockSize, dType)
						if err != nil {
							log.Fatalf("err occurred when create new db file: %+v", err)
							return
						}
						archFiles[fileId] = df
						fileId += 1
					}

					if err = df.Write(entry); err != nil {
						log.Fatalf("err occurred when write the entry: %+v", err)
						return
					}

					// Since the str types value will be read from db file, so should update the index info.
					if dType == String {
						item := db.strIndex.idxList.Get(entry.Meta.Key)
						idx := item.Value().(*index.Indexer)
						idx.Offset = df.Offset - int64(entry.Size())
						idx.FileId = fileId
						db.strIndex.idxList.Put(idx.Meta.Key, idx)
					}
				}
			}
			reclaimedTypes.Store(dType, struct{}{})
			newArchivedFiles.Store(dType, archFiles)
		}(uint16(i))
	}
	wg.Wait()

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

	// delete the old db files.
	for dataType, files := range db.archFiles {
		if _, exist := reclaimedTypes.Load(dataType); exist {
			for _, f := range files {
				_ = os.Remove(f.File.Name())
			}
		}
	}

	// copy the temporary reclaim directory as new db files.
	for dataType, files := range dbArchivedFiles {
		if _, exist := reclaimedTypes.Load(dataType); exist {
			for _, f := range files {
				name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[dataType], f.Id)
				os.Rename(reclaimPath+name, db.config.DirPath+name)
			}
		}
	}

	db.archFiles = dbArchivedFiles
	return
}

// SingleReclaim reclaim the db files`space according to the SingleReclaimThreshold in config, you can execute it by setting a cron.
// Only support String type now.
func (db *RoseDB) SingleReclaim() (err error) {
	// if reclaim operation is in progress, single reclaim can`t be executed.
	if db.isReclaiming {
		return ErrDBisReclaiming
	}

	// create a temporary directory for storing the new db files.
	reclaimPath := db.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(reclaimPath)

	db.mu.Lock()
	defer func() {
		db.isSingleReclaiming = false
		db.mu.Unlock()
	}()

	db.isSingleReclaiming = true
	var fileIds []int
	for _, file := range db.archFiles[String] {
		fileIds = append(fileIds, int(file.Id))
	}
	// read db files in order.
	sort.Ints(fileIds)

	for _, fid := range fileIds {
		file := db.archFiles[String][uint32(fid)]
		// not reached the threshold.
		if db.meta.ReclaimableSpace[file.Id] < db.config.SingleReclaimThreshold {
			continue
		}

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
			if db.validEntry(entry, readOff, uint32(fid)) {
				validEntries = append(validEntries, entry)
			}
			readOff += int64(entry.Size())
		}

		// remove redundant db file, update reclaimable space and archived files.
		if len(validEntries) == 0 {
			os.Remove(file.File.Name())
			delete(db.meta.ReclaimableSpace, uint32(fid))
			delete(db.archFiles[String], uint32(fid))
			continue
		}

		// rewrite the valid entry.
		df, err := storage.NewDBFile(reclaimPath, uint32(fid), db.config.RwMethod, db.config.BlockSize, String)
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
			idx.FileId = uint32(fid)
			db.strIndex.idxList.Put(idx.Meta.Key, idx)
		}

		// delete old db file.
		os.Remove(file.File.Name())
		// copy the temporary file as new archived file.
		name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[String], fid)
		os.Rename(reclaimPath+name, db.config.DirPath+name)

		// update reclaimable space in db meta.
		db.meta.ReclaimableSpace[uint32(fid)] = 0
		// update the archived file.
		db.archFiles[String][uint32(fid)] = df
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

func (db *RoseDB) saveMeta() error {
	metaPath := db.config.DirPath + dbMetaSaveFile
	return db.meta.Store(metaPath)
}

// build the indexes for different data structures.
func (db *RoseDB) buildIndex(entry *storage.Entry, idx *index.Indexer) error {
	if db.config.IdxMode == KeyValueMemMode {
		idx.Meta.Value = entry.Meta.Value
		idx.Meta.ValueSize = uint32(len(entry.Meta.Value))
	}

	switch entry.GetType() {
	case storage.String:
		db.buildStringIndex(idx, entry)
	case storage.List:
		db.buildListIndex(idx, entry)
	case storage.Hash:
		db.buildHashIndex(idx, entry)
	case storage.Set:
		db.buildSetIndex(idx, entry)
	case storage.ZSet:
		db.buildZsetIndex(idx, entry)
	}
	return nil
}

// write entry to db file.
func (db *RoseDB) store(e *storage.Entry) error {
	// sync the db file if file size is not enough, and open a new db file.
	config := db.config
	if db.activeFile[e.GetType()].Offset+int64(e.Size()) > config.BlockSize {
		if err := db.activeFile[e.GetType()].Sync(); err != nil {
			return err
		}

		// save the old db file as arched file.
		activeFileId := db.activeFileIds[e.GetType()]
		db.archFiles[e.GetType()][activeFileId] = db.activeFile[e.GetType()]
		activeFileId = activeFileId + 1

		newDbFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize, e.GetType())
		if err != nil {
			return err
		}
		db.activeFile[e.GetType()] = newDbFile
		db.activeFileIds[e.GetType()] = activeFileId
		db.meta.ActiveWriteOff[e.GetType()] = 0
	}

	// write entry to db file.
	if err := db.activeFile[e.GetType()].Write(e); err != nil {
		return err
	}

	db.meta.ActiveWriteOff[e.GetType()] = db.activeFile[e.GetType()].Offset

	// persist db file according to the config.
	if config.Sync {
		if err := db.activeFile[e.GetType()].Sync(); err != nil {
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
				if indexer == nil || indexer.FileId != fileId || indexer.Offset != offset {
					return false
				}
			}

			if val, err := db.Get(e.Meta.Key); err == nil && string(val) == string(e.Meta.Value) {
				return true
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
		if mark == HashExpire {
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
				score := db.ZScore(e.Meta.Key, e.Meta.Value)
				if score == val {
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
			if ele := db.strIndex.idxList.Remove(key); ele != nil {
				db.incrReclaimableSpace(key)
			}
		case List:
			e = storage.NewEntryNoExtra(key, nil, List, ListLClear)
			db.listIndex.indexes.LClear(string(key))
		case Hash:
			e = storage.NewEntryNoExtra(key, nil, Hash, HashClear)
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

package rosedb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"rosedb/ds/list"
	"rosedb/index"
	"rosedb/storage"
	"rosedb/utils"
	"sync"
)

var (
	ErrEmptyKey = errors.New("rosedb: the key is empty")

	ErrKeyNotExist = errors.New("rosedb: key not exist")

	ErrKeyTooLarge = errors.New("rosedb: key exceeded the max length")

	ErrValueTooLarge = errors.New("rosedb: value exceeded the max length")

	ErrNilIndexer = errors.New("rosedb: indexer is nil")

	ErrCfgNotExist = errors.New("rosedb: the config file not exist")

	ErrReclaimUnreached = errors.New("rosedb: unused space not reach the threshold")

	ErrExtraContainsSeparator = errors.New("rosedb: extra contains separator \\0")
)

const (

	//保存配置的文件名称
	configSaveFile = string(os.PathSeparator) + "db.cfg"

	//保存索引状态的文件名称
	indexSaveFile = string(os.PathSeparator) + "db.idx"

	//保存数据库相关信息的文件名称
	dbMetaSaveFile = string(os.PathSeparator) + "db.meta"

	//回收磁盘空间时的临时目录
	reclaimPath = string(os.PathSeparator) + "rosedb_reclaim"

	//额外信息的分隔符，用于存储一些额外的信息（因此一些操作的value中不能包含此分隔符）
	ExtraSeparator = "\\0"
)

type (
	RoseDB struct {
		activeFile   *storage.DBFile
		archFiles    ArchivedFiles
		idxList      *index.SkipList
		listIndex    *list.List
		config       Config
		activeFileId uint32
		mu           sync.RWMutex
		meta         *storage.DBMeta
	}

	//已封存的文件定义
	ArchivedFiles map[uint32]*storage.DBFile
)

//打开一个数据库实例
func Open(config Config) (*RoseDB, error) {

	//如果目录不存在则创建
	if !utils.Exist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//如果存在索引文件，则加载索引状态
	skipList := index.NewSkipList()
	if utils.Exist(config.DirPath + indexSaveFile) {
		err := index.Build(skipList, config.DirPath+indexSaveFile)
		if err != nil {
			return nil, err
		}
	}

	//加载数据文件
	archFiles, activeFileId, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	activeFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	//加载数据库额外的信息
	meta := storage.LoadMeta(config.DirPath + dbMetaSaveFile)
	activeFile.Offset = meta.ActiveWriteOff

	db := &RoseDB{
		activeFile:   activeFile,
		archFiles:    archFiles,
		config:       config,
		activeFileId: activeFileId,
		idxList:      skipList,
		meta:         meta,
		listIndex:    list.New(),
	}

	//再加载List、Hash、Set、ZSet索引
	if err := db.loadIdxFromFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

//根据配置重新打开数据库
func Reopen(path string) (*RoseDB, error) {
	if exist := utils.Exist(path + configSaveFile); !exist {
		return nil, ErrCfgNotExist
	}

	var config Config

	if bytes, err := ioutil.ReadFile(path + configSaveFile); err != nil {
		return nil, err
	} else {
		if err := json.Unmarshal(bytes, &config); err != nil {
			return nil, err
		}
	}

	return Open(config)
}

//关闭数据库，保存相关配置
func (db *RoseDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.saveConfig(); err != nil {
		return err
	}

	if err := db.saveIndexes(); err != nil {
		return err
	}

	if err := db.saveMeta(); err != nil {
		return err
	}

	db.activeFile = nil
	db.idxList = nil
	return nil
}

//数据持久化
func (db *RoseDB) Sync() error {
	if db == nil || db.activeFile == nil {
		return nil
	}

	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.activeFile.Sync()
}

//删除数据
func (db *RoseDB) Remove(key []byte) error {

	if err := db.checkKeyValue(key, nil); err != nil {
		return err
	}

	//增加可回收的磁盘空间
	e := db.idxList.Get(key)
	if e != nil {
		idx := e.Value().(*index.Indexer)
		if idx != nil {
			db.meta.UnusedSpace += uint64(idx.EntrySize)
		}
	}

	//删除其在内存中的索引
	if e != nil {
		db.idxList.Remove(key)
	}
	return nil
}

//重新组织磁盘中的数据，回收磁盘空间
func (db *RoseDB) Reclaim() error {

	if db.meta.UnusedSpace < db.config.ReclaimThreshold {
		return ErrReclaimUnreached
	}

	if db.idxList.Len <= 0 {
		return nil
	}

	//新建临时目录，用于暂存新的数据文件
	reclaimPath := db.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}

	defer os.RemoveAll(reclaimPath)

	var (
		success             = true
		activeFileId uint32 = 0
		newArchFiles        = make(ArchivedFiles)
		df           *storage.DBFile
	)

	//遍历所有的key，将数据写入到临时文件中
	db.idxList.Foreach(func(e *index.Element) bool {
		idx := e.Value().(*index.Indexer)

		if idx != nil && db.archFiles[idx.FileId] != nil {
			if df == nil {
				df, _ = storage.NewDBFile(reclaimPath, activeFileId, db.config.RwMethod, db.config.BlockSize)
				newArchFiles[activeFileId] = df
			}

			if int64(idx.EntrySize)+df.Offset > db.config.BlockSize {
				df.Close(true)
				activeFileId += 1

				df, _ = storage.NewDBFile(reclaimPath, activeFileId, db.config.RwMethod, db.config.BlockSize)
				newArchFiles[activeFileId] = df
			}

			entry, err := db.archFiles[idx.FileId].Read(idx.Offset)
			if err != nil {
				success = false
				return false
			}

			//更新索引
			idx.FileId = df.Id
			idx.Offset = df.Offset
			e.SetValue(idx)

			if err := df.Write(entry); err != nil {
				success = false
				return false
			}
		}

		return true
	})

	db.mu.Lock()
	defer db.mu.Unlock()

	//重新保存索引
	if err := db.saveIndexes(); err != nil {
		return err
	}

	if success {

		//旧数据删除，临时目录拷贝为新的数据文件
		for _, v := range db.archFiles {
			os.Remove(v.File.Name())
		}

		for _, v := range newArchFiles {
			name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatName, v.Id)
			os.Rename(reclaimPath+name, db.config.DirPath+name)
		}

		//更新数据库相关信息
		db.meta.UnusedSpace = 0
		db.archFiles = newArchFiles
	}

	return nil
}

//复制数据库目录，用于备份
func (db *RoseDB) Backup(dir string) (err error) {
	if utils.Exist(db.config.DirPath) {

		err = utils.CopyDir(db.config.DirPath, dir)
	}

	return err
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

//关闭数据库之前保存配置
func (db *RoseDB) saveConfig() (err error) {
	//保存配置
	path := db.config.DirPath + configSaveFile
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)

	bytes, err := json.Marshal(db.config)
	_, err = file.Write(bytes)
	err = file.Close()

	return
}

//保存索引状态
func (db *RoseDB) saveIndexes() error {
	idxPath := db.config.DirPath + indexSaveFile
	return index.Store(db.idxList, idxPath)
}

func (db *RoseDB) saveMeta() error {
	metaPath := db.config.DirPath + dbMetaSaveFile
	return db.meta.Store(metaPath)
}

//建立索引
func (db *RoseDB) buildIndex(e *storage.Entry, idx *index.Indexer) error {

	if db.config.IdxMode == KeyValueRamMode {
		idx.Meta.Value = e.Meta.Value
		idx.Meta.ValueSize = uint32(len(e.Meta.Value))
	}

	if e.Type == storage.String {
		db.idxList.Put(idx.Meta.Key, idx)
	}

	if e.Type == storage.List {
		db.buildListIndex(idx, e.Mark)
	}

	return nil
}

//写数据
func (db *RoseDB) store(e *storage.Entry) (idx *index.Indexer, err error) {

	//如果数据文件空间不够，则关闭该文件，并新打开一个文件
	config := db.config
	if db.activeFile.Offset+int64(e.Size()) > config.BlockSize {
		if err = db.activeFile.Close(true); err != nil {
			return
		}

		//保存旧的文件
		db.archFiles[db.activeFileId] = db.activeFile

		activeFileId := db.activeFileId + 1
		var dbFile *storage.DBFile

		if dbFile, err = storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize); err != nil {
			return
		} else {
			db.activeFile = dbFile
			db.activeFileId = activeFileId
			db.meta.ActiveWriteOff = 0
		}
	}

	//如果key已经存在，则原来的值被舍弃，所以需要新增可回收的磁盘空间值
	if e := db.idxList.Get(e.Meta.Key); e != nil {
		item := e.Value().(*index.Indexer)
		if item != nil {
			db.meta.UnusedSpace += uint64(item.EntrySize)
		}
	}

	//数据索引
	idx = &index.Indexer{
		Meta: &storage.Meta{
			KeySize: uint32(len(e.Meta.Key)),
			Key:     e.Meta.Key,
		},
		FileId:    db.activeFileId,
		EntrySize: e.Size(),
		Offset:    db.activeFile.Offset,
	}

	//写入数据至文件中
	if err = db.activeFile.Write(e); err != nil {
		return
	}

	db.meta.ActiveWriteOff = db.activeFile.Offset

	//数据持久化
	if config.Sync {
		if err = db.activeFile.Sync(); err != nil {
			return
		}
	}

	return
}

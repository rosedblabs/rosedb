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
	"os"
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
)

const (

	// 保存配置的文件名称
	// rosedb config save path
	configSaveFile = string(os.PathSeparator) + "db.cfg"

	// 保存数据库相关信息的文件名称
	// rosedb meta info save path
	dbMetaSaveFile = string(os.PathSeparator) + "db.meta"

	// 回收磁盘空间时的临时目录
	// rosedb reclaim path
	reclaimPath = string(os.PathSeparator) + "rosedb_reclaim"

	// 保存过期字典的文件名称
	// expired directory save path
	expireFile = string(os.PathSeparator) + "db.expires"

	// ExtraSeparator 额外信息的分隔符，用于存储一些额外的信息（因此一些操作的value中不能包含此分隔符）
	// separator of the extra info
	ExtraSeparator = "\\0"
)

type (
	// RoseDB the rosedb struct
	RoseDB struct {
		activeFile   *storage.DBFile //当前活跃文件       current active file
		activeFileId uint32          //活跃文件id	       current active file id
		archFiles    ArchivedFiles   //已封存文件        the archived files
		strIndex     *StrIdx         //字符串索引列表     string indexes
		listIndex    *ListIdx        //list索引列表      list indexes
		hashIndex    *HashIdx        //hash索引列表      hash indexes
		setIndex     *SetIdx         //集合索引列表       set indexes
		zsetIndex    *ZsetIdx        //有序集合索引列表   sorted set indexes
		config       Config          //数据库配置		   config of rosedb
		mu           sync.RWMutex    //mutex
		meta         *storage.DBMeta //数据库配置额外信息  meta info for rosedb
		expires      storage.Expires //过期字典          expired directory
	}

	// ArchivedFiles 已封存的文件定义
	// define the archived files
	ArchivedFiles map[uint32]*storage.DBFile
)

// Open 打开一个数据库实例
// open a rosedb instance
func Open(config Config) (*RoseDB, error) {

	//如果目录不存在则创建
	//create the dirs if not exists.
	if !utils.Exist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//加载数据文件
	//load the db files
	archFiles, activeFileId, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	activeFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	//加载过期字典
	//load expired directories
	expires := storage.LoadExpires(config.DirPath + expireFile)

	//加载数据库额外的信息
	//load db meta info
	meta := storage.LoadMeta(config.DirPath + dbMetaSaveFile)
	activeFile.Offset = meta.ActiveWriteOff

	db := &RoseDB{
		activeFile:   activeFile,
		activeFileId: activeFileId,
		archFiles:    archFiles,
		config:       config,
		strIndex:     newStrIdx(),
		meta:         meta,
		listIndex:    newListIdx(),
		hashIndex:    newHashIdx(),
		setIndex:     newSetIdx(),
		zsetIndex:    newZsetIdx(),
		expires:      expires,
	}

	//加载索引信息
	//load indexes from files
	if err := db.loadIdxFromFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Reopen 根据配置重新打开数据库
// reopen the db according to the specific config path
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

// Close 关闭数据库，保存相关配置
// close db and save relative configs.
func (db *RoseDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.saveConfig(); err != nil {
		return err
	}
	if err := db.saveMeta(); err != nil {
		return err
	}
	if err := db.expires.SaveExpires(db.config.DirPath + expireFile); err != nil {
		return err
	}

	// close and sync the active file.
	if err := db.activeFile.Close(true); err != nil {
		return err
	}

	// close the archived files.
	for _, archFile := range db.archFiles {
		if err := archFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 数据持久化
// persist data to disk.
func (db *RoseDB) Sync() error {
	if db == nil || db.activeFile == nil {
		return nil
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.activeFile.Sync()
}

// Reclaim 重新组织磁盘中的数据，回收磁盘空间
// reclaim db files in disk.
func (db *RoseDB) Reclaim() (err error) {
	if len(db.archFiles) < db.config.ReclaimThreshold {
		return ErrReclaimUnreached
	}

	//新建临时目录，用于暂存新的数据文件
	//create a temporary directory for storing the new db files.
	reclaimPath := db.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(reclaimPath)

	var (
		activeFileId uint32 = 0
		newArchFiles        = make(ArchivedFiles)
		df           *storage.DBFile
	)

	db.mu.Lock()
	defer db.mu.Unlock()
	for _, file := range db.archFiles {
		var offset int64 = 0
		var reclaimEntries []*storage.Entry

		var dfFile *os.File
		dfFile, err = os.Open(file.File.Name())
		if err != nil {
			return err
		}
		file.File = dfFile
		fileId := file.Id

		for {
			if e, err := file.Read(offset); err == nil {
				//判断是否为有效的entry
				//check whether the entry is valid.
				if db.validEntry(e, offset, fileId) {
					reclaimEntries = append(reclaimEntries, e)
				}
				offset += int64(e.Size())
			} else {
				if err == io.EOF {
					break
				}
				return err
			}
		}

		//重新将entry写入到文件中
		//rewrite entry to the db file.
		if len(reclaimEntries) > 0 {
			for _, entry := range reclaimEntries {
				if df == nil || int64(entry.Size())+df.Offset > db.config.BlockSize {
					df, err = storage.NewDBFile(reclaimPath, activeFileId, db.config.RwMethod, db.config.BlockSize)
					if err != nil {
						return
					}

					newArchFiles[activeFileId] = df
					activeFileId++
				}

				if err = df.Write(entry); err != nil {
					return
				}

				//字符串类型的索引需要在这里更新
				//update string indexes.
				if entry.Type == String {
					item := db.strIndex.idxList.Get(entry.Meta.Key)
					idx := item.Value().(*index.Indexer)
					idx.Offset = df.Offset - int64(entry.Size())
					idx.FileId = activeFileId
					db.strIndex.idxList.Put(idx.Meta.Key, idx)
				}
			}
		}
	}

	//旧数据删除，临时目录拷贝为新的数据文件
	//delete the old db files, and copy the directory as new db files.
	for _, v := range db.archFiles {
		_ = os.Remove(v.File.Name())
	}

	for _, v := range newArchFiles {
		name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatName, v.Id)
		os.Rename(reclaimPath+name, db.config.DirPath+name)
	}

	db.archFiles = newArchFiles
	return
}

// Backup 复制数据库目录，用于备份
// copy the database directory for backup.
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

// saveConfig 关闭数据库之前保存配置
// save db config.
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

// buildIndex 建立索引
// build the different indexes.
func (db *RoseDB) buildIndex(e *storage.Entry, idx *index.Indexer) error {

	if db.config.IdxMode == KeyValueRamMode {
		idx.Meta.Value = e.Meta.Value
		idx.Meta.ValueSize = uint32(len(e.Meta.Value))
	}

	switch e.Type {
	case storage.String:
		db.buildStringIndex(idx, e.Mark)
	case storage.List:
		db.buildListIndex(idx, e.Mark)
	case storage.Hash:
		db.buildHashIndex(idx, e.Mark)
	case storage.Set:
		db.buildSetIndex(idx, e.Mark)
	case storage.ZSet:
		db.buildZsetIndex(idx, e.Mark)
	}

	return nil
}

// store 写数据
// write entry to db file.
func (db *RoseDB) store(e *storage.Entry) error {

	//如果数据文件空间不够，则持久化该文件，并新打开一个文件
	//sync the db file if file size is not enough, and open a new db file.
	config := db.config
	if db.activeFile.Offset+int64(e.Size()) > config.BlockSize {
		if err := db.activeFile.Sync(); err != nil {
			return err
		}

		//保存旧的文件
		//save old db file
		db.archFiles[db.activeFileId] = db.activeFile
		activeFileId := db.activeFileId + 1

		if dbFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize); err != nil {
			return err
		} else {
			db.activeFile = dbFile
			db.activeFileId = activeFileId
			db.meta.ActiveWriteOff = 0
		}
	}

	//写入数据至文件中
	//write data to db file.
	if err := db.activeFile.Write(e); err != nil {
		return err
	}

	db.meta.ActiveWriteOff = db.activeFile.Offset

	//数据持久化
	//persist the data to disk.
	if config.Sync {
		if err := db.activeFile.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// validEntry 判断entry所属的操作标识(增、改类型的操作)，以及val是否是有效的
// check whether entry is valid(contains add and update types of operations).
func (db *RoseDB) validEntry(e *storage.Entry, offset int64, fileId uint32) bool {
	if e == nil {
		return false
	}

	mark := e.Mark
	switch e.Type {
	case String:
		if mark == StringSet {
			// expired key is not valid.
			now := uint32(time.Now().Unix())
			if deadline, exist := db.expires[string(e.Meta.Key)]; exist && deadline <= now {
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
		if mark == ListLPush || mark == ListRPush || mark == ListLInsert || mark == ListLSet {
			//由于List是链表结构，无法有效的进行检索，取出全部数据依次比较的开销太大
			//因此暂时不参与reclaim，后续再想想其他的解决方案
			return true
		}
	case Hash:
		if mark == HashHSet {
			if val := db.HGet(e.Meta.Key, e.Meta.Extra); string(val) == string(e.Meta.Value) {
				return true
			}
		}
	case Set:
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

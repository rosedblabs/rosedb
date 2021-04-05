package rosedb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"rosedb/ds/hash"
	"rosedb/ds/list"
	"rosedb/ds/set"
	"rosedb/ds/zset"
	"rosedb/index"
	"rosedb/storage"
	"rosedb/utils"
	"sync"
	"time"
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

	ErrInvalidTtl = errors.New("rosedb: invalid ttl")

	ErrKeyExpired = errors.New("rosedb: key is expired")
)

const (

	//保存配置的文件名称
	configSaveFile = string(os.PathSeparator) + "db.cfg"

	//保存数据库相关信息的文件名称
	dbMetaSaveFile = string(os.PathSeparator) + "db.meta"

	//回收磁盘空间时的临时目录
	reclaimPath = string(os.PathSeparator) + "rosedb_reclaim"

	//保存过期字典的文件名称
	expireFile = string(os.PathSeparator) + "db.expires"

	//额外信息的分隔符，用于存储一些额外的信息（因此一些操作的value中不能包含此分隔符）
	ExtraSeparator = "\\0"
)

type (
	RoseDB struct {
		activeFile   *storage.DBFile //当前活跃文件
		activeFileId uint32          //活跃文件id
		archFiles    ArchivedFiles   //已封存文件
		idxList      *index.SkipList //字符串索引列表
		listIndex    *list.List      //list索引列表
		hashIndex    *hash.Hash      //hash索引列表
		setIndex     *set.Set        //集合索引列表
		zsetIndex    *zset.SortedSet //有序集合索引列表
		config       Config          //数据库配置
		mu           sync.RWMutex    //mutex
		meta         *storage.DBMeta //数据库配置额外信息
		expires      storage.Expires //过期字典
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

	//加载数据文件
	archFiles, activeFileId, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	activeFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	//加载过期字典
	expires := storage.LoadExpires(config.DirPath + expireFile)

	//加载数据库额外的信息
	meta := storage.LoadMeta(config.DirPath + dbMetaSaveFile)
	activeFile.Offset = meta.ActiveWriteOff

	db := &RoseDB{
		activeFile:   activeFile,
		activeFileId: activeFileId,
		archFiles:    archFiles,
		config:       config,
		idxList:      index.NewSkipList(),
		meta:         meta,
		listIndex:    list.New(),
		hashIndex:    hash.New(),
		setIndex:     set.New(),
		zsetIndex:    zset.New(),
		expires:      expires,
	}

	//加载索引信息
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
	if err := db.saveMeta(); err != nil {
		return err
	}
	if err := db.expires.SaveExpires(db.config.DirPath + expireFile); err != nil {
		return err
	}
	if err := db.activeFile.Close(true); err != nil {
		return err
	}
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

//重新组织磁盘中的数据，回收磁盘空间
func (db *RoseDB) Reclaim() (err error) {
	if len(db.archFiles) < db.config.ReclaimThreshold {
		return ErrReclaimUnreached
	}

	//新建临时目录，用于暂存新的数据文件
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

	for _, file := range db.archFiles {
		var offset int64 = 0
		var reclaimEntries []*storage.Entry

		for {
			if e, err := file.Read(offset); err == nil {
				//判断是否为有效的entry
				if db.validEntry(e) {
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
				if entry.Type == String {
					item := db.idxList.Get(entry.Meta.Key)
					idx := item.Value().(*index.Indexer)
					idx.Offset = df.Offset - int64(entry.Size())
					idx.FileId = activeFileId
					db.idxList.Put(idx.Meta.Key, idx)
				}
			}
		}
	}

	//旧数据删除，临时目录拷贝为新的数据文件
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

//复制数据库目录，用于备份
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

//写数据
func (db *RoseDB) store(e *storage.Entry) error {

	//如果数据文件空间不够，则关闭该文件，并新打开一个文件
	config := db.config
	if db.activeFile.Offset+int64(e.Size()) > config.BlockSize {
		if err := db.activeFile.Close(true); err != nil {
			return err
		}

		//保存旧的文件
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
	if err := db.activeFile.Write(e); err != nil {
		return err
	}

	db.meta.ActiveWriteOff = db.activeFile.Offset

	//数据持久化
	if config.Sync {
		if err := db.activeFile.Sync(); err != nil {
			return err
		}
	}

	return nil
}

//判断entry所属的操作标识(增、改类型的操作)，以及val是否是有效的
func (db *RoseDB) validEntry(e *storage.Entry) bool {
	if e == nil {
		return false
	}

	mark := e.Mark
	switch e.Type {
	case String:
		if mark == StringSet {
			now := uint32(time.Now().Unix())
			if deadline, exist := db.expires[string(e.Meta.Key)]; exist && deadline <= now {
				return false
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

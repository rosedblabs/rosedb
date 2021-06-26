package rosedb

import (
	"github.com/roseduan/rosedb/ds/list"
	"github.com/roseduan/rosedb/index"
	"github.com/roseduan/rosedb/storage"
	"github.com/roseduan/rosedb/utils"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DataType 数据类型定义
// Define the data type.
type DataType = uint16

// 数据类型定义
// Five different data types, support String, List, Hash, Set, Sorted Set right now.
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

// 字符串相关操作标识
// The operations of String, will be a part of Entry, the same for the other four types.
const (
	StringSet uint16 = iota
	StringRem
	StringExpire
	StringPersist
)

// 列表相关操作标识
// The operations of List.
const (
	ListLPush uint16 = iota
	ListRPush
	ListLPop
	ListRPop
	ListLRem
	ListLInsert
	ListLSet
	ListLTrim
)

// 哈希相关操作标识
// The operations of Hash.
const (
	HashHSet uint16 = iota
	HashHDel
)

// 集合相关操作标识
// The operations of Set.
const (
	SetSAdd uint16 = iota
	SetSRem
	SetSMove
)

// 有序集合相关操作标识
// The operations of Sorted Set.
const (
	ZSetZAdd uint16 = iota
	ZSetZRem
)

// buildStringIndex 建立字符串索引
// build string indexes.
func (db *RoseDB) buildStringIndex(idx *index.Indexer, entry *storage.Entry) {
	if db.listIndex == nil || idx == nil {
		return
	}

	switch entry.GetMark() {
	case StringSet:
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
	case StringRem:
		db.strIndex.idxList.Remove(idx.Meta.Key)
	case StringExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.strIndex.idxList.Remove(idx.Meta.Key)
		} else {
			db.expires[String][string(idx.Meta.Key)] = int64(entry.Timestamp)
		}
	case StringPersist:
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
		delete(db.expires[String], string(idx.Meta.Key))
	}
}

// buildListIndex 建立列表索引
// build list indexes.
func (db *RoseDB) buildListIndex(idx *index.Indexer, opt uint16) {
	if db.listIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt {
	case ListLPush:
		db.listIndex.indexes.LPush(key, idx.Meta.Value)
	case ListLPop:
		db.listIndex.indexes.LPop(key)
	case ListRPush:
		db.listIndex.indexes.RPush(key, idx.Meta.Value)
	case ListRPop:
		db.listIndex.indexes.RPop(key)
	case ListLRem:
		if count, err := strconv.Atoi(string(idx.Meta.Extra)); err == nil {
			db.listIndex.indexes.LRem(key, idx.Meta.Value, count)
		}
	case ListLInsert:
		extra := string(idx.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			pivot := []byte(s[0])
			if opt, err := strconv.Atoi(s[1]); err == nil {
				db.listIndex.indexes.LInsert(string(idx.Meta.Key), list.InsertOption(opt), pivot, idx.Meta.Value)
			}
		}
	case ListLSet:
		if i, err := strconv.Atoi(string(idx.Meta.Extra)); err == nil {
			db.listIndex.indexes.LSet(key, i, idx.Meta.Value)
		}
	case ListLTrim:
		extra := string(idx.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			start, _ := strconv.Atoi(s[0])
			end, _ := strconv.Atoi(s[1])

			db.listIndex.indexes.LTrim(string(idx.Meta.Key), start, end)
		}
	}
}

// buildHashIndex 建立哈希索引
// build hash indexes.
func (db *RoseDB) buildHashIndex(idx *index.Indexer, opt uint16) {

	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt {
	case HashHSet:
		db.hashIndex.indexes.HSet(key, string(idx.Meta.Extra), idx.Meta.Value)
	case HashHDel:
		db.hashIndex.indexes.HDel(key, string(idx.Meta.Extra))
	}
}

// buildSetIndex 建立集合索引
// build set indexes.
func (db *RoseDB) buildSetIndex(idx *index.Indexer, opt uint16) {

	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt {
	case SetSAdd:
		db.setIndex.indexes.SAdd(key, idx.Meta.Value)
	case SetSRem:
		db.setIndex.indexes.SRem(key, idx.Meta.Value)
	case SetSMove:
		extra := idx.Meta.Extra
		db.setIndex.indexes.SMove(key, string(extra), idx.Meta.Value)
	}
}

// buildZsetIndex 建立有序集合索引
// build sorted set indexes.
func (db *RoseDB) buildZsetIndex(idx *index.Indexer, opt uint16) {

	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt {
	case ZSetZAdd:
		if score, err := utils.StrToFloat64(string(idx.Meta.Extra)); err == nil {
			db.zsetIndex.indexes.ZAdd(key, score, string(idx.Meta.Value))
		}
	case ZSetZRem:
		db.zsetIndex.indexes.ZRem(key, string(idx.Meta.Value))
	}
}

// loadIdxFromFiles 从文件中加载String、List、Hash、Set、ZSet索引
// load String、List、Hash、Set、ZSet indexes from db files.
func (db *RoseDB) loadIdxFromFiles() error {
	if db.archFiles == nil && db.activeFile == nil {
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(DataStructureNum)
	for dataType := 0; dataType < DataStructureNum; dataType++ {
		go func(dType uint16) {
			defer func() {
				wg.Done()
			}()

			// archived files
			var fileIds []int
			dbFile := make(map[uint32]*storage.DBFile)
			for k, v := range db.archFiles[dType] {
				dbFile[k] = v
				fileIds = append(fileIds, int(k))
			}

			// active file
			dbFile[db.activeFileIds[dType]] = db.activeFile[dType]
			fileIds = append(fileIds, int(db.activeFileIds[dType]))

			// load the db files in a specified order.
			sort.Ints(fileIds)
			for i := 0; i < len(fileIds); i++ {
				fid := uint32(fileIds[i])
				df := dbFile[fid]
				var offset int64 = 0

				for offset <= db.config.BlockSize {
					if e, err := df.Read(offset); err == nil {
						idx := &index.Indexer{
							Meta:      e.Meta,
							FileId:    fid,
							EntrySize: e.Size(),
							Offset:    offset,
						}
						offset += int64(e.Size())

						if len(e.Meta.Key) > 0 {
							if err := db.buildIndex(e, idx); err != nil {
								log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
							}
						}
					} else {
						if err == io.EOF {
							break
						}
						log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
					}
				}
			}
		}(uint16(dataType))
	}
	wg.Wait()
	return nil
}

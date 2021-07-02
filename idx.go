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

// DataType Define the data structure type.
type DataType = uint16

// Five different data types, support String, List, Hash, Set, Sorted Set right now.
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

// The operations of a String Type, will be a part of Entry, the same for the other four types.
const (
	StringSet uint16 = iota
	StringRem
	StringExpire
	StringPersist
)

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
	ListLClear
	ListLExpire
)

// The operations of Hash.
const (
	HashHSet uint16 = iota
	HashHDel
	HashHClear
	HashHExpire
)

// The operations of Set.
const (
	SetSAdd uint16 = iota
	SetSRem
	SetSMove
	SetSClear
	SetSExpire
)

// The operations of Sorted Set.
const (
	ZSetZAdd uint16 = iota
	ZSetZRem
	ZSetZClear
	ZSetZExpire
)

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

// build list indexes.
func (db *RoseDB) buildListIndex(idx *index.Indexer, entry *storage.Entry) {
	if db.listIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch entry.GetMark() {
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
	case ListLExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.listIndex.indexes.LClear(key)
		} else {
			db.expires[List][key] = int64(entry.Timestamp)
		}
	case ListLClear:
		db.listIndex.indexes.LClear(key)
	}
}

// build hash indexes.
func (db *RoseDB) buildHashIndex(idx *index.Indexer, entry *storage.Entry) {
	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch entry.GetMark() {
	case HashHSet:
		db.hashIndex.indexes.HSet(key, string(idx.Meta.Extra), idx.Meta.Value)
	case HashHDel:
		db.hashIndex.indexes.HDel(key, string(idx.Meta.Extra))
	case HashHClear:
		db.hashIndex.indexes.HClear(key)
	case HashHExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.hashIndex.indexes.HClear(key)
		} else {
			db.expires[Hash][key] = int64(entry.Timestamp)
		}
	}
}

// build set indexes.
func (db *RoseDB) buildSetIndex(idx *index.Indexer, entry *storage.Entry) {
	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch entry.GetMark() {
	case SetSAdd:
		db.setIndex.indexes.SAdd(key, idx.Meta.Value)
	case SetSRem:
		db.setIndex.indexes.SRem(key, idx.Meta.Value)
	case SetSMove:
		extra := idx.Meta.Extra
		db.setIndex.indexes.SMove(key, string(extra), idx.Meta.Value)
	case SetSClear:
		db.setIndex.indexes.SClear(key)
	case SetSExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.setIndex.indexes.SClear(key)
		} else {
			db.expires[Set][key] = int64(entry.Timestamp)
		}
	}
}

// build sorted set indexes.
func (db *RoseDB) buildZsetIndex(idx *index.Indexer, entry *storage.Entry) {
	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch entry.GetMark() {
	case ZSetZAdd:
		if score, err := utils.StrToFloat64(string(idx.Meta.Extra)); err == nil {
			db.zsetIndex.indexes.ZAdd(key, score, string(idx.Meta.Value))
		}
	case ZSetZRem:
		db.zsetIndex.indexes.ZRem(key, string(idx.Meta.Value))
	case ZSetZClear:
		db.zsetIndex.indexes.ZClear(key)
	case ZSetZExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.zsetIndex.indexes.ZClear(key)
		} else {
			db.expires[ZSet][key] = int64(entry.Timestamp)
		}
	}
}

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

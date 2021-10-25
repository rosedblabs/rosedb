package rosedb

import (
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/roseduan/rosedb/ds/list"
	"github.com/roseduan/rosedb/index"
	"github.com/roseduan/rosedb/storage"
	"github.com/roseduan/rosedb/utils"
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
	if db.strIndex == nil || idx == nil {
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
			db.strIndex.idxList.Put(idx.Meta.Key, idx)
		}
	case StringPersist:
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
		delete(db.expires[String], string(idx.Meta.Key))
	}
}

// build list indexes.
func (db *RoseDB) buildListIndex(entry *storage.Entry) {
	if db.listIndex == nil || entry == nil {
		return
	}

	key := string(entry.Meta.Key)
	switch entry.GetMark() {
	case ListLPush:
		db.listIndex.indexes.LPush(key, entry.Meta.Value)
	case ListLPop:
		db.listIndex.indexes.LPop(key)
	case ListRPush:
		db.listIndex.indexes.RPush(key, entry.Meta.Value)
	case ListRPop:
		db.listIndex.indexes.RPop(key)
	case ListLRem:
		if count, err := strconv.Atoi(string(entry.Meta.Extra)); err == nil {
			db.listIndex.indexes.LRem(key, entry.Meta.Value, count)
		}
	case ListLInsert:
		extra := string(entry.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			pivot := []byte(s[0])
			if opt, err := strconv.Atoi(s[1]); err == nil {
				db.listIndex.indexes.LInsert(string(entry.Meta.Key), list.InsertOption(opt), pivot, entry.Meta.Value)
			}
		}
	case ListLSet:
		if i, err := strconv.Atoi(string(entry.Meta.Extra)); err == nil {
			db.listIndex.indexes.LSet(key, i, entry.Meta.Value)
		}
	case ListLTrim:
		extra := string(entry.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			start, _ := strconv.Atoi(s[0])
			end, _ := strconv.Atoi(s[1])

			db.listIndex.indexes.LTrim(string(entry.Meta.Key), start, end)
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
func (db *RoseDB) buildHashIndex(entry *storage.Entry) {
	if db.hashIndex == nil || entry == nil {
		return
	}

	key := string(entry.Meta.Key)
	switch entry.GetMark() {
	case HashHSet:
		db.setHashIndexer(entry, false)
	case HashHDel:
		db.hashIndex.indexes.HDel(key, string(entry.Meta.Extra))
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
func (db *RoseDB) buildSetIndex(entry *storage.Entry) {
	if db.hashIndex == nil || entry == nil {
		return
	}

	key := string(entry.Meta.Key)
	switch entry.GetMark() {
	case SetSAdd:
		db.setIndex.indexes.SAdd(key, entry.Meta.Value)
	case SetSRem:
		db.setIndex.indexes.SRem(key, entry.Meta.Value)
	case SetSMove:
		extra := entry.Meta.Extra
		db.setIndex.indexes.SMove(key, string(extra), entry.Meta.Value)
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
func (db *RoseDB) buildZsetIndex(entry *storage.Entry) {
	if db.hashIndex == nil || entry == nil {
		return
	}

	key := string(entry.Meta.Key)
	switch entry.GetMark() {
	case ZSetZAdd:
		if score, err := utils.StrToFloat64(string(entry.Meta.Extra)); err == nil {
			db.zsetIndex.indexes.ZAdd(key, score, string(entry.Meta.Value))
		}
	case ZSetZRem:
		db.zsetIndex.indexes.ZRem(key, string(entry.Meta.Value))
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
			defer wg.Done()

			// archived files
			var fileIds []int
			dbFile := make(map[uint32]*storage.DBFile)
			for k, v := range db.archFiles[dType] {
				dbFile[k] = v
				fileIds = append(fileIds, int(k))
			}

			// active file
			activeFile, err := db.getActiveFile(dType)
			if err != nil {
				log.Fatalf("active file is nil, the db can not open.[%+v]", err)
				return
			}
			dbFile[activeFile.Id] = activeFile
			fileIds = append(fileIds, int(activeFile.Id))

			// load the db files in a specified order.
			sort.Ints(fileIds)
			for i := 0; i < len(fileIds); i++ {
				fid := uint32(fileIds[i])
				df := dbFile[fid]
				var offset int64 = 0

				for offset <= db.config.BlockSize {
					if e, err := df.Read(offset); err == nil {
						idx := &index.Indexer{
							Meta:   e.Meta,
							FileId: fid,
							Offset: offset,
						}
						offset += int64(e.Size())

						if len(e.Meta.Key) > 0 {
							if err := db.buildIndex(e, idx, true); err != nil {
								log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
							}

							// save active tx ids.
							if i == len(fileIds)-1 && e.TxId != 0 {
								db.txnMeta.ActiveTxIds.Store(e.TxId, struct{}{})
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

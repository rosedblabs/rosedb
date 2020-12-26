package rosedb

import (
	"io"
	"rosedb/ds/list"
	"rosedb/index"
	"sort"
	"strconv"
	"strings"
)

//数据类型定义
type DataType = uint16

const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

//字符串相关操作标识
const (
	StringSet uint16 = iota
)

//列表相关操作标识
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

//建立列表索引
func (db *RoseDB) buildListIndex(idx *index.Indexer, opt uint16) {
	if db.listIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt {
	case ListLPush:
		db.listIndex.LPush(key, idx.Meta.Value)
	case ListLPop:
		db.listIndex.LPop(key)
	case ListRPush:
		db.listIndex.RPush(key, idx.Meta.Value)
	case ListRPop:
		db.listIndex.RPop(key)
	case ListLRem:
		if count, err := strconv.Atoi(string(idx.Meta.Extra)); err == nil {
			db.listIndex.LRem(key, idx.Meta.Value, count)
		}
	case ListLInsert:
		extra := string(idx.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			pivot := []byte(s[0])
			if opt, err := strconv.Atoi(s[1]); err == nil {
				db.listIndex.LInsert(string(idx.Meta.Key), list.InsertOption(opt), pivot, idx.Meta.Value)
			}
		}
	case ListLSet:
		if i, err := strconv.Atoi(string(idx.Meta.Extra)); err == nil {
			db.listIndex.LSet(key, i, idx.Meta.Value)
		}
	case ListLTrim:
		extra := string(idx.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			start, _ := strconv.Atoi(s[0])
			end, _ := strconv.Atoi(s[1])

			db.listIndex.LTrim(string(idx.Meta.Key), start, end)
		}
	}
}

//从文件中加载List、Set、Hash、ZSet索引
func (db *RoseDB) loadIdxFromFiles() error {
	if db.archFiles == nil && db.activeFile == nil {
		return nil
	}

	var fileIds []int
	dbFile := make(ArchivedFiles)
	for k, v := range db.archFiles {
		dbFile[k] = v
		fileIds = append(fileIds, int(k))
	}

	dbFile[db.activeFileId] = db.activeFile
	fileIds = append(fileIds, int(db.activeFileId))

	sort.Ints(fileIds)
	for i := 0; i < len(fileIds); i++ {
		fid := uint32(fileIds[i])
		df := dbFile[fid]
		var offset int64 = 0

		for {
			if e, err := df.Read(offset); err == nil {
				idx := &index.Indexer{
					Meta:      e.Meta,
					FileId:    fid,
					EntrySize: e.Size(),
					Offset:    offset,
				}
				offset += int64(e.Size())

				if err := db.buildIndex(e, idx); err != nil {
					return err
				}
			} else {
				if err == io.EOF {
					break
				}

				return err
			}
		}
	}

	return nil
}

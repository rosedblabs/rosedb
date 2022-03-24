package rosedb

import (
	"github.com/flower-corp/rosedb/ds/list"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"github.com/flower-corp/rosedb/util"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DataType Define the data structure type.
type DataType = int8

// Five different data types, support String, List, Hash, Set, Sorted Set right now.
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

func (db *RoseDB) buildIndex(dataType DataType, ent *logfile.LogEntry, pos *valuePos) {
	switch dataType {
	case String:
		db.buildStrsIndex(ent, pos)
	case List:
		db.buildListIndex(ent)
	case Hash:
		db.buildHashIndex(ent)
	case Set:
		db.buildSetIndex(ent)
	case ZSet:
		db.buildZSetIndex(ent)
	}
}

func (db *RoseDB) buildStrsIndex(ent *logfile.LogEntry, pos *valuePos) {
	ts := time.Now().Unix()
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		db.strIndex.idxTree.Delete(ent.Key)
		return
	}
	idxNode := &strIndexNode{
		fid:    pos.fid,
		offset: pos.offset,
	}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	db.strIndex.idxTree.Put(ent.Key, idxNode)
}

func (db *RoseDB) buildListIndex(ent *logfile.LogEntry) {
	key, cmd := list.DecodeCommandKey(ent.Key)
	switch cmd {
	case list.LPush:
		db.listIndex.indexes.LPush(key, ent.Value)
	case list.RPush:
		db.listIndex.indexes.RPush(key, ent.Value)
	case list.LPop:
		db.listIndex.indexes.LPop(key)
	case list.RPop:
		db.listIndex.indexes.RPop(key)
	}
}

func (db *RoseDB) buildHashIndex(ent *logfile.LogEntry) {
	key, field := db.decodeKey(ent.Key)
	if ent.Type == logfile.TypeDelete {
		db.hashIndex.indexes.HDel(string(key), string(field))
		return
	}
	db.hashIndex.indexes.HSet(string(key), string(field), ent.Value)
}

func (db *RoseDB) buildSetIndex(ent *logfile.LogEntry) {
	if ent.Type == logfile.TypeDelete {
		db.setIndex.indexes.SRem(string(ent.Key), ent.Value)
		return
	}
	db.setIndex.indexes.SAdd(string(ent.Key), ent.Value)
}

func (db *RoseDB) buildZSetIndex(ent *logfile.LogEntry) {
	if ent.Type == logfile.TypeDelete {
		db.zsetIndex.indexes.ZRem(string(ent.Key), string(ent.Value))
		return
	}

	key, scoreBuf := db.decodeKey(ent.Key)
	score, _ := util.StrToFloat64(string(scoreBuf))
	db.zsetIndex.indexes.ZAdd(string(key), score, string(ent.Value))
}

func (db *RoseDB) loadIndexFromLogFiles() error {
	iterateAndHandle := func(dataType DataType, wg *sync.WaitGroup) {
		defer wg.Done()

		fids := db.fidMap[dataType]
		if len(fids) == 0 {
			return
		}
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})

		for i, fid := range fids {
			var logFile *logfile.LogFile
			if i == len(fids)-1 {
				logFile = db.activeLogFiles[dataType]
			} else {
				logFile = db.archivedLogFiles[dataType][fid]
			}
			if logFile == nil {
				logger.Fatalf("log file is nil, failed to open db")
			}

			var offset int64
			for {
				entry, esize, err := logFile.ReadLogEntry(offset)
				if err != nil {
					if err == io.EOF || err == logfile.ErrEndOfEntry {
						break
					}
					logger.Fatalf("read log entry from file err, failed to open db")
				}
				pos := &valuePos{fid: fid, offset: offset}
				db.buildIndex(dataType, entry, pos)
				offset += esize
			}
			// set latest log file`s WriteAt.
			if i == len(fids)-1 {
				atomic.StoreInt64(&logFile.WriteAt, offset)
			}
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(logFileTypeNum)
	for i := 0; i < logFileTypeNum; i++ {
		go iterateAndHandle(DataType(i), wg)
	}
	wg.Wait()
	return nil
}

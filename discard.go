package rosedb

import (
	"encoding/binary"
	"errors"
	"io"
	"path/filepath"
	"sort"
	"sync"

	"github.com/flower-corp/rosedb/ioselector"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
)

const (
	discardRecordSize = 12
	// 8kb, contains mostly 682 records in file.
	discardFileSize int64 = 2 << 12
	discardFileName       = "DISCARD"
)

// ErrDiscardNoSpace no enough space for discard file.
var ErrDiscardNoSpace = errors.New("not enough space can be allocated for the discard file")

// Discard is used to record total size and discarded size in a log file.
// Mainly for log files compaction.
type discard struct {
	sync.Mutex
	valChan  chan *strIndexNode
	file     ioselector.IOSelector
	freeList []int64          // contains file offset that can be allocated
	location map[uint32]int64 // offset of each fid
}

func newDiscard(path, name string) (*discard, error) {
	fname := filepath.Join(path, name)
	file, err := ioselector.NewMMapSelector(fname, discardFileSize)
	if err != nil {
		return nil, err
	}

	var freeList []int64
	var offset int64
	location := make(map[uint32]int64)
	for {
		// read fid and total is enough.
		buf := make([]byte, 8)
		if _, err := file.Read(buf, offset); err != nil {
			if err == io.EOF || err == logfile.ErrEndOfEntry {
				break
			}
			return nil, err
		}
		fid := binary.LittleEndian.Uint32(buf[:4])
		total := binary.LittleEndian.Uint32(buf[4:8])
		if fid == 0 && total == 0 {
			freeList = append(freeList, offset)
		} else {
			location[fid] = offset
		}
		offset += discardRecordSize
	}

	d := &discard{
		valChan:  make(chan *strIndexNode, 2048),
		file:     file,
		freeList: freeList,
		location: location,
	}
	go d.listenUpdates()
	return d, nil
}

// CCL means compaction cnadidate list.
// iterate and find the file with most discarded data,
// there are 682 records at most, no need to worry about the performance.
func (d *discard) getCCL(activeFid uint32, ratio float64) ([]uint32, error) {
	var offset int64
	var ccl []uint32
	d.Lock()
	defer d.Unlock()
	for {
		buf := make([]byte, discardRecordSize)
		_, err := d.file.Read(buf, offset)
		if err != nil {
			if err == io.EOF || err == logfile.ErrEndOfEntry {
				break
			}
			return nil, err
		}
		offset += discardRecordSize

		fid := binary.LittleEndian.Uint32(buf[:4])
		total := binary.LittleEndian.Uint32(buf[4:8])
		discard := binary.LittleEndian.Uint32(buf[8:12])
		var curRatio float64
		if total != 0 && discard != 0 {
			curRatio = float64(discard) / float64(total)
		}
		if curRatio >= ratio && fid != activeFid {
			ccl = append(ccl, fid)
		}
	}

	// sort in ascending order, guarantee the older file will compact firstly.
	sort.Slice(ccl, func(i, j int) bool {
		return ccl[i] < ccl[j]
	})
	return ccl, nil
}

func (d *discard) listenUpdates() {
	for {
		select {
		case idxNode := <-d.valChan:
			d.incrDiscard(idxNode.fid, idxNode.entrySize)
		}
	}
}

func (d *discard) setTotal(fid uint32, totalSize uint32) {
	d.Lock()
	defer d.Unlock()

	if _, ok := d.location[fid]; ok {
		return
	}
	offset, err := d.alloc(fid)
	if err != nil {
		logger.Errorf("discard file allocate err: %+v", err)
		return
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], fid)
	binary.LittleEndian.PutUint32(buf[4:8], totalSize)
	if _, err = d.file.Write(buf, offset); err != nil {
		logger.Errorf("incr value in discard err: %v", err)
		return
	}
}

func (d *discard) clear(fid uint32) {
	d.incr(fid, -1)
	d.Lock()
	if offset, ok := d.location[fid]; ok {
		d.freeList = append(d.freeList, offset)
		delete(d.location, fid)
	}
	d.Unlock()
}

func (d *discard) incrDiscard(fid uint32, delta int) {
	if delta > 0 {
		d.incr(fid, delta)
	}
}

// format of discard file` record:
// +-------+--------------+----------------+  +-------+--------------+----------------+
// |  fid  |  total size  | discarded size |  |  fid  |  total size  | discarded size |
// +-------+--------------+----------------+  +-------+--------------+----------------+
// 0-------4--------------8---------------12  12------16------------20----------------24
func (d *discard) incr(fid uint32, delta int) {
	d.Lock()
	defer d.Unlock()

	offset, err := d.alloc(fid)
	if err != nil {
		logger.Errorf("discard file allocate err: %+v", err)
		return
	}

	var buf []byte
	if delta > 0 {
		buf = make([]byte, 4)
		offset += 8
		if _, err := d.file.Read(buf, offset); err != nil {
			logger.Errorf("incr value in discard err:%v", err)
			return
		}

		v := binary.LittleEndian.Uint32(buf)
		binary.LittleEndian.PutUint32(buf, v+uint32(delta))
	} else {
		buf = make([]byte, discardRecordSize)
	}

	if _, err := d.file.Write(buf, offset); err != nil {
		logger.Errorf("incr value in discard err:%v", err)
		return
	}
}

// must hold the lock before invoking
func (d *discard) alloc(fid uint32) (int64, error) {
	if offset, ok := d.location[fid]; ok {
		return offset, nil
	}
	if len(d.freeList) == 0 {
		return 0, ErrDiscardNoSpace
	}

	offset := d.freeList[len(d.freeList)-1]
	d.freeList = d.freeList[:len(d.freeList)-1]
	d.location[fid] = offset
	return offset, nil
}

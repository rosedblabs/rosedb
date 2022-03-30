package rosedb

import (
	"encoding/binary"
	"github.com/flower-corp/rosedb/ioselector"
	"github.com/flower-corp/rosedb/logfile"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func (db *RoseDB) loadDumpState() error {
	fileName := filepath.Join(db.opts.DBPath, dumpStateFile)
	dumpStateFile, err := ioselector.NewMMapSelector(fileName, 64)
	if err != nil {
		return err
	}
	db.dumpState = dumpStateFile

	dumpPath := filepath.Join(db.opts.DBPath, dumpFilePath)
	fileInfos, err := ioutil.ReadDir(dumpPath)
	defer func() {
		_ = os.RemoveAll(dumpPath)
	}()
	if err != nil || len(fileInfos) == 0 {
		if os.IsNotExist(err) {
			err = nil
		}
		return err
	}

	type fileInfo struct {
		fid  uint32
		name string
	}
	originalInfos, err := ioutil.ReadDir(db.opts.DBPath)
	if err != nil {
		return err
	}
	filesMap := make(map[DataType][]*fileInfo)
	for _, file := range originalInfos {
		if strings.HasPrefix(file.Name(), logfile.FilePrefix) {
			splitNames := strings.Split(file.Name(), ".")
			fid, err := strconv.Atoi(splitNames[2])
			if err != nil {
				return err
			}
			typ := DataType(logfile.FileTypesMap[splitNames[1]])
			filesMap[typ] = append(filesMap[typ], &fileInfo{fid: uint32(fid), name: file.Name()})
		}
	}

	for dType := List; dType < logFileTypeNum; dType++ {
		buf := make([]byte, dumpRecordSize)
		if _, err := dumpStateFile.Read(buf, int64((dType-1)*dumpRecordSize)); err != nil {
			return err
		}
		startFid := binary.LittleEndian.Uint32(buf[:4])
		endFid := binary.LittleEndian.Uint32(buf[4:8])
		finished := binary.LittleEndian.Uint32(buf[8:])
		// if dump finished sucessfully, remove the old log files.
		if finished == 1 {
			for _, fileInfo := range filesMap[dType] {
				if fileInfo.fid >= startFid && fileInfo.fid <= endFid {
					path := filepath.Join(db.opts.DBPath, fileInfo.name)
					if err := os.Remove(path); err != nil {
						return err
					}
				}
			}

			fileType := logfile.FileType(dType)
			// move dumped files to the db path
			for _, file := range fileInfos {
				oldPath := filepath.Join(dumpPath, file.Name())
				newPath := filepath.Join(db.opts.DBPath, file.Name())
				if strings.HasPrefix(file.Name(), logfile.FileNamesMap[fileType]) {
					if err := os.Rename(oldPath, newPath); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (db *RoseDB) markDumpStart(dataType DataType, startFid, endFid uint32) error {
	buf := make([]byte, dumpRecordSize)
	binary.LittleEndian.PutUint32(buf[:4], startFid)
	binary.LittleEndian.PutUint32(buf[4:8], endFid)
	binary.LittleEndian.PutUint32(buf[8:], 0)
	_, err := db.dumpState.Write(buf, int64((dataType-1)*dumpRecordSize))
	_ = db.dumpState.Sync()
	return err
}

func (db *RoseDB) markDumpFinish(dataType DataType) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf[:], 1)
	_, err := db.dumpState.Write(buf, int64((dataType-1)*dumpRecordSize+8))
	_ = db.dumpState.Sync()
	return err
}

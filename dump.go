package rosedb

import "encoding/binary"

func (db *RoseDB) markDumpStart(dataType DataType, startFid, endFid uint32) error {
	buf := make([]byte, dumpRecordSize)
	binary.LittleEndian.PutUint32(buf[:4], startFid)
	binary.LittleEndian.PutUint32(buf[4:8], endFid)
	binary.LittleEndian.PutUint32(buf[8:], 0)
	_, err := db.dumpState.Write(buf, int64((dataType-1)*dumpRecordSize))
	return err
}

func (db *RoseDB) markDumpFinish(dataType DataType) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf[:], 1)
	_, err := db.dumpState.Write(buf, int64((dataType-1)*dumpRecordSize+8))
	return err
}

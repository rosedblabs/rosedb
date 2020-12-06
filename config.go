package rosedb

import (
	"os"
	"rosedb/storage"
)

//数据索引的模式
type DataIndexMode int

const (
	//键和值均存于内存中的模式
	KeyValueRamMode DataIndexMode = iota

	//只有键存于内存中的模式
	KeyOnlyRamMode
)

const (
	//默认数据块文件大小：16MB
	DefaultBlockSize = 16 * 1024 * 1024

	//默认的key最大值 128字节
	DefaultMaxKeySize = uint32(128)

	//默认的value最大值 1MB
	DefaultMaxValueSize = uint32(1 * 1024 * 1024)
)

//数据库配置
type Config struct {
	dirPath      string               //数据库数据存储目录
	blockSize    int64                //每个数据块文件的大小
	rwMethod     storage.FileRWMethod //数据读写模式
	idxMode      DataIndexMode        //数据索引模式
	MaxKeySize   uint32
	MaxValueSize uint32
	Sync         bool //数据是否持久化
}

//获取默认配置
func DefaultConfig() *Config {
	return &Config{
		dirPath:      os.TempDir(),
		blockSize:    DefaultBlockSize,
		rwMethod:     storage.FileIO,
		idxMode:      KeyValueRamMode,
		MaxKeySize:   DefaultMaxKeySize,
		MaxValueSize: DefaultMaxValueSize,
		Sync:         true,
	}
}

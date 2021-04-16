package rosedb

import (
	"rosedb/storage"
)

// DataIndexMode 数据索引的模式
type DataIndexMode int

const (
	// KeyValueRamMode 键和值均存于内存中的模式
	KeyValueRamMode DataIndexMode = iota

	// KeyOnlyRamMode 只有键存于内存中的模式
	KeyOnlyRamMode
)

const (
	// DefaultAddr 默认服务器地址
	DefaultAddr = "127.0.0.1:5200"

	// DefaultDirPath 默认数据库目录
	DefaultDirPath = "/tmp/rosedb_server"

	// DefaultBlockSize 默认数据块文件大小：16MB
	DefaultBlockSize = 16 * 1024 * 1024

	// DefaultMaxKeySize 默认的key最大值 128字节
	DefaultMaxKeySize = uint32(128)

	// DefaultMaxValueSize 默认的value最大值 1MB
	DefaultMaxValueSize = uint32(1 * 1024 * 1024)

	// DefaultReclaimThreshold 默认回收磁盘空间的阈值，当已封存文件个数到达 4 时，可进行回收
	DefaultReclaimThreshold = 4
)

// Config 数据库配置
type Config struct {
	Addr             string               `json:"addr" toml:"addr"`             //服务器地址
	DirPath          string               `json:"dir_path" toml:"dir_path"`     //数据库数据存储目录
	BlockSize        int64                `json:"block_size" toml:"block_size"` //每个数据块文件的大小
	RwMethod         storage.FileRWMethod `json:"rw_method" toml:"rw_method"`   //数据读写模式
	IdxMode          DataIndexMode        `json:"idx_mode" toml:"idx_mode"`     //数据索引模式
	MaxKeySize       uint32               `json:"max_key_size" toml:"max_key_size"`
	MaxValueSize     uint32               `json:"max_value_size" toml:"max_value_size"`
	Sync             bool                 `json:"sync" toml:"sync"`                           //每次写数据是否持久化
	ReclaimThreshold int                  `json:"reclaim_threshold" toml:"reclaim_threshold"` //回收磁盘空间的阈值
}

// DefaultConfig 获取默认配置
func DefaultConfig() Config {
	return Config{
		Addr:             DefaultAddr,
		DirPath:          DefaultDirPath,
		BlockSize:        DefaultBlockSize,
		RwMethod:         storage.FileIO,
		IdxMode:          KeyValueRamMode,
		MaxKeySize:       DefaultMaxKeySize,
		MaxValueSize:     DefaultMaxValueSize,
		Sync:             false,
		ReclaimThreshold: DefaultReclaimThreshold,
	}
}

package rosedb

import (
	"github.com/roseduan/rosedb/storage"
	"time"
)

// DataIndexMode the data index mode.
type DataIndexMode int

const (
	// KeyValueMemMode key and value are both in memory, read operation will be very fast in this mode.
	// Because there is no disk seek, just get value from the corresponding data structures in memory.
	// This mode is suitable for scenarios where the value are relatively small.
	KeyValueMemMode DataIndexMode = iota

	// KeyOnlyMemMode only key in memory, there is a disk seek while getting a value.
	// Because values are in db file on disk.
	KeyOnlyMemMode
)

const (
	// DefaultAddr default rosedb server address and port.
	DefaultAddr = "127.0.0.1:5200"

	// defaultGrpcAddr server address and port.
	DefaultGrpcAddr = "127.0.0.1:5221"

	// DefaultDirPath default rosedb data dir.
	DefaultDirPath = "/tmp/rosedb_server"

	// DefaultBlockSize default db file size: 16mb.
	// If reach the size, db file will never be opened for writing.
	DefaultBlockSize = 16 * 1024 * 1024

	// DefaultMaxKeySize default max key size: 1mb.
	DefaultMaxKeySize = uint32(1 * 1024 * 1024)

	// DefaultMaxValueSize default max value size: 8mb.
	DefaultMaxValueSize = uint32(8 * 1024 * 1024)

	// DefaultMergeThreshold default disk files reclaim threshold: 64.
	// This means that it will be reclaimed when there are at least 64 archived files on disk.
	DefaultMergeThreshold = 64

	// DefaultMergeCheckInterval a timer will be set according to the check interval.
	// Then merge operation will execute periodically.
	DefaultMergeCheckInterval = time.Hour * 24
)

// Config the opening options of rosedb.
type Config struct {
	Addr     string `json:"addr" toml:"addr"`           // server address
	GrpcAddr string `json:"grpc_addr" toml:"grpc_addr"` // grpc server address
	DirPath  string `json:"dir_path" toml:"dir_path"`   // rosedb dir path of db file
	// Deprecated: don`t edit the option, it will be removed in future release.
	BlockSize    int64                `json:"block_size" toml:"block_size"` // each db file size
	RwMethod     storage.FileRWMethod `json:"rw_method" toml:"rw_method"`   // db file read and write method
	IdxMode      DataIndexMode        `json:"idx_mode" toml:"idx_mode"`     // data index mode
	MaxKeySize   uint32               `json:"max_key_size" toml:"max_key_size"`
	MaxValueSize uint32               `json:"max_value_size" toml:"max_value_size"`

	// Sync is whether to sync writes from the OS buffer cache through to actual disk.
	// If false, and the machine crashes, then some recent writes may be lost.
	//
	// Note that if it is just the process that crashes (and the machine does not) then no writes will be lost.
	//
	// The default value is false.
	Sync bool `json:"sync" toml:"sync"`

	MergeThreshold int `json:"merge_threshold" toml:"merge_threshold"` // threshold to reclaim disk

	MergeCheckInterval time.Duration `json:"merge_check_interval"`
}

// DefaultConfig get the default config.
func DefaultConfig() Config {
	return Config{
		Addr:               DefaultAddr,
		GrpcAddr:           DefaultGrpcAddr,
		DirPath:            DefaultDirPath,
		BlockSize:          DefaultBlockSize,
		RwMethod:           storage.FileIO,
		IdxMode:            KeyValueMemMode,
		MaxKeySize:         DefaultMaxKeySize,
		MaxValueSize:       DefaultMaxValueSize,
		Sync:               false,
		MergeThreshold:     DefaultMergeThreshold,
		MergeCheckInterval: DefaultMergeCheckInterval,
	}
}

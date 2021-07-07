package rosedb

import (
	"github.com/roseduan/rosedb/storage"
)

// DataIndexMode the data index mode.
type DataIndexMode int

const (
	// KeyValueMemMode key and value are both in memory, read operation will be very fast in this mode.
	// Because there is no disk seek, just get value from the corresponding data structures in memory.
	// This mode is suitable for scenarios where the value are relatively small.
	KeyValueMemMode DataIndexMode = iota

	// KeyOnlyMemMode only key in memory, there is a disk seek while getting a value.
	// Because the value is in db file.
	KeyOnlyMemMode
)

const (
	// DefaultAddr default rosedb server address and port.
	DefaultAddr = "127.0.0.1:5200"

	// DefaultDirPath default rosedb data dir.
	DefaultDirPath = "/tmp/rosedb_server"

	// DefaultBlockSize default db file size: 16mb.
	// If reach the size, db file will never opening for writing.
	DefaultBlockSize = 16 * 1024 * 1024

	// DefaultMaxKeySize default max key size: 128 bytes.
	DefaultMaxKeySize = uint32(128)

	// DefaultMaxValueSize default max value size: 1mb.
	DefaultMaxValueSize = uint32(1 * 1024 * 1024)

	// DefaultReclaimThreshold default disk reclaim threshold: at least 4 archived db files.
	DefaultReclaimThreshold = 4

	// DefaultSingleReclaimThreshold single reclaimable space: at least 4MB space of a db file.
	// when a single db file`s reclaimable space reached the threshold, it can be reclaimed automatically.
	// Only support String type now.
	DefaultSingleReclaimThreshold = 4 * 1024 * 1024
)

// Config the config options of rosedb.
type Config struct {
	Addr                   string               `json:"addr" toml:"addr"`             // server address
	DirPath                string               `json:"dir_path" toml:"dir_path"`     // rosedb dir path of db file
	BlockSize              int64                `json:"block_size" toml:"block_size"` // each db file size
	RwMethod               storage.FileRWMethod `json:"rw_method" toml:"rw_method"`   // db file read and write method
	IdxMode                DataIndexMode        `json:"idx_mode" toml:"idx_mode"`     // data index mode
	MaxKeySize             uint32               `json:"max_key_size" toml:"max_key_size"`
	MaxValueSize           uint32               `json:"max_value_size" toml:"max_value_size"`
	Sync                   bool                 `json:"sync" toml:"sync"`                           // sync to disk if necessary
	ReclaimThreshold       int                  `json:"reclaim_threshold" toml:"reclaim_threshold"` // threshold to reclaim disk
	SingleReclaimThreshold int64                `json:"single_reclaim_threshold"`                   // single reclaim threshold
}

// DefaultConfig get the default config.
func DefaultConfig() Config {
	return Config{
		Addr:                   DefaultAddr,
		DirPath:                DefaultDirPath,
		BlockSize:              DefaultBlockSize,
		RwMethod:               storage.FileIO,
		IdxMode:                KeyValueMemMode,
		MaxKeySize:             DefaultMaxKeySize,
		MaxValueSize:           DefaultMaxValueSize,
		Sync:                   false,
		ReclaimThreshold:       DefaultReclaimThreshold,
		SingleReclaimThreshold: DefaultSingleReclaimThreshold,
	}
}

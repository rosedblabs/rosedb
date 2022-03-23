package rosedb

// DataIndexMode the data index mode.
type DataIndexMode int

const (
	// KeyValueMemMode key and value are both in memory, read operation will be very fast in this mode.
	// Because there is no disk seek, just get value from the corresponding data structures in memory.
	// This mode is suitable for scenarios where the value are relatively small.
	KeyValueMemMode DataIndexMode = iota

	// KeyOnlyMemMode only key in memory, there is a disk seek while getting a value.
	// Because values are in log file on disk.
	KeyOnlyMemMode
)

// IOType represents different types of file io: FileIO(standard file io) and MMap(Memory Map).
type IOType int8

const (
	// FileIO standard file io.
	FileIO IOType = iota
	// MMap Memory Map.
	MMap
)

// Options for opening a db.
type Options struct {
	// DBPath db path, will be created automatically if not exist.
	DBPath string

	// IndexMode mode of index, support KeyValueMemMode and KeyOnlyMemMode now.
	// Note that this mode is only for kv pairs, not List, Hash, Set, and ZSet.
	// Default value is KeyOnlyMemMode.
	IndexMode DataIndexMode

	// IoType file r/w io type, support FileIO and MMap now.
	// Default value is FileIO.
	IoType IOType

	// Sync is whether to sync writes from the OS buffer cache through to actual disk.
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (and the machine does not) then no writes will be lost.
	// Default value is false.
	Sync bool `json:"sync" toml:"sync"`
}

// DefaultOptions default options for opening a RoseDB.
func DefaultOptions(path string) Options {
	return Options{
		DBPath:    path,
		IndexMode: KeyOnlyMemMode,
		IoType:    FileIO,
		Sync:      false,
	}
}

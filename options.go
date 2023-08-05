package rosedb

import "os"

// Options specifies the options for opening a database.
type Options struct {
	// DirPath specifies the directory path where the WAL segment files will be stored.
	DirPath string

	// SegmentSize specifies the maximum size of each segment file in bytes.
	SegmentSize int64

	// BlockCache specifies the size of the block cache in number of bytes.
	// A block cache is used to store recently accessed data blocks, improving read performance.
	// If BlockCache is set to 0, no block cache will be used.
	BlockCache uint32

	// Sync is whether to synchronize writes through os buffer cache and down onto the actual disk.
	// Setting sync is required for durability of a single write operation, but also results in slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (machine does not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.
	Sync bool

	// BytesPerSync specifies the number of bytes to write before calling fsync.
	BytesPerSync uint32

	// WatchQueueSize the cache length of the watch queue.
	// if the size greater than 0, which means enable the watch.
	WatchQueueSize uint64
}

// BatchOptions specifies the options for creating a batch.
type BatchOptions struct {
	// Sync has the same semantics as Options.Sync.
	Sync bool
	// ReadOnly specifies whether the batch is read only.
	ReadOnly bool
}

// IteratorOptions is the options for the iterator.
type IteratorOptions struct {
	// Prefix filters the keys by prefix.
	Prefix []byte

	// Reverse indicates whether the iterator is reversed.
	// false is forward, true is backward.
	Reverse bool
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:        tempDBDir(),
	SegmentSize:    1 * GB,
	BlockCache:     0,
	Sync:           false,
	BytesPerSync:   0,
	WatchQueueSize: 0,
}

var DefaultBatchOptions = BatchOptions{
	Sync:     true,
	ReadOnly: false,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

func tempDBDir() string {
	dir, _ := os.MkdirTemp("", "rosedb-temp")
	return dir
}

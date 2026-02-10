package bptree

// Options represents the configuration options for B+Tree.
type Options struct {
	// PageSize is the size of each page in bytes.
	// Default is 4KB, must be a power of 2.
	PageSize uint32

	// Order is the maximum number of children per internal node.
	// For leaf nodes, it's the maximum number of key-value pairs.
	// Default is calculated based on PageSize.
	Order int

	// CacheSize is the maximum number of pages to cache in memory.
	// Default is 1000 pages.
	CacheSize int

	// SyncWrites determines whether to sync writes to disk immediately.
	// Default is false for better performance.
	SyncWrites bool
}

// DefaultOptions returns the default options for B+Tree.
var DefaultOptions = Options{
	PageSize:   4096,
	Order:      0, // will be calculated based on PageSize
	CacheSize:  1000,
	SyncWrites: false,
}

// calculateOrder calculates the order based on page size.
// For leaf nodes: each entry = key_len(4) + key(avg 64) + value(20) = ~88 bytes
// For internal nodes: each entry = key_len(4) + key(avg 64) + child_ptr(4) = ~72 bytes
// We use a conservative estimate to ensure nodes fit in a page.
func calculateOrder(pageSize uint32) int {
	// Reserve space for node header (32 bytes) and some padding
	usableSize := int(pageSize) - nodeHeaderSize
	// Average entry size estimate
	avgEntrySize := 100
	order := usableSize / avgEntrySize
	if order < 4 {
		order = 4 // minimum order
	}
	return order
}

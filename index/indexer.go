package index

import (
	"github.com/roseduan/rosedb/storage"
)

// Indexer the data index info, stored in skip list.
type Indexer struct {
	Meta      *storage.Meta // metadata info.
	FileId    uint32        // the file id of storing the data.
	EntrySize uint32        // the size of entry.
	Offset    int64         // entry data query start position.
}

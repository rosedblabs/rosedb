package index

import "github.com/rosedblabs/wal"

type ChunkPositionWrapper struct {
	pos *wal.ChunkPosition
	deleted bool
}
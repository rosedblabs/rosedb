package bptree

import (
	"encoding/binary"
	"errors"

	"github.com/rosedblabs/wal"
)

const (
	// Magic number for identifying B+Tree index files
	MagicNumber uint32 = 0x42505449 // "BPTI"

	// Version of the B+Tree format
	Version uint16 = 1

	// Page types
	PageTypeMeta     uint8 = 0
	PageTypeInternal uint8 = 1
	PageTypeLeaf     uint8 = 2
	PageTypeFreeList uint8 = 3

	// Size constants
	metaPageSize   = 64
	nodeHeaderSize = 32

	// Invalid page ID
	InvalidPageID uint32 = 0
)

var (
	ErrInvalidMagic   = errors.New("invalid magic number")
	ErrInvalidVersion = errors.New("unsupported version")
	ErrInvalidPage    = errors.New("invalid page")
	ErrKeyTooLarge    = errors.New("key is too large")
	ErrKeyNotFound    = errors.New("key not found")
)

// MetaPage represents the metadata page of the B+Tree.
// Layout (64 bytes):
// +--------+--------+--------+--------+--------+--------+--------+--------+
// | Magic(4)| Ver(2)| PgSize(4)| Root(4)| FreeList(4)| KeyCount(8)| ...   |
// +--------+--------+--------+--------+--------+--------+--------+--------+
type MetaPage struct {
	Magic        uint32
	Version      uint16
	PageSize     uint32
	RootPageID   uint32
	FreeListPage uint32
	KeyCount     uint64
	PageCount    uint32
}

// Serialize serializes the meta page to bytes.
func (m *MetaPage) Serialize() []byte {
	buf := make([]byte, metaPageSize)
	binary.LittleEndian.PutUint32(buf[0:4], m.Magic)
	binary.LittleEndian.PutUint16(buf[4:6], m.Version)
	binary.LittleEndian.PutUint32(buf[6:10], m.PageSize)
	binary.LittleEndian.PutUint32(buf[10:14], m.RootPageID)
	binary.LittleEndian.PutUint32(buf[14:18], m.FreeListPage)
	binary.LittleEndian.PutUint64(buf[18:26], m.KeyCount)
	binary.LittleEndian.PutUint32(buf[26:30], m.PageCount)
	return buf
}

// DeserializeMetaPage deserializes bytes to a meta page.
func DeserializeMetaPage(buf []byte) (*MetaPage, error) {
	if len(buf) < metaPageSize {
		return nil, ErrInvalidPage
	}

	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != MagicNumber {
		return nil, ErrInvalidMagic
	}

	version := binary.LittleEndian.Uint16(buf[4:6])
	if version != Version {
		return nil, ErrInvalidVersion
	}

	return &MetaPage{
		Magic:        magic,
		Version:      version,
		PageSize:     binary.LittleEndian.Uint32(buf[6:10]),
		RootPageID:   binary.LittleEndian.Uint32(buf[10:14]),
		FreeListPage: binary.LittleEndian.Uint32(buf[14:18]),
		KeyCount:     binary.LittleEndian.Uint64(buf[18:26]),
		PageCount:    binary.LittleEndian.Uint32(buf[26:30]),
	}, nil
}

// Node represents a B+Tree node (internal or leaf).
// Layout:
// Header (32 bytes):
// +--------+--------+--------+--------+--------+--------+
// | Type(1)| KeyCnt(2)| Parent(4)| Next(4)| Prev(4)| ...  |
// +--------+--------+--------+--------+--------+--------+
// Body (variable):
// For leaf: [keyLen(2)|key|valLen(2)|value] ...
// For internal: [keyLen(2)|key|childPtr(4)] ... [lastChildPtr(4)]
type Node struct {
	PageID   uint32
	PageType uint8
	KeyCount uint16
	Parent   uint32
	Next     uint32 // for leaf nodes: next sibling
	Prev     uint32 // for leaf nodes: previous sibling

	Keys     [][]byte
	Values   []*wal.ChunkPosition // for leaf nodes
	Children []uint32             // for internal nodes

	dirty bool
}

// IsLeaf returns true if the node is a leaf node.
func (n *Node) IsLeaf() bool {
	return n.PageType == PageTypeLeaf
}

// Serialize serializes the node to bytes.
func (n *Node) Serialize(pageSize uint32) []byte {
	buf := make([]byte, pageSize)

	// Header
	buf[0] = n.PageType
	binary.LittleEndian.PutUint16(buf[1:3], n.KeyCount)
	binary.LittleEndian.PutUint32(buf[3:7], n.Parent)
	binary.LittleEndian.PutUint32(buf[7:11], n.Next)
	binary.LittleEndian.PutUint32(buf[11:15], n.Prev)

	offset := nodeHeaderSize

	if n.IsLeaf() {
		// Leaf node: serialize key-value pairs
		for i := 0; i < int(n.KeyCount); i++ {
			key := n.Keys[i]
			// key length
			binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(key)))
			offset += 2
			// key
			copy(buf[offset:], key)
			offset += len(key)
			// value (ChunkPosition)
			offset += serializeChunkPosition(buf[offset:], n.Values[i])
		}
	} else {
		// Internal node: serialize keys and children
		for i := 0; i < int(n.KeyCount); i++ {
			key := n.Keys[i]
			// key length
			binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(key)))
			offset += 2
			// key
			copy(buf[offset:], key)
			offset += len(key)
			// child pointer
			binary.LittleEndian.PutUint32(buf[offset:offset+4], n.Children[i])
			offset += 4
		}
		// last child pointer
		if len(n.Children) > int(n.KeyCount) {
			binary.LittleEndian.PutUint32(buf[offset:offset+4], n.Children[n.KeyCount])
		}
	}

	return buf
}

// DeserializeNode deserializes bytes to a node.
func DeserializeNode(pageID uint32, buf []byte) (*Node, error) {
	if len(buf) < nodeHeaderSize {
		return nil, ErrInvalidPage
	}

	n := &Node{
		PageID:   pageID,
		PageType: buf[0],
		KeyCount: binary.LittleEndian.Uint16(buf[1:3]),
		Parent:   binary.LittleEndian.Uint32(buf[3:7]),
		Next:     binary.LittleEndian.Uint32(buf[7:11]),
		Prev:     binary.LittleEndian.Uint32(buf[11:15]),
	}

	offset := nodeHeaderSize

	if n.IsLeaf() {
		n.Keys = make([][]byte, n.KeyCount)
		n.Values = make([]*wal.ChunkPosition, n.KeyCount)

		for i := 0; i < int(n.KeyCount); i++ {
			// key length
			keyLen := binary.LittleEndian.Uint16(buf[offset : offset+2])
			offset += 2
			// key
			n.Keys[i] = make([]byte, keyLen)
			copy(n.Keys[i], buf[offset:offset+int(keyLen)])
			offset += int(keyLen)
			// value
			var pos *wal.ChunkPosition
			pos, offset = deserializeChunkPosition(buf, offset)
			n.Values[i] = pos
		}
	} else {
		n.Keys = make([][]byte, n.KeyCount)
		n.Children = make([]uint32, n.KeyCount+1)

		for i := 0; i < int(n.KeyCount); i++ {
			// key length
			keyLen := binary.LittleEndian.Uint16(buf[offset : offset+2])
			offset += 2
			// key
			n.Keys[i] = make([]byte, keyLen)
			copy(n.Keys[i], buf[offset:offset+int(keyLen)])
			offset += int(keyLen)
			// child pointer
			n.Children[i] = binary.LittleEndian.Uint32(buf[offset : offset+4])
			offset += 4
		}
		// last child pointer
		n.Children[n.KeyCount] = binary.LittleEndian.Uint32(buf[offset : offset+4])
	}

	return n, nil
}

// ChunkPosition serialization size: SegmentId(4) + BlockNumber(4) + ChunkOffset(8) + ChunkSize(4) = 20 bytes
const chunkPositionSize = 20

func serializeChunkPosition(buf []byte, pos *wal.ChunkPosition) int {
	if pos == nil {
		// Write zeros for nil position
		for i := 0; i < chunkPositionSize; i++ {
			buf[i] = 0
		}
		return chunkPositionSize
	}
	binary.LittleEndian.PutUint32(buf[0:4], pos.SegmentId)
	binary.LittleEndian.PutUint32(buf[4:8], pos.BlockNumber)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(pos.ChunkOffset))
	binary.LittleEndian.PutUint32(buf[16:20], pos.ChunkSize)
	return chunkPositionSize
}

func deserializeChunkPosition(buf []byte, offset int) (*wal.ChunkPosition, int) {
	pos := &wal.ChunkPosition{
		SegmentId:   binary.LittleEndian.Uint32(buf[offset : offset+4]),
		BlockNumber: binary.LittleEndian.Uint32(buf[offset+4 : offset+8]),
		ChunkOffset: int64(binary.LittleEndian.Uint64(buf[offset+8 : offset+16])),
		ChunkSize:   binary.LittleEndian.Uint32(buf[offset+16 : offset+20]),
	}
	return pos, offset + chunkPositionSize
}

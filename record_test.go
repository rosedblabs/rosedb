package rosedb

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/rosedblabs/wal"
	"github.com/valyala/bytebufferpool"
)

func TestEncodeDecodeLogRecord(t *testing.T) {
	now := time.Now().Unix()
	cases := []struct {
		name string
		rec  *LogRecord
	}{
		{"normal", &LogRecord{Key: []byte("key"), Value: []byte("value"), Type: LogRecordNormal, BatchId: 42, Expire: now + 3600}},
		{"empty_key_value", &LogRecord{Key: []byte(""), Value: []byte(""), Type: LogRecordDeleted, BatchId: 0, Expire: 0}},
		{"large_ids", &LogRecord{Key: bytes.Repeat([]byte{'a'}, 512), Value: bytes.Repeat([]byte{'b'}, 1024), Type: LogRecordNormal, BatchId: 1 << 50, Expire: now}},
	}

	header := make([]byte, maxLogRecordHeaderSize)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			buf := bytebufferpool.Get()
			defer bytebufferpool.Put(buf)

			data := encodeLogRecord(c.rec, header, buf)
			if data == nil {
				t.Fatalf("encodeLogRecord returned nil")
			}

			got := decodeLogRecord(data)
			if got.Type != c.rec.Type {
				t.Fatalf("Type mismatch: got %v want %v", got.Type, c.rec.Type)
			}
			if got.BatchId != c.rec.BatchId {
				t.Fatalf("BatchId mismatch: got %v want %v", got.BatchId, c.rec.BatchId)
			}
			if got.Expire != c.rec.Expire {
				t.Fatalf("Expire mismatch: got %v want %v", got.Expire, c.rec.Expire)
			}
			if !bytes.Equal(got.Key, c.rec.Key) {
				t.Fatalf("Key mismatch: got %v want %v", got.Key, c.rec.Key)
			}
			if !bytes.Equal(got.Value, c.rec.Value) {
				t.Fatalf("Value mismatch: got %v want %v", got.Value, c.rec.Value)
			}
		})
	}
}

func TestEncodeDecodeHintRecord(t *testing.T) {
	pos := &wal.ChunkPosition{
		SegmentId:   wal.SegmentID(7),
		BlockNumber: 13,
		ChunkOffset: 1234567,
		ChunkSize:   4096,
	}
	key := []byte("hint-key")
	data := encodeHintRecord(key, pos)
	if len(data) == 0 {
		t.Fatal("encodeHintRecord returned empty slice")
	}
	gotKey, gotPos := decodeHintRecord(data)
	if !bytes.Equal(gotKey, key) {
		t.Fatalf("hint key mismatch: got %v want %v", gotKey, key)
	}
	if gotPos.SegmentId != pos.SegmentId || gotPos.BlockNumber != pos.BlockNumber || gotPos.ChunkOffset != pos.ChunkOffset || gotPos.ChunkSize != pos.ChunkSize {
		t.Fatalf("hint pos mismatch: got %+v want %+v", gotPos, pos)
	}
}

func TestEncodeMergeFinRecord(t *testing.T) {
	seg := wal.SegmentID(uint32(0xDEADBEEF))
	b := encodeMergeFinRecord(seg)
	if len(b) != 4 {
		t.Fatalf("encodeMergeFinRecord returned wrong length: %d", len(b))
	}
	val := binary.LittleEndian.Uint32(b)
	if uint32(seg) != val {
		t.Fatalf("segment id mismatch: got %x want %x", val, uint32(seg))
	}
}

func TestLogRecordIsExpired(t *testing.T) {
	now := time.Now().Unix()
	cases := []struct {
		name   string
		expire int64
		want   bool
	}{
		{"zero_not_expired", 0, false},
		{"past_expired", now - 1, true},
		{"future_not_expired", now + 1000, false},
		{"exact_now_expired", now, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lr := &LogRecord{Expire: c.expire}
			if got := lr.IsExpired(now); got != c.want {
				t.Fatalf("IsExpired(%d) = %v, want %v", c.expire, got, c.want)
			}
		})
	}
}

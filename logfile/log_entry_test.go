package logfile

import (
	"reflect"
	"testing"
)

func TestEncodeEntry(t *testing.T) {
	type args struct {
		e *LogEntry
	}
	tests := []struct {
		name  string
		args  args
		want  []byte
		want1 int
	}{
		{
			"nil", args{e: nil}, nil, 0,
		},
		{
			"no-fields", args{e: &LogEntry{}}, []byte{28, 223, 68, 33, 0, 0, 0, 0}, 8,
		},
		{
			"no-key-value", args{e: &LogEntry{ExpiredAt: 443434211}}, []byte{51, 97, 150, 123, 0, 0, 0, 198, 147, 242, 166, 3}, 12,
		},
		{
			"with-key-value", args{e: &LogEntry{Key: []byte("kv"), Value: []byte("lotusdb"), ExpiredAt: 443434211}}, []byte{101, 208, 223, 156, 0, 4, 14, 198, 147, 242, 166, 3, 107, 118, 108, 111, 116, 117, 115, 100, 98}, 21,
		},
		{
			"type-delete", args{e: &LogEntry{Key: []byte("kv"), Value: []byte("lotusdb"), ExpiredAt: 443434211, Type: TypeDelete}}, []byte{38, 27, 121, 27, 1, 4, 14, 198, 147, 242, 166, 3, 107, 118, 108, 111, 116, 117, 115, 100, 98}, 21,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := EncodeEntry(tt.args.e)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EncodeEntry() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("EncodeEntry() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_decodeHeader(t *testing.T) {
	type args struct {
		buf []byte
	}
	tests := []struct {
		name  string
		args  args
		want  *entryHeader
		want1 int64
	}{
		{
			"nil", args{buf: nil}, nil, 0,
		},
		{
			"no-enough-bytes", args{buf: []byte{1, 4, 3, 22}}, nil, 0,
		},
		{
			"no-fields", args{buf: []byte{28, 223, 68, 33, 0, 0, 0, 0}}, &entryHeader{crc32: 558161692}, 8,
		},
		{
			"normal", args{buf: []byte{101, 208, 223, 156, 0, 4, 14, 198, 147, 242, 166, 3}}, &entryHeader{crc32: 2631913573, typ: 0, kSize: 2, vSize: 7, expiredAt: 443434211}, 12,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := decodeHeader(tt.args.buf)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decodeHeader() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("decodeHeader() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_getEntryCrc(t *testing.T) {
	type args struct {
		e *LogEntry
		h []byte
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			"nil", args{e: nil, h: nil}, 0,
		},
		{
			"no-fields", args{e: &LogEntry{}, h: []byte{0, 0, 0, 0}}, 558161692,
		},
		{
			"normal", args{e: &LogEntry{Key: []byte("kv"), Value: []byte("lotusdb")}, h: []byte{0, 4, 14, 198, 147, 242, 166, 3}}, 2631913573,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getEntryCrc(tt.args.e, tt.args.h); got != tt.want {
				t.Errorf("getEntryCrc() = %v, want %v", got, tt.want)
			}
		})
	}
}

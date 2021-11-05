package storage

import (
	"github.com/roseduan/mmap-go"
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"testing"
)

func TestBuild(t *testing.T) {
	type args struct {
		path      string
		method    FileRWMethod
		blockSize int64
	}
	tests := []struct {
		name    string
		args    args
		want    map[uint16]map[uint32]*DBFile
		want1   map[uint16]uint32
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := Build(tt.args.path, tt.args.method, tt.args.blockSize)
			if (err != nil) != tt.wantErr {
				t.Errorf("Build() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Build() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("Build() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestDBFile_Close(t *testing.T) {
	type fields struct {
		Id     uint32
		Path   string
		File   *os.File
		mmap   mmap.MMap
		Offset int64
		method FileRWMethod
	}
	type args struct {
		sync bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DBFile{
				Id:     tt.fields.Id,
				Path:   tt.fields.Path,
				File:   tt.fields.File,
				mmap:   tt.fields.mmap,
				Offset: tt.fields.Offset,
				method: tt.fields.method,
			}
			if err := df.Close(tt.args.sync); (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDBFile_Read(t *testing.T) {
	type fields struct {
		Id     uint32
		Path   string
		File   *os.File
		mmap   mmap.MMap
		Offset int64
		method FileRWMethod
	}
	type args struct {
		offset int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantE   *Entry
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DBFile{
				Id:     tt.fields.Id,
				Path:   tt.fields.Path,
				File:   tt.fields.File,
				mmap:   tt.fields.mmap,
				Offset: tt.fields.Offset,
				method: tt.fields.method,
			}
			gotE, err := df.Read(tt.args.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotE, tt.wantE) {
				t.Errorf("Read() gotE = %v, want %v", gotE, tt.wantE)
			}
		})
	}
}

func TestDBFile_ReadAll(t *testing.T) {
	type fields struct {
		Id     uint32
		Path   string
		File   *os.File
		mmap   mmap.MMap
		Offset int64
		method FileRWMethod
	}
	tests := []struct {
		name    string
		fields  fields
		want    map[int64]*Entry
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DBFile{
				Id:     tt.fields.Id,
				Path:   tt.fields.Path,
				File:   tt.fields.File,
				mmap:   tt.fields.mmap,
				Offset: tt.fields.Offset,
				method: tt.fields.method,
			}
			got, err := df.ReadAll()
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadAll() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDBFile_Sync(t *testing.T) {
	type fields struct {
		Id     uint32
		Path   string
		File   *os.File
		mmap   mmap.MMap
		Offset int64
		method FileRWMethod
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DBFile{
				Id:     tt.fields.Id,
				Path:   tt.fields.Path,
				File:   tt.fields.File,
				mmap:   tt.fields.mmap,
				Offset: tt.fields.Offset,
				method: tt.fields.method,
			}
			if err := df.Sync(); (err != nil) != tt.wantErr {
				t.Errorf("Sync() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDBFile_Write(t *testing.T) {
	type fields struct {
		Id     uint32
		Path   string
		File   *os.File
		mmap   mmap.MMap
		Offset int64
		method FileRWMethod
	}

	type args struct {
		e *Entry
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DBFile{
				Id:     tt.fields.Id,
				Path:   tt.fields.Path,
				File:   tt.fields.File,
				mmap:   tt.fields.mmap,
				Offset: tt.fields.Offset,
				method: tt.fields.method,
			}
			if err := df.Write(tt.args.e); (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDBFile_readBuf(t *testing.T) {
	type fields struct {
		Id     uint32
		Path   string
		File   *os.File
		mmap   mmap.MMap
		Offset int64
		method FileRWMethod
	}
	type args struct {
		offset int64
		n      int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DBFile{
				Id:     tt.fields.Id,
				Path:   tt.fields.Path,
				File:   tt.fields.File,
				mmap:   tt.fields.mmap,
				Offset: tt.fields.Offset,
				method: tt.fields.method,
			}
			got, err := df.readBuf(tt.args.offset, tt.args.n)
			if (err != nil) != tt.wantErr {
				t.Errorf("readBuf() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readBuf() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewDBFile(t *testing.T) {
	path := "/tmp/rosedb"
	_ = os.MkdirAll("/tmp/rosedb", os.ModePerm)
	defer os.RemoveAll(path)

	type args struct {
		path      string
		fileId    uint32
		method    FileRWMethod
		blockSize int64
		eType     uint16
	}

	tests := []struct {
		name    string
		args    args
		want    *DBFile
		wantErr bool
	}{
		{"f-string", args{"/tmp/rosedb", 0, FileIO, 1 * 1024 * 1024, String}, nil, false},
		{"f-list", args{"/tmp/rosedb", 0, FileIO, 1 * 1024 * 1024, List}, nil, false},
		{"f-hash", args{"/tmp/rosedb", 0, FileIO, 1 * 1024 * 1024, Hash}, nil, false},
		{"f-set", args{"/tmp/rosedb", 0, FileIO, 1 * 1024 * 1024, Set}, nil, false},
		{"f-zset", args{"/tmp/rosedb", 0, FileIO, 1 * 1024 * 1024, ZSet}, nil, false},

		{"m-zset", args{"/tmp/rosedb", 1, MMap, 1 * 1024 * 1024, String}, nil, false},
		{"m-zset", args{"/tmp/rosedb", 2, MMap, 2 * 1024 * 1024, List}, nil, false},
		{"m-zset", args{"/tmp/rosedb", 3, MMap, 3 * 1024 * 1024, Hash}, nil, false},
		{"m-zset", args{"/tmp/rosedb", 4, MMap, 4 * 1024 * 1024, Set}, nil, false},
		{"m-zset", args{"/tmp/rosedb", 5, MMap, 5 * 1024 * 1024, ZSet}, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewDBFile(tt.args.path, tt.args.fileId, tt.args.method, tt.args.blockSize, tt.args.eType)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewDBFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.NotNil(t, got)
		})
	}
}

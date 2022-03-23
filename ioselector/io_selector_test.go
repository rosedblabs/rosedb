package ioselector

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestNewFileIOSelector(t *testing.T) {
	testNewIOSelector(t, 0)
}

func TestNewMMapSelector(t *testing.T) {
	testNewIOSelector(t, 1)
}

func TestFileIOSelector_Write(t *testing.T) {
	testIOSelectorWrite(t, 0)
}

func TestMMapSelector_Write(t *testing.T) {
	testIOSelectorWrite(t, 1)
}

func TestFileIOSelector_Read(t *testing.T) {
	testIOSelectorRead(t, 0)
}

func TestMMapSelector_Read(t *testing.T) {
	testIOSelectorRead(t, 1)
}

func TestFileIOSelector_Sync(t *testing.T) {
	testIOSelectorSync(t, 0)
}

func TestMMapSelector_Sync(t *testing.T) {
	testIOSelectorSync(t, 1)
}

func TestFileIOSelector_Close(t *testing.T) {
	testIOSelectorClose(t, 0)
}

func TestMMapSelector_Close(t *testing.T) {
	testIOSelectorClose(t, 1)
}

func TestFileIOSelector_Delete(t *testing.T) {
	testIOSelectorDelete(t, 0)
}

func TestMMapSelector_Delete(t *testing.T) {
	testIOSelectorDelete(t, 1)
}

func testNewIOSelector(t *testing.T, ioType uint8) {
	type args struct {
		fName string
		fsize int64
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"size-zero", args{fName: "000000001.wal", fsize: 0},
		},
		{
			"size-negative", args{fName: "000000002.wal", fsize: -1},
		},
		{
			"size-big", args{fName: "000000003.wal", fsize: 1024 << 20},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			absPath, err := filepath.Abs(filepath.Join("/tmp", tt.args.fName))
			assert.Nil(t, err)

			var got IOSelector
			if ioType == 0 {
				got, err = NewFileIOSelector(absPath, tt.args.fsize)
			}
			if ioType == 1 {
				got, err = NewMMapSelector(absPath, tt.args.fsize)
			}
			defer func() {
				if got != nil {
					err = got.Delete()
					assert.Nil(t, err)
				}
			}()
			if tt.args.fsize > 0 {
				assert.Nil(t, err)
				assert.NotNil(t, got)
			} else {
				assert.Equal(t, err, ErrInvalidFsize)
			}
		})
	}
}

func testIOSelectorWrite(t *testing.T, ioType uint8) {
	absPath, err := filepath.Abs(filepath.Join("/tmp", "00000001.vlog"))
	assert.Nil(t, err)
	var size int64 = 1048576

	var selector IOSelector
	if ioType == 0 {
		selector, err = NewFileIOSelector(absPath, size)
	}
	if ioType == 1 {
		selector, err = NewMMapSelector(absPath, size)
	}
	assert.Nil(t, err)
	defer func() {
		if selector != nil {
			_ = selector.Delete()
		}
	}()

	type fields struct {
		selector IOSelector
	}
	type args struct {
		b      []byte
		offset int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			"nil-byte", fields{selector: selector}, args{b: nil, offset: 0}, 0, false,
		},
		{
			"one-byte", fields{selector: selector}, args{b: []byte("0"), offset: 0}, 1, false,
		},
		{
			"many-bytes", fields{selector: selector}, args{b: []byte("lotusdb"), offset: 0}, 7, false,
		},
		{
			"bigvalue-byte", fields{selector: selector}, args{b: []byte(fmt.Sprintf("%01048576d", 123)), offset: 0}, 1048576, false,
		},
		{
			"exceed-size", fields{selector: selector}, args{b: []byte(fmt.Sprintf("%01048577d", 123)), offset: 0}, 1048577, false,
		},
		{
			"EOF-error", fields{selector: selector}, args{b: []byte("lotusdb"), offset: -1}, 0, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fields.selector.Write(tt.args.b, tt.args.offset)
			// io.EOF err in mmmap.
			if tt.want == 1048577 && ioType == 1 {
				tt.wantErr = true
				tt.want = 0
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Write() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func testIOSelectorRead(t *testing.T, ioType uint8) {
	absPath, err := filepath.Abs(filepath.Join("/tmp", "00000001.wal"))
	var selector IOSelector
	if ioType == 0 {
		selector, err = NewFileIOSelector(absPath, 100)
	}
	if ioType == 1 {
		selector, err = NewMMapSelector(absPath, 100)
	}
	assert.Nil(t, err)
	defer func() {
		if selector != nil {
			_ = selector.Delete()
		}
	}()
	offsets := writeSomeData(selector, t)
	results := [][]byte{
		[]byte(""),
		[]byte("1"),
		[]byte("lotusdb"),
	}

	type fields struct {
		selector IOSelector
	}
	type args struct {
		b      []byte
		offset int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			"nil", fields{selector: selector}, args{b: make([]byte, 0), offset: offsets[0]}, 0, false,
		},
		{
			"one-byte", fields{selector: selector}, args{b: make([]byte, 1), offset: offsets[1]}, 1, false,
		},
		{
			"many-bytes", fields{selector: selector}, args{b: make([]byte, 7), offset: offsets[2]}, 7, false,
		},
		{
			"EOF-1", fields{selector: selector}, args{b: make([]byte, 100), offset: -1}, 0, true,
		},
		{
			"EOF-2", fields{selector: selector}, args{b: make([]byte, 100), offset: 1024}, 0, true,
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fields.selector.Read(tt.args.b, tt.args.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Read() got = %v, want %v", got, tt.want)
			}
			if !tt.wantErr {
				assert.Equal(t, tt.args.b, results[i])
			}
		})
	}
}

func writeSomeData(selector IOSelector, t *testing.T) []int64 {
	tests := [][]byte{
		[]byte(""),
		[]byte("1"),
		[]byte("lotusdb"),
	}

	var offsets []int64
	var offset int64
	for _, tt := range tests {
		offsets = append(offsets, offset)
		n, err := selector.Write(tt, offset)
		assert.Nil(t, err)
		offset += int64(n)
	}
	return offsets
}

func testIOSelectorSync(t *testing.T, ioType uint8) {
	sync := func(id int, fsize int64) {
		absPath, err := filepath.Abs(filepath.Join("/tmp", fmt.Sprintf("0000000%d.wal", id)))
		assert.Nil(t, err)
		var selector IOSelector
		if ioType == 0 {
			selector, err = NewFileIOSelector(absPath, fsize)
		}
		if ioType == 1 {
			selector, err = NewMMapSelector(absPath, fsize)
		}
		assert.Nil(t, err)
		defer func() {
			if selector != nil {
				_ = selector.Delete()
			}
		}()
		writeSomeData(selector, t)
		err = selector.Sync()
		assert.Nil(t, err)
	}

	for i := 1; i < 4; i++ {
		sync(i, int64(i*100))
	}
}

func testIOSelectorClose(t *testing.T, ioType uint8) {
	sync := func(id int, fsize int64) {
		absPath, err := filepath.Abs(filepath.Join("/tmp", fmt.Sprintf("0000000%d.wal", id)))
		defer func() {
			_ = os.Remove(absPath)
		}()
		assert.Nil(t, err)
		var selector IOSelector
		if ioType == 0 {
			selector, err = NewFileIOSelector(absPath, fsize)
		}
		if ioType == 1 {
			selector, err = NewMMapSelector(absPath, fsize)
		}
		assert.Nil(t, err)
		defer func() {
			if selector != nil {
				err := selector.Close()
				assert.Nil(t, err)
			}
		}()
		writeSomeData(selector, t)
		assert.Nil(t, err)
	}

	for i := 1; i < 4; i++ {
		sync(i, int64(i*100))
	}
}

func testIOSelectorDelete(t *testing.T, ioType uint8) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{"1", false},
		{"2", false},
		{"3", false},
		{"4", false},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			absPath, err := filepath.Abs(filepath.Join("/tmp", fmt.Sprintf("0000000%d.wal", i)))
			assert.Nil(t, err)
			var selector IOSelector
			if ioType == 0 {
				selector, err = NewFileIOSelector(absPath, int64((i+1)*100))
			}
			if ioType == 1 {
				selector, err = NewFileIOSelector(absPath, int64((i+1)*100))
			}
			assert.Nil(t, err)

			if err := selector.Delete(); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

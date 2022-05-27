package util

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestStrToFloat64(t *testing.T) {
	testCases := []struct {
		name   string
		val    string
		expVal float64
		expErr bool
	}{
		{
			name:   "valid value",
			val:    "3434.4455664545",
			expVal: 3434.4455664545,
		},
		{
			name:   "out of range",
			val:    "1.7e+309", // max float64 = 1.7e+308
			expVal: math.Inf(1),
			expErr: true,
		},
		{
			name:   "invalid value",
			val:    "invalid",
			expVal: 0,
			expErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := StrToFloat64(tc.val)
			if tc.expErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
			assert.Equal(t, tc.expVal, res)
		})
	}
}

func TestFloat64ToStr(t *testing.T) {
	val := 9902.99355664
	res := Float64ToStr(val)
	assert.Equal(t, res, "9902.99355664")
}

func TestStrToInt64(t *testing.T) {
	testCases := []struct {
		name   string
		val    string
		expVal int64
		expErr bool
	}{
		{
			name:   "valid",
			val:    "12345678910",
			expVal: 12345678910,
		},
		{
			name:   "out of range",
			val:    "9243372036854775909", // Bigger than MaxInt64
			expVal: math.MaxInt64,
			expErr: true,
		},
		{
			name:   "invalid",
			val:    "invalid",
			expVal: 0,
			expErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := StrToInt64(tc.val)
			if tc.expErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
			assert.Equal(t, tc.expVal, res)
		})
	}
}

func TestStrToUint(t *testing.T) {
	testCases := []struct {
		name   string
		val    string
		expVal uint64
		expErr bool
	}{
		{
			name:   "valid",
			val:    "12345678910",
			expVal: 12345678910,
		},
		{
			name:   "out of range - exceeds max limit",
			val:    "18446744073709551620", // MaxUint64 = 18446744073709551615
			expVal: math.MaxUint64,
			expErr: true,
		},
		{
			name:   "out of range - negative value",
			val:    "-123",
			expVal: 0,
			expErr: true,
		},
		{
			name:   "invalid",
			val:    "invalid",
			expVal: 0,
			expErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := StrToUint(tc.val)
			if tc.expErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
			assert.Equal(t, tc.expVal, res)
		})
	}
}

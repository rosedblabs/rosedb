package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStrToFloat64(t *testing.T) {
	val := "3434.4455664545"
	res, err := StrToFloat64(val)
	assert.Nil(t, err)
	assert.Equal(t, res, 3434.4455664545)
}

func TestFloat64ToStr(t *testing.T) {
	val := 9902.99355664
	res := Float64ToStr(val)
	assert.Equal(t, res, "9902.99355664")
}

func TestStrToInt64(t *testing.T) {
	// valid
	val := "12345678910"
	expVal := int64(12345678910)
	res, err := StrToInt64(val)
	assert.Nil(t, err)
	assert.Equal(t, expVal, res)

	// invalid
	val = "invalid"
	expVal = 0
	res, err = StrToInt64(val)
	assert.NotNil(t, err)
	assert.Equal(t, expVal, res)
}

func TestStrToUint(t *testing.T) {
	// valid
	val := "12345678910"
	expVal := uint64(12345678910)
	res, err := StrToUint(val)
	assert.Nil(t, err)
	assert.Equal(t, expVal, res)

	// invalid
	invalidVal := "-123456"
	expVal = 0
	res, err = StrToUint(invalidVal)
	assert.NotNil(t, err)
	t.Log(err)
	assert.Equal(t, expVal, res)
}

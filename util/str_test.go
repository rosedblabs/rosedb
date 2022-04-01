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

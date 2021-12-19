package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStrToFloat64(t *testing.T) {
	val := "3434.4455664545"
	res, err := StrToFloat64(val)
	assert.Error(t, err)
	t.Log(res)
}

func TestFloat64ToStr(t *testing.T) {
	val := 3434.4455664545
	res := Float64ToStr(val)
	t.Log(res)
}

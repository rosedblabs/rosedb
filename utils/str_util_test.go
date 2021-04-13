package utils

import (
	"testing"
)

func TestStrToFloat64(t *testing.T) {
	val := 3434.4455664545
	res := Float64ToStr(val)
	t.Log(res)
}

package utils

import "strconv"

// Float64ToStr float64类型转换为string
func Float64ToStr(val float64) string {
	return strconv.FormatFloat(val, 'f', -1, 64)
}

// StrToFloat64 string转换为float64
func StrToFloat64(val string) (float64, error) {
	return strconv.ParseFloat(val, 64)
}

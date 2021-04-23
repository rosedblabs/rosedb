package rosedb

import (
	"encoding/json"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	b, _ := json.Marshal(config)
	t.Log(string(b))
}

package storage

import (
	"fmt"
	"testing"
)

func TestExpires_SaveExpires(t *testing.T) {
	expires := make(Expires)
	expires["key_001"] = 43312223
	expires["key_002"] = 18334312
	expires["key_003"] = 2312223
	expires["key_005"] = 7312223

	err := expires.SaveExpires("/tmp/rosedb/db.expires")
	if err != nil {
		t.Error(err)
	}
}

func TestLoadExpires(t *testing.T) {
	newExpires := LoadExpires("/tmp/rosedb/db.expires")
	t.Logf("%+v\n", newExpires)
	for k, v := range newExpires {
		fmt.Println(k, ":", v)
	}
}

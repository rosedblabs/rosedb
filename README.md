![rosedb_ico.png](https://i.loli.net/2021/04/28/gIL2FXZcOesPmyD.png)

[![Go Report Card](https://goreportcard.com/badge/github.com/roseduan/rosedb)&nbsp;](https://goreportcard.com/report/github.com/roseduan/rosedb)![GitHub top language](https://img.shields.io/github/languages/top/roseduan/rosedb)&nbsp;[![GitHub stars](https://img.shields.io/github/stars/roseduan/rosedb)&nbsp;](https://github.com/roseduan/rosedb/stargazers)[![codecov](https://codecov.io/gh/flower-corp/rosedb/branch/main/graph/badge.svg)](https://codecov.io/gh/flower-corp/rosedb) [![CodeFactor](https://www.codefactor.io/repository/github/flower-corp/rosedb/badge)](https://www.codefactor.io/repository/github/flower-corp/rosedb) [![Go Reference](https://pkg.go.dev/badge/github.com/roseduan/rosedb.svg)](https://pkg.go.dev/github.com/roseduan/rosedb) [![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go#database) [![LICENSE](https://img.shields.io/github/license/flower-corp/rosedb.svg?style=flat-square)](https://github.com/flower-corp/rosedb/blob/main/LICENSE)

English| [简体中文](https://github.com/roseduan/rosedb/blob/main/README-CN.md)

> Note: rosedb has no stable release now, don`t use it in production.

rosedb is a fast, stable, and embedded storage engine based on `bitcask`, also supports a variety of data structures such as `string`, `list`, `hash`, `set`, and `sorted set`.     

## Design Overview

![](https://github.com/flower-corp/rosedb/blob/main/resource/img/design-overview-rosedb.png)

## Get Started

```go
package main

import (
	"fmt"
	"github.com/flower-corp/rosedb"
	"github.com/flower-corp/rosedb/logger"
	"os"
	"path/filepath"
)

func main() {
	path := filepath.Join("/tmp", "rosedb")
	opts := rosedb.DefaultOptions(path)
	opts.IoType = rosedb.FileIO
	opts.IndexMode = rosedb.KeyValueMemMode
	db, err := rosedb.Open(opts)
	if err != nil {
		panic(err)
	}
	defer func() {
		if db != nil {
			err := os.RemoveAll(opts.DBPath)
			if err != nil {
				logger.Errorf("destroy db err: %v", err)
			}
		}
	}()

	key := []byte("key_1")
	val := []byte("val-1")
	if err = db.Set(key, val); err != nil {
		panic(err)
	}

	gotVal, err := db.Get(key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("val is %s\n", string(gotVal))
}
```

## Contributing

If you are intersted in contributing to rosedb, please see here: [CONTRIBUTING](https://github.com/roseduan/rosedb/blob/main/CONTRIBUTING.md)

## License

rosedb is licensed under the term of the [Apache 2.0 License](https://github.com/roseduan/rosedb/blob/main/LICENSE)


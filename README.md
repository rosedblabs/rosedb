![rosedb_ico.png](https://i.loli.net/2021/04/28/gIL2FXZcOesPmyD.png)

![](https://img.shields.io/github/license/roseduan/rosedb)&nbsp;[![Go Report Card](https://goreportcard.com/badge/github.com/roseduan/rosedb)&nbsp;](https://goreportcard.com/report/github.com/roseduan/rosedb)![GitHub top language](https://img.shields.io/github/languages/top/roseduan/rosedb)&nbsp;[![GitHub stars](https://img.shields.io/github/stars/roseduan/rosedb)&nbsp;](https://github.com/roseduan/rosedb/stargazers)[![codecov](https://codecov.io/gh/roseduan/rosedb/branch/main/graph/badge.svg?token=YZUB9QT6XF)](https://codecov.io/gh/roseduan/rosedb) [![CodeFactor](https://www.codefactor.io/repository/github/flower-corp/rosedb/badge)](https://www.codefactor.io/repository/github/flower-corp/rosedb) [![Go Reference](https://pkg.go.dev/badge/github.com/roseduan/rosedb.svg)](https://pkg.go.dev/github.com/roseduan/rosedb) [![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go#database) 

[English](https://github.com/roseduan/rosedb#rosedb) | [简体中文](https://github.com/roseduan/rosedb/blob/main/README-CN.md)

### rosedb is not production-ready, we are doing more tests now, we recommend you use [LotusDB](https://github.com/flower-corp/lotusdb) instead.

rosedb is a fast, stable, and embedded key-value (k-v) storage engine based on `bitcask`, also supports a variety of data structures such as `string`, `list`, `hash`, `set`, and `sorted set`.     

## Features

* Supports all common data structures:  `string`, `list`, `hash`, `set`, `zset`.
* Easy to embed (`import "github.com/roseduan/rosedb"`).
* Low latency and high throughput.
* Has built-in parallel execution of data modification on many provided data structures.
* Comes with rosedb-cli for command-line access, that is compatible with redis-cli.
* Supports TTL-based key eviction.
* Supports prefix scan and range scan for string keys.
* Support simple transaction, ACID features.
* Merge operation can be stopped manually.

## Usage

### Cli example

Navigate to rosedb/cmd/server and run `main.go`

![Xnip2021-04-14_14-33-11.png](https://i.loli.net/2021/04/14/EsMFv48YB3P9j7k.png)

Open a new terminal, navigate to rosedb/cmd/cli, and run `main.go`：

![Xnip2021-04-14_14-35-50.png](https://i.loli.net/2021/04/14/9uh1ElVF3C4D6dM.png)

### Embedding example

Import rosedb in the application:

```go
import "github.com/roseduan/rosedb"
```

Open a connection to the database:

```go
package main

import (
	"github.com/roseduan/rosedb"
	"log"
)

func main() {
	config := rosedb.DefaultConfig()
	db, err := rosedb.Open(config)
	
	if err != nil {
		log.Fatal(err)
	}
	
  // don`t forget to close!
	defer db.Close()
	
	//...
}
```

## Contributing

If you are intersted in contributing to rosedb, please see here: [CONTRIBUTING](https://github.com/roseduan/rosedb/blob/main/CONTRIBUTING.md)

## Contact me

If you have any questions, you can contact me by email: roseduan520@gmail.com

## License

rosedb is licensed under the term of the [MIT License](https://github.com/roseduan/rosedb/blob/main/LICENSE)


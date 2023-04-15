![rosedb_ico.png](https://i.loli.net/2021/04/28/gIL2FXZcOesPmyD.png)

[![Go Report Card](https://goreportcard.com/badge/github.com/roseduan/rosedb)&nbsp;](https://goreportcard.com/report/github.com/roseduan/rosedb)![GitHub top language](https://img.shields.io/github/languages/top/roseduan/rosedb)&nbsp;[![GitHub stars](https://img.shields.io/github/stars/roseduan/rosedb)&nbsp;](https://github.com/roseduan/rosedb/stargazers)[![codecov](https://codecov.io/gh/flower-corp/rosedb/branch/main/graph/badge.svg)](https://codecov.io/gh/flower-corp/rosedb) [![CodeFactor](https://www.codefactor.io/repository/github/flower-corp/rosedb/badge)](https://www.codefactor.io/repository/github/flower-corp/rosedb) [![Go Reference](https://pkg.go.dev/badge/github.com/roseduan/rosedb.svg)](https://pkg.go.dev/github.com/roseduan/rosedb) [![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go#database) [![LICENSE](https://img.shields.io/github/license/flower-corp/rosedb.svg?style=flat-square)](https://github.com/flower-corp/rosedb/blob/main/LICENSE)

# The project is being refactored and should not be used in a production, thanks for your support!
# 项目正在重构中，暂时不要用于生产环境，谢谢支持！
English| [简体中文](https://github.com/roseduan/rosedb/blob/main/README-CN.md)

rosedb is a fast, stable, and embedded NoSQL database based on `bitcask`, supports a variety of data structures such as `string`, `list`, `hash`, `set`, and `sorted set`.     

It is similar to `Redis` but store values on disk.

Key features:

* **Compatible with Redis protocol (not fully)**
* **Many data structures: `string`, `list`, `hash`, `set`, and `sorted set`**
* **Easy to embed into your own Go application**
* **High performance, suitable for both read and write intensive workload**
* **Values are not limited by RAM**

## Design Overview

![](https://github.com/flower-corp/rosedb/blob/main/resource/img/design-overview-rosedb.png)

## Quick Start

### Embedded usage:

rosedb provides code example for each structure, please see [example](https://github.com/flower-corp/rosedb/tree/main/examples).

### Command line usage:

1. start rosedb server.

```shell
cd rosedb
make
./rosedb-server [-option value]
```

2. use redis client to access data, such as `redis-cli`.

```shell
./redis-cli -p 5200

127.0.0.1:5200> 
127.0.0.1:5200> set my_key RoseDB
OK
127.0.0.1:5200> get my_key
"RoseDB"
127.0.0.1:5200> 
```

## Documentation

See [wiki](https://github.com/flower-corp/rosedb/wiki)

## Community

Welcome to join the [Slack channel](https://join.slack.com/t/flowercorp-slack/shared_invite/zt-19oj8ecqb-V02ycMV0BH1~Tn6tfeTz6A) and [Discussions](https://github.com/flower-corp/rosedb/discussions) to connect with RoseDB team members and other users.

If you are a Chinese user, you are also welcome to join our WeChat group, scan the QR code and you will be invited:

| <img src="https://i.loli.net/2021/05/06/tGTH7SXg8w95slA.jpg" width="200px" align="left"/> |
| ------------------------------------------------------------ |

## Contributing

If you are interested in contributing to rosedb, see [CONTRIBUTING](https://github.com/roseduan/rosedb/blob/main/CONTRIBUTING.md) and [how to contribute?](https://github.com/flower-corp/rosedb/issues/103)

## License

rosedb is licensed under the term of the [Apache 2.0 License](https://github.com/roseduan/rosedb/blob/main/LICENSE)


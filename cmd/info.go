package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"
)

var (
	rosedbVersion   string
	gitHash         string
	buildTime       string
	operatingSystem string
	osArch          string
	goVersion       string
	processID       string
	tcpPort         string
	startTime       time.Time
)

func init() {
	goVersion = runtime.Version()
	operatingSystem = runtime.GOOS
	osArch = runtime.GOARCH
	processID = strconv.Itoa(os.Getpid())
}

func getInfoString() (infoS string) {
	infoS = fmt.Sprintf(
		`# Server
rosedb_version: %s,
rosedb_git_sha1: %s,
rosedb_build_time: %s,
os: %s,
os_arch: %s,
go_version: %s,
process_ID: %s,
tcp_port: %s,
uptime_in_seconds: %d,
uptime_in_days: %d,
executable: %s,
`,
		rosedbVersion,
		gitHash,
		buildTime,
		operatingSystem,
		osArch,
		goVersion,
		processID,
		tcpPort,
		time.Now().Second()-startTime.Second(),
		time.Now().Day()-startTime.Day(),
		os.Args[0],
	)
	return
}

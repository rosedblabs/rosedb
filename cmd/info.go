package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/shirou/gopsutil/host"
)

var (
	rosedbVersion string
	gitHash       string
	buildTime     string
	osInfo        string
	archBits      string
	goVersion     string
	processID     string
	tcpPort       string
	startTime     time.Time
)

func init() {
	goVersion = runtime.Version()
	processID = strconv.Itoa(os.Getpid())
	hInfo, _ := host.Info()
	osInfo = fmt.Sprintf("%s %s %s", hInfo.Platform, hInfo.KernelVersion, hInfo.KernelArch)
	archBits = hInfo.KernelArch
}

func getInfoString() (infoS string) {
	infoS += getServerInfo()
	return
}

func getServerInfo() (serverInfo string) {
	upTimeInSec := time.Now().Unix() - startTime.Unix()
	upTimeInDay := upTimeInSec/(24*60*60) + 1
	serverInfo = fmt.Sprintf(
		`# Server
rosedb_version: %s
rosedb_git_sha1: %s
rosedb_build_time: %s
os: %s
arch_bits: %s
go_version: %s
process_id: %s
tcp_port: %s
uptime_in_seconds: %d
uptime_in_days: %d
executable: %s
`,
		rosedbVersion,
		gitHash,
		buildTime,
		osInfo,
		archBits,
		goVersion,
		processID,
		tcpPort,
		upTimeInSec,
		upTimeInDay,
		os.Args[0],
	)
	return
}

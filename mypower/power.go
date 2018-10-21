package mypower

import (
	"dannytools/mycmd"
	"strings"
)

//0: ok, 1: bad
func GetPowerStat(cmd string, timeout uint32) int8 {
	if cmd == "" {
		cmd = "sudo ipmitool sdr type 'Power Supply'"
	}
	outMsg, errMsg, err := mycmd.ExecCmdTimeOutStringBash(timeout, cmd)
	if err != nil || errMsg != "" {
		return -1
	} else if outMsg == "" {
		return -2
	}
	arr := strings.Split(outMsg, "\n")
	if len(arr) < 1 {
		return -1
	}
	for _, line := range arr {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		tArr := strings.Split(line, "|")
		if len(tArr) < 4 {
			continue
		}
		tArr[2] = strings.ToLower(strings.TrimSpace(tArr[2]))
		if tArr[2] != "ok" {
			return 1
		}
	}
	return 0
}

package mylvs

import (
	"dannytools/mycmd"
	"dannytools/myos"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/toolkits/slice"
)

var (
	IpVsConnReg *regexp.Regexp = regexp.MustCompile(`size=(\d+)`)
	LvsPortReg  *regexp.Regexp = regexp.MustCompile(`^\d+$`)
)

type RealSeverStat struct {
	Ip         string
	Port       string
	Weight     string
	ActiveConn int64
	InactConn  int64
	CPS        int64
	InPPS      int64
	OutPPS     int64
	InBPS      int64
	OutBPS     int64
}

type VhostStat struct {
	Vip         string
	Port        string
	LB          string //负载均衡算法
	ActiveConn  int64
	InactConn   int64
	CPS         int64
	InPPS       int64
	OutPPS      int64
	InBPS       int64
	OutBPS      int64
	RealServers map[string]*RealSeverStat
}

type LvsStat struct {
	VhostStats map[string]*VhostStat
	IpVsConn   int64
}

func GetLvsConnection(sts *LvsStat, cmd string, timeout uint32) {
	var (
		tmpInt       int64
		err          error
		CurrentVhost string
		ok           bool
	)

	if cmd == "" {
		cmd = "sudo /sbin/ipvsadm -Ln"
	}
	outMsg, errMsg, err := mycmd.ExecCmdTimeOutStringBash(timeout, cmd)
	if err != nil || errMsg != "" {
		if sts.IpVsConn == 0 || len(sts.VhostStats) == 0 {
			sts.IpVsConn = -1
		}
		return
	}
	if outMsg == "" {
		if sts.IpVsConn == 0 || len(sts.VhostStats) == 0 {
			sts.IpVsConn = -2
		}
		return
	}
	lines := strings.Split(outMsg, "\n")
	if len(lines) < 1 {
		if sts.IpVsConn == 0 || len(sts.VhostStats) == 0 {
			sts.IpVsConn = -2
		}
		return
	}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "IP") {
			if sts.IpVsConn <= 0 {
				tArr := IpVsConnReg.FindStringSubmatch(line)
				if len(tArr) > 1 {
					tmpInt, err = strconv.ParseInt(tArr[1], 10, 64)
					if err != nil {

						sts.IpVsConn = -1
					} else {
						sts.IpVsConn = tmpInt
					}
				}
			}
			continue
		}
		arr := strings.Fields(line)
		arrLen := len(arr)
		if arrLen >= 3 {
			if sts.VhostStats == nil || len(sts.VhostStats) == 0 {
				sts.VhostStats = map[string]*VhostStat{}
			}
			if arr[0] == "TCP" || arr[0] == "UDP" {
				tmpArr := strings.Split(arr[1], ":")
				if len(tmpArr) == 2 {

					if _, ok = sts.VhostStats[arr[1]]; ok {
						sts.VhostStats[arr[1]].LB = arr[2]
					} else {
						sts.VhostStats[arr[1]] = &VhostStat{Vip: tmpArr[0], Port: tmpArr[1], LB: arr[2], RealServers: map[string]*RealSeverStat{}}
					}
					CurrentVhost = arr[1]
				}
				continue
			}
			if arrLen >= 6 {
				if arr[2] == "Route" {
					tmpArr := strings.Split(arr[1], ":")
					if len(tmpArr) == 2 {

						if _, ok = sts.VhostStats[CurrentVhost]; ok {
							if _, ok = sts.VhostStats[CurrentVhost].RealServers[arr[1]]; ok {
								sts.VhostStats[CurrentVhost].RealServers[arr[1]].Weight = arr[3]
							} else {
								sts.VhostStats[CurrentVhost].RealServers[arr[1]] = &RealSeverStat{Ip: tmpArr[0], Port: tmpArr[1], Weight: arr[3]}
							}

							tmpInt, err = strconv.ParseInt(arr[4], 10, 64)
							if err == nil {
								sts.VhostStats[CurrentVhost].RealServers[arr[1]].ActiveConn = tmpInt
								sts.VhostStats[CurrentVhost].ActiveConn += tmpInt
							} else {
								sts.VhostStats[CurrentVhost].RealServers[arr[1]].ActiveConn = -1
							}
							tmpInt, err = strconv.ParseInt(arr[5], 10, 64)
							if err == nil {
								sts.VhostStats[CurrentVhost].RealServers[arr[1]].InactConn = tmpInt
								sts.VhostStats[CurrentVhost].InactConn += tmpInt
							} else {
								sts.VhostStats[CurrentVhost].RealServers[arr[1]].InactConn = -1
							}
						}
					}
				}
				continue
			}
			continue
		}

	}

}

func GetLvsNetStat(sts *LvsStat, cmd string, timeout uint32) {
	var (
		tmpInt       int64
		err          error
		CurrentVhost string
		ok           bool
		ifVhost      bool = false
	)

	if cmd == "" {
		cmd = "sudo /sbin/ipvsadm -Ln --rate"
	}
	outMsg, errMsg, err := mycmd.ExecCmdTimeOutStringBash(timeout, cmd)
	if err != nil || errMsg != "" {
		if sts.IpVsConn == 0 || len(sts.VhostStats) == 0 {
			sts.IpVsConn = -1
		}
		return
	}
	if outMsg == "" {
		if sts.IpVsConn == 0 || len(sts.VhostStats) == 0 {
			sts.IpVsConn = -2
		}
		return
	}
	lines := strings.Split(outMsg, "\n")
	if len(lines) < 1 {
		if sts.IpVsConn == 0 || len(sts.VhostStats) == 0 {
			sts.IpVsConn = -2
		}
		return
	}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "IP") {
			if sts.IpVsConn <= 0 {
				tArr := IpVsConnReg.FindStringSubmatch(line)
				if len(tArr) > 1 {
					tmpInt, err = strconv.ParseInt(tArr[1], 10, 64)
					if err != nil {
						sts.IpVsConn = -1
					} else {
						sts.IpVsConn = tmpInt
					}
				}
			}
			continue
		}
		arr := strings.Fields(line)
		arrLen := len(arr)
		if arrLen >= 7 {
			tArr := strings.Split(arr[1], ":")
			if len(tArr) == 2 {
				if LvsPortReg.MatchString(tArr[1]) {
					if sts.VhostStats == nil || len(sts.VhostStats) == 0 {
						sts.VhostStats = map[string]*VhostStat{}
					}
					if arr[0] == "TCP" || arr[0] == "UDP" {
						ifVhost = true
						if _, ok = sts.VhostStats[arr[1]]; !ok {
							sts.VhostStats[arr[1]] = &VhostStat{RealServers: map[string]*RealSeverStat{}}
						}
						CurrentVhost = arr[1]
					} else if arr[0] == "->" {
						ifVhost = false
						if _, ok = sts.VhostStats[CurrentVhost].RealServers[arr[1]]; !ok {
							sts.VhostStats[CurrentVhost].RealServers[arr[1]] = &RealSeverStat{Ip: tArr[0], Port: tArr[1]}
						}
					} else {
						continue
					}
					//CPS
					tmpInt, err = strconv.ParseInt(arr[2], 10, 64)
					if err != nil {
						tmpInt = -1
					}
					if ifVhost {
						sts.VhostStats[arr[1]].CPS = tmpInt
					} else {
						sts.VhostStats[CurrentVhost].RealServers[arr[1]].CPS = tmpInt
					}

					//InPPS
					tmpInt, err = strconv.ParseInt(arr[3], 10, 64)
					if err != nil {
						tmpInt = -1
					}
					if ifVhost {
						sts.VhostStats[arr[1]].InPPS = tmpInt
					} else {
						sts.VhostStats[CurrentVhost].RealServers[arr[1]].InPPS = tmpInt
					}

					//OutPPS
					tmpInt, err = strconv.ParseInt(arr[4], 10, 64)
					if err != nil {
						tmpInt = -1
					}
					if ifVhost {
						sts.VhostStats[arr[1]].OutPPS = tmpInt
					} else {
						sts.VhostStats[CurrentVhost].RealServers[arr[1]].OutPPS = tmpInt
					}

					//InBPS
					tmpInt, err = strconv.ParseInt(arr[5], 10, 64)
					if err != nil {
						tmpInt = -1
					}
					if ifVhost {
						sts.VhostStats[arr[1]].InBPS = tmpInt
					} else {
						sts.VhostStats[CurrentVhost].RealServers[arr[1]].InBPS = tmpInt
					}

					//OutBPS
					tmpInt, err = strconv.ParseInt(arr[6], 10, 64)
					if err != nil {
						tmpInt = -1
					}
					if ifVhost {
						sts.VhostStats[arr[1]].OutBPS = tmpInt
					} else {
						sts.VhostStats[CurrentVhost].RealServers[arr[1]].OutBPS = tmpInt
					}
				}
			}
			continue
		}
	}
}

func GetIrpCpuCounts(cmd string, timeout uint32) int64 {
	if cmd == "" {
		cmd = "sudo -i  cat /proc/interrupts  | egrep 'eth[0-9]-rx|eth[0-9]-TxRx|eth[0-9]-[0-9]' | awk -F: '{print $1}' | xargs -i sudo cat /proc/irq/{}/smp_affinity | sort | uniq -c | wc -l"
	}
	outMsg, errMsg, err := mycmd.ExecCmdTimeOutStringBash(timeout, cmd)
	if err != nil || errMsg != "" || outMsg == "" {
		return -1
	}
	tmpInt, err := strconv.ParseInt(outMsg, 10, 64)
	if err != nil {
		return -1
	}
	return tmpInt
}

func GetNetConfig(ifaces []string, cfgs []string, cmdFmt string, timeout uint32) map[string]map[string]int8 {
	var (
		m        map[string]map[string]int8 = map[string]map[string]int8{}
		ifTarget bool                       = false
		cmd      string
		iface    string
		outMsg   string
		errMsg   string
		err      error
		lines    []string
		line     string
		arr      []string
	)
	for _, iface = range ifaces {
		cmd = fmt.Sprintf(cmdFmt, iface)
		outMsg, errMsg, err = mycmd.ExecCmdTimeOutStringBash(timeout, cmd)
		if err != nil || errMsg != "" || outMsg == "" {
			continue
		}
		lines = strings.Split(outMsg, "\n")
		if len(lines) == 0 {
			continue
		}
		m[iface] = map[string]int8{}
		for _, line = range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			arr = strings.Split(line, ":")
			if len(arr) <= 1 {
				continue
			}
			arr[0] = strings.TrimSpace(arr[0])
			ifTarget = false
			if len(cfgs) > 0 {
				if slice.ContainsString(cfgs, arr[0]) {
					ifTarget = true
				}
			} else {
				ifTarget = true
			}
			if !ifTarget {
				continue
			}
			tArr := strings.Fields(strings.TrimSpace(arr[1]))
			if tArr[0] == "on" {
				m[iface][arr[0]] = 1
			} else {
				m[iface][arr[0]] = 0
			}
		}

	}
	return m
}

func GetNetLROGRO(ifacePrefix []string, timeout uint32) map[string]map[string]int8 {
	netIfs, err := myos.GetNetIfaces(ifacePrefix)
	if err != nil || len(netIfs) <= 0 {
		return nil
	}

	return GetNetConfig(netIfs, []string{"generic-receive-offload", "large-receive-offload"}, "sudo ethtool -k %s", timeout)
}

func CheckIfLvsRunning(psLvsCmd string, checkLvsCfgCmd string, timeout uint32) bool {
	var (
		outMsg string
		errMsg string
		err    error
		tmpInt int64
	)
	//fmt.Println("enter CheckIfLvsRunning")
	if psLvsCmd == "" {
		psLvsCmd = "ps uax | grep 'keepalived [-]D' | grep -v grep | wc -l"
	}
	if checkLvsCfgCmd == "" {
		checkLvsCfgCmd = "grep -i 'real_server' /etc/keepalived/keepalived.conf | grep -v grep | wc -l"
	}
	outMsg, errMsg, err = mycmd.ExecCmdTimeOutStringBash(timeout, psLvsCmd)
	if err != nil || errMsg != "" || outMsg == "" {
		return false
	}
	tmpInt, err = strconv.ParseInt(outMsg, 10, 64)
	if err != nil {
		return false
	}
	if tmpInt < 1 {
		return false
	}

	outMsg, errMsg, err = mycmd.ExecCmdTimeOutStringBash(timeout, checkLvsCfgCmd)
	if err != nil || errMsg != "" || outMsg == "" {
		return false
	}
	tmpInt, err = strconv.ParseInt(outMsg, 10, 64)
	if err != nil {
		return false
	}
	if tmpInt < 1 {
		return false
	}
	return true
}

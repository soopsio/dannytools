package myraid

import (
	"dannytools/mycmd"
	"fmt"
	"strconv"
	"strings"
	//"github.com/davecgh/go-spew/spew"
)

type SSDStat struct {
	DeviceId      string
	SSDLifeRemain float64
}

type HardDiskStat struct {
	MediaType         string // hdd, ssd
	DeviceId          string
	SlotNumb          string
	MediaErrCnt       int64
	OtherErrCnt       int64
	PredictErrCnt     int64
	IsFirmwareStateOk int8
}

func (this *HardDiskStat) SetHardDiskStatUnknown() {
	this.SlotNumb = "-1"
	this.MediaErrCnt = -1
	this.OtherErrCnt = -1
	this.PredictErrCnt = -1
	this.IsFirmwareStateOk = -1
}

type RaidVdStat struct {
	VDSeqNumb      string
	StateIsOptimal int8
}

type BBUStats struct {
	AdapterIndex             string
	IsBatteryReplaceRequired int8
}

func GetBBUStats(cmd string, timeout uint32) []BBUStats {
	var (
		sts   []BBUStats
		adIdx string = "-1"
	)
	if cmd == "" {
		cmd = "sudo /opt/MegaRAID/MegaCli/MegaCli64 -AdpBbuCmd -aALL -Nolog"
	}
	outMsg, errMsg, err := mycmd.ExecCmdTimeOutStringBash(timeout, cmd)
	if err != nil || errMsg != "" {
		sts = append(sts, BBUStats{AdapterIndex: "-1", IsBatteryReplaceRequired: -1})
		return sts
	} else if outMsg == "" {
		return sts
	}
	arr := strings.Split(outMsg, "\n")
	if len(arr) == 0 {
		sts = append(sts, BBUStats{AdapterIndex: "-1", IsBatteryReplaceRequired: -1})
		return sts
	}
	for _, line := range arr {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		tArr := strings.Split(line, ":")
		if len(tArr) < 2 {
			continue
		}
		tArr[0] = strings.TrimSpace(tArr[0])
		switch tArr[0] {
		case "BBU status for Adapter":
			adIdx = strings.TrimSpace(tArr[1])
		case "Battery Replacement required":
			if strings.ToLower(strings.TrimSpace(tArr[1])) == "yes" {
				sts = append(sts, BBUStats{AdapterIndex: adIdx, IsBatteryReplaceRequired: 1})
			} else {
				sts = append(sts, BBUStats{AdapterIndex: adIdx, IsBatteryReplaceRequired: 0})
			}
		}
	}
	return sts
}

func GetSSDStats(devIds []string, timeout uint32) []SSDStat {
	var (
		cmd      string
		outMsg   string
		errMsg   string
		err      error
		sts      []SSDStat
		id       string
		tmpFloat float64
	)
	//233 Media_Wearout_Indicator 0x0032   100   100   000    Old_age   Always       -       0

	for _, id = range devIds {
		//fmt.Printf("ssd life id=%s\n", id)
		//not care /dev/sdx, since then get the same output if /dev/sdx exists
		cmd = fmt.Sprintf("sudo smartctl -a -d sat+megaraid,%s /dev/sda", id)
		outMsg, errMsg, err = mycmd.ExecCmdTimeOutStringBash(timeout, cmd)
		if err != nil && !strings.Contains(err.Error(), "exit status 4") {
			sts = append(sts, SSDStat{DeviceId: id, SSDLifeRemain: -1})
			continue
		} else if errMsg != "" && !strings.Contains(errMsg, "exit status 4") {
			sts = append(sts, SSDStat{DeviceId: id, SSDLifeRemain: -1})
			continue
		} else if outMsg == "" {
			//fmt.Printf("ssd life empty output\n")
			continue
		}
		arr := strings.Split(outMsg, "\n")
		if len(arr) == 0 {
			//fmt.Printf("ssd life output arr len=0\n")
			sts = append(sts, SSDStat{DeviceId: id, SSDLifeRemain: -1})
			continue
		}
		for _, line := range arr {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			tArr := strings.Fields(line)
			//spew.Dump(tArr)
			if len(tArr) < 10 {
				continue
			}
			tArr[1] = strings.TrimSpace(tArr[1])

			switch tArr[1] {
			case "Media_Wearout_Indicator":
				//spew.Dump(tArr)
				tArr[3] = strings.TrimPrefix(strings.TrimSpace(tArr[3]), "0")
				tmpFloat, err = strconv.ParseFloat(tArr[3], 64)
				if err != nil {
					//fmt.Printf("ssd life parse error: %s\n", err)
					sts = append(sts, SSDStat{DeviceId: id, SSDLifeRemain: -1})
				} else {
					sts = append(sts, SSDStat{DeviceId: id, SSDLifeRemain: tmpFloat})
				}
				break // only care this metric now
			}
		}
	}
	return sts
}

func GetRaidVirtualDriveStat(cmd string, timeout uint32) []RaidVdStat {
	var (
		sts   []RaidVdStat
		VdSeq string = "-1"
	)
	if cmd == "" {
		cmd = "sudo /opt/MegaRAID/MegaCli/MegaCli64 -LDInfo -Lall -aALL -Nolog"
	}
	outMsg, errMsg, err := mycmd.ExecCmdTimeOutStringBash(timeout, cmd)
	if err != nil || errMsg != "" {
		sts = append(sts, RaidVdStat{VDSeqNumb: "-1", StateIsOptimal: -1})
		return sts
	} else if outMsg == "" {
		return sts
	}
	arr := strings.Split(outMsg, "\n")
	if len(arr) == 0 {
		sts = append(sts, RaidVdStat{VDSeqNumb: "-1", StateIsOptimal: -1})
		return sts
	}
	for _, line := range arr {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		tArr := strings.Split(line, ":")
		if len(tArr) < 2 {
			continue
		}
		tArr[0] = strings.TrimSpace(tArr[0])
		switch tArr[0] {
		case "Virtual Drive":
			tStr := strings.Fields(tArr[1])[0]
			tStr = strings.TrimSpace(tStr)
			if tStr == "" {
				continue
			}
			VdSeq = tStr
		case "State":
			tArr[1] = strings.TrimSpace(tArr[1])
			if tArr[1] == "Optimal" {
				sts = append(sts, RaidVdStat{VDSeqNumb: VdSeq, StateIsOptimal: 1})
			} else {
				sts = append(sts, RaidVdStat{VDSeqNumb: VdSeq, StateIsOptimal: 0})
			}
		}
	}
	return sts
}

func GetHardDiskStat(cmd string, timeout uint32) []HardDiskStat {
	var (
		sts    []HardDiskStat
		tmpInt int64
		err    error
		arrIdx int           = -1
		oneSts *HardDiskStat = &HardDiskStat{}
	)
	if cmd == "" {
		cmd = "sudo /opt/MegaRAID/MegaCli/MegaCli64 -PDList -aALL -Nolog"
	}
	outMsg, errMsg, err := mycmd.ExecCmdTimeOutStringBash(timeout, cmd)
	if err != nil || errMsg != "" {
		oneSts.SetHardDiskStatUnknown()
		sts = append(sts, *oneSts)
		return sts
	} else if outMsg == "" {
		return sts
	}
	arr := strings.Split(outMsg, "\n")
	if len(arr) == 0 {
		oneSts.SetHardDiskStatUnknown()
		sts = append(sts, *oneSts)
		return sts
	}
	for _, line := range arr {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		tArr := strings.Split(line, ":")
		if len(tArr) < 2 {
			continue
		}
		tArr[0] = strings.TrimSpace(tArr[0])
		switch tArr[0] {
		case "Slot Number":
			arrIdx++
			sts = append(sts, HardDiskStat{SlotNumb: strings.TrimSpace(tArr[1])})
		case "Device Id":
			if arrIdx < 0 {
				continue
			}
			sts[arrIdx].DeviceId = strings.TrimSpace(tArr[1])
		case "Media Error Count":
			if arrIdx < 0 {
				continue
			}
			tmpInt, err = strconv.ParseInt(strings.TrimSpace(tArr[1]), 10, 64)
			if err != nil {
				sts[arrIdx].MediaErrCnt = -1
			} else {
				sts[arrIdx].MediaErrCnt = tmpInt
			}
		case "Other Error Count":
			if arrIdx < 0 {
				continue
			}
			tmpInt, err = strconv.ParseInt(strings.TrimSpace(tArr[1]), 10, 64)
			if err != nil {
				sts[arrIdx].OtherErrCnt = -1
			} else {
				sts[arrIdx].OtherErrCnt = tmpInt
			}
		case "Predictive Failure Count":
			if arrIdx < 0 {
				continue
			}
			tmpInt, err = strconv.ParseInt(strings.TrimSpace(tArr[1]), 10, 64)
			if err != nil {
				sts[arrIdx].PredictErrCnt = -1
			} else {
				sts[arrIdx].PredictErrCnt = tmpInt
			}
		case "Firmware state":
			//Firmware state: Rebuild
			if arrIdx < 0 {
				continue
			}
			tmpStr := strings.TrimSpace(tArr[1])
			if tmpStr == "Online, Spun Up" {
				sts[arrIdx].IsFirmwareStateOk = 1
			} else if tmpStr == "Rebuild" {
				sts[arrIdx].IsFirmwareStateOk = 2
			} else {
				sts[arrIdx].IsFirmwareStateOk = 0
			}
		case "Media Type":
			if arrIdx < 0 {
				continue
			}
			if strings.TrimSpace(tArr[1]) == "Solid State Device" {
				sts[arrIdx].MediaType = "ssd"
			} else {
				sts[arrIdx].MediaType = "hdd"
			}

		}
	}
	return sts
}

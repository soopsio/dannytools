package mypciecard

import (
	"encoding/json"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/toolkits/file"
)

const (
	C_SEU_FLAG_NORMAL        uint8 = 1
	C_SEU_FLAG_CORRECTABLE   uint8 = 2
	C_SEU_FLAG_UNCORRECTABLE uint8 = 3
	C_SEU_FLAG_UNKNOWN       uint8 = 255
)

var (
	//Found Shannon PCIE Flash card /dev/sctb:
	G_New_Shannon_Card_Line_Regexp *regexp.Regexp = regexp.MustCompile(`^Found\s+Shannon\s+PCIE`)

	//Totally found 2 Direct-IO PCIe Flash card on this system
	G_shannon_Card_Numb_Regexp *regexp.Regexp = regexp.MustCompile(`Totally\s+found\s+(\d+)\s+Direct-IO\s+PCIe`)
)

type ShannonStatusAll struct {
	StatusAllOut string

	ShannonCardsNumb int64
	ShannonStatuses  []*ShannonStatusOne
	IfHasSysFile     bool
}

type ShannonStatusOne struct {
	MountPoint            string
	IsReadWriteable       uint8
	OverProvision         float64
	ControllerTemperature int64
	BoardTemperature      int64
	FlashTemperature      int64
	DynamicBadBlocks      int64
	IsMediaStatusHealthy  uint8
	SeuFlag               uint8
	EstimatedLifeLeft     float64
	SeuCrcError           int64
	SeuCrcErrorHistory    int64
	SeuEccError           int64
	SeuEccErrorHistory    int64
	StaticBadBlkCnt       int64
	FreeBlkCnt            int64
}

func (this *ShannonStatusAll) GetShannonStatusAllOut(timeOut uint32) string {
	cmd := "sudo shannon-status -a -p"
	errMsg := ""
	this.StatusAllOut, errMsg = RunPcieCardStatusCmd(timeOut, cmd)
	return errMsg
}

func (this *ShannonStatusAll) ParseShannonStatusAllOut() {
	var (
		cardIndx int = -1
		tmpFloat float64
		err      error
		tmpInt   int64
	)
	if this.StatusAllOut == "" {
		this.ShannonCardsNumb = -1
		return
	}

	this.ShannonStatuses = []*ShannonStatusOne{}

	arr := strings.Split(this.StatusAllOut, "\n")
	for _, line := range arr {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		//Found Shannon PCIE Flash card /dev/sctb:
		//Found Shannon PCIE SSD card /dev/scta:
		if G_New_Shannon_Card_Line_Regexp.MatchString(line) {
			cardIndx++
			this.ShannonStatuses = append(this.ShannonStatuses, &ShannonStatusOne{})
			continue
		}
		//Totally found 2 Direct-IO PCIe Flash card on this system.
		numbArr := G_shannon_Card_Numb_Regexp.FindStringSubmatch(line)
		if len(numbArr) > 1 {
			tmpInt, err = strconv.ParseInt(numbArr[1], 10, 64)
			if err == nil {
				this.ShannonCardsNumb = tmpInt
			}
		}
		tArr := strings.Split(line, "=")
		if len(tArr) < 2 {
			continue
		}
		switch tArr[0] {
		case "block_device_node":
			this.ShannonStatuses[cardIndx].MountPoint = tArr[1]
		case "access_mode":
			if tArr[1] == "ReadWrite" {
				this.ShannonStatuses[cardIndx].IsReadWriteable = 1
			} else {
				this.ShannonStatuses[cardIndx].IsReadWriteable = 0
			}
		case "overprovision":
			tArr[1] = strings.Trim(tArr[1], "%")
			tmpFloat, err = strconv.ParseFloat(tArr[1], 64)
			if err == nil {
				this.ShannonStatuses[cardIndx].OverProvision = tmpFloat
			} else {
				this.ShannonStatuses[cardIndx].OverProvision = -1
			}
		case "controller_temperature":
			tempArr := strings.Fields(tArr[1])
			tmpInt, err = strconv.ParseInt(tempArr[0], 10, 64)
			if err == nil {
				this.ShannonStatuses[cardIndx].ControllerTemperature = tmpInt
			} else {
				this.ShannonStatuses[cardIndx].ControllerTemperature = -1
			}
		case "board_temperature":
			tempArr := strings.Fields(tArr[1])
			tmpInt, err = strconv.ParseInt(tempArr[0], 10, 64)
			if err == nil {
				this.ShannonStatuses[cardIndx].BoardTemperature = tmpInt
			} else {
				this.ShannonStatuses[cardIndx].BoardTemperature = -1
			}
		case "flash_temperature":
			tempArr := strings.Fields(tArr[1])
			tmpInt, err = strconv.ParseInt(tempArr[0], 10, 64)
			if err == nil {
				this.ShannonStatuses[cardIndx].FlashTemperature = tmpInt
			} else {
				this.ShannonStatuses[cardIndx].FlashTemperature = -1
			}
		case "dynamic_bad_blocks":
			tmpInt, err = strconv.ParseInt(tArr[1], 10, 64)
			if err == nil {
				this.ShannonStatuses[cardIndx].DynamicBadBlocks = tmpInt
			} else {
				this.ShannonStatuses[cardIndx].DynamicBadBlocks = -1
			}
		case "media_status":
			if tArr[1] == "Healthy" {
				this.ShannonStatuses[cardIndx].IsMediaStatusHealthy = 1
			} else {
				this.ShannonStatuses[cardIndx].IsMediaStatusHealthy = 0
			}
		case "seu_flag":
			tArr[1] = strings.ToLower(tArr[1])
			if tArr[1] == "normal" {
				this.ShannonStatuses[cardIndx].SeuFlag = C_SEU_FLAG_NORMAL
			} else if tArr[1] == "correctable" {
				this.ShannonStatuses[cardIndx].SeuFlag = C_SEU_FLAG_CORRECTABLE
			} else if tArr[1] == "uncorrectable" {
				this.ShannonStatuses[cardIndx].SeuFlag = C_SEU_FLAG_UNCORRECTABLE
			} else {
				this.ShannonStatuses[cardIndx].SeuFlag = C_SEU_FLAG_UNKNOWN
			}
		case "estimated_life_left":
			tArr[1] = strings.Trim(tArr[1], "%")
			tmpFloat, err = strconv.ParseFloat(tArr[1], 64)
			if err == nil {
				this.ShannonStatuses[cardIndx].EstimatedLifeLeft = tmpFloat
			} else {
				this.ShannonStatuses[cardIndx].EstimatedLifeLeft = -1
			}
		}
	}
	if int64(len(this.ShannonStatuses)) > this.ShannonCardsNumb {
		this.ShannonCardsNumb = int64(len(this.ShannonStatuses))
	}
	return

}

func (this *ShannonStatusAll) GetShannonSysFile(mountPoint string, metric string) string {
	f := path.Join("/sys/class/block", path.Base(mountPoint), "shannon", metric)
	if file.IsFile(f) {
		return f
	} else {
		return ""
	}
}

func (this *ShannonStatusAll) GetShannonSysFileContent(mountPoint string, metric string) string {
	mf := this.GetShannonSysFile(mountPoint, metric)
	if mf == "" {
		return mf
	}
	result, err := file.ToTrimString(mf)
	if err != nil {
		return ""
	}
	return result
}

func (this *ShannonStatusAll) GetShannonStatusValueFromSys() {
	var (
		i        int
		fContent string
		tmpInt   int64
		err      error
	)
	if this.ShannonCardsNumb <= 0 {
		return
	}
	for i = range this.ShannonStatuses {
		fContent = this.GetShannonSysFileContent(this.ShannonStatuses[i].MountPoint, "dynamic_bad_blkcnt")
		if fContent != "" {
			this.IfHasSysFile = true
			tmpInt, err = strconv.ParseInt(fContent, 10, 64)
			if err == nil {
				if tmpInt > this.ShannonStatuses[i].DynamicBadBlocks {
					this.ShannonStatuses[i].DynamicBadBlocks = tmpInt
				}
			}
		}
		//mode=0, readwrite
		fContent = this.GetShannonSysFileContent(this.ShannonStatuses[i].MountPoint, "access_mode")
		if fContent != "" {
			this.IfHasSysFile = true
			tArr := strings.Split(fContent, ",")
			if len(tArr) == 2 {
				if strings.ToLower(strings.TrimSpace(tArr[1])) == "readwrite" {
					this.ShannonStatuses[i].IsReadWriteable = 1
				} else {
					this.ShannonStatuses[i].IsReadWriteable = 0
				}
			}
		}

		fContent = this.GetShannonSysFileContent(this.ShannonStatuses[i].MountPoint, "seu_flag")
		if fContent != "" {
			this.IfHasSysFile = true
			seuFlag := strings.ToLower(fContent)
			if seuFlag == "normal" {
				this.ShannonStatuses[i].SeuFlag = C_SEU_FLAG_NORMAL
			} else if seuFlag == "correctable" {
				this.ShannonStatuses[i].SeuFlag = C_SEU_FLAG_CORRECTABLE
			} else if seuFlag == "uncorrectable" {
				this.ShannonStatuses[i].SeuFlag = C_SEU_FLAG_UNCORRECTABLE
			} else {
				this.ShannonStatuses[i].SeuFlag = C_SEU_FLAG_UNKNOWN
			}
		}

		fContent = this.GetShannonSysFileContent(this.ShannonStatuses[i].MountPoint, "seu_crc_error")
		if fContent != "" {
			this.IfHasSysFile = true
			tmpInt, err = strconv.ParseInt(fContent, 10, 64)
			if err == nil {
				this.ShannonStatuses[i].SeuCrcError = tmpInt
			} else {
				this.ShannonStatuses[i].SeuCrcError = -1
			}
		} else {
			this.ShannonStatuses[i].SeuCrcError = -1
		}

		fContent = this.GetShannonSysFileContent(this.ShannonStatuses[i].MountPoint, "seu_crc_error_history")
		if fContent != "" {
			this.IfHasSysFile = true
			tmpInt, err = strconv.ParseInt(fContent, 10, 64)
			if err == nil {
				this.ShannonStatuses[i].SeuCrcErrorHistory = tmpInt
			} else {
				this.ShannonStatuses[i].SeuCrcErrorHistory = -1
			}
		} else {
			this.ShannonStatuses[i].SeuCrcErrorHistory = -1
		}

		fContent = this.GetShannonSysFileContent(this.ShannonStatuses[i].MountPoint, "seu_ecc_error")
		if fContent != "" {
			this.IfHasSysFile = true
			tmpInt, err = strconv.ParseInt(fContent, 10, 64)
			if err == nil {
				this.ShannonStatuses[i].SeuEccError = tmpInt
			} else {
				this.ShannonStatuses[i].SeuEccError = -1
			}
		} else {
			this.ShannonStatuses[i].SeuEccError = -1
		}

		fContent = this.GetShannonSysFileContent(this.ShannonStatuses[i].MountPoint, "seu_ecc_error_history")
		if fContent != "" {
			this.IfHasSysFile = true
			tmpInt, err = strconv.ParseInt(fContent, 10, 64)
			if err == nil {
				this.ShannonStatuses[i].SeuEccErrorHistory = tmpInt
			} else {
				this.ShannonStatuses[i].SeuEccErrorHistory = -1
			}
		} else {
			this.ShannonStatuses[i].SeuEccErrorHistory = -1
		}

		fContent = this.GetShannonSysFileContent(this.ShannonStatuses[i].MountPoint, "static_bad_blkcnt")
		if fContent != "" {
			this.IfHasSysFile = true
			tmpInt, err = strconv.ParseInt(fContent, 10, 64)
			if err == nil {
				this.ShannonStatuses[i].StaticBadBlkCnt = tmpInt
			} else {
				this.ShannonStatuses[i].StaticBadBlkCnt = -1
			}
		} else {
			this.ShannonStatuses[i].StaticBadBlkCnt = -1
		}

		fContent = this.GetShannonSysFileContent(this.ShannonStatuses[i].MountPoint, "free_blkcnt")
		if fContent != "" {
			this.IfHasSysFile = true
			tmpInt, err = strconv.ParseInt(fContent, 10, 64)
			if err == nil {
				this.ShannonStatuses[i].FreeBlkCnt = tmpInt
			} else {
				this.ShannonStatuses[i].FreeBlkCnt = -1
			}
		} else {
			this.ShannonStatuses[i].FreeBlkCnt = -1
		}
	}
}

func (this *ShannonStatusAll) GetShannonStatusValueFromCheckShannon(timeout uint32) {
	var (
		cmd    string
		outMsg string
		errMsg string
		err    error
	)
	//sys file has target metric. check_shannon does not work for multi cards, so it should be dfa
	if this.IfHasSysFile || this.ShannonCardsNumb > 1 || this.ShannonCardsNumb <= 0 {
		return
	}

	cmd = "sudo /usr/local/zabbix/scripts/shannon/check_shannon -s"
	outMsg, errMsg = RunPcieCardStatusCmd(timeout, cmd)
	if errMsg == "" && outMsg != "" {
		seuFlag := strings.ToLower(outMsg)
		if seuFlag == "normal" {
			this.ShannonStatuses[0].SeuFlag = C_SEU_FLAG_NORMAL
		} else if seuFlag == "correctable" {
			this.ShannonStatuses[0].SeuFlag = C_SEU_FLAG_CORRECTABLE
		} else if seuFlag == "uncorrectable" {
			this.ShannonStatuses[0].SeuFlag = C_SEU_FLAG_UNCORRECTABLE
		} else {
			this.ShannonStatuses[0].SeuFlag = C_SEU_FLAG_UNKNOWN
		}
	}

	cmd = "sudo /usr/local/zabbix/scripts/shannon/check_shannon -j -g dbbk"
	outMsg, errMsg = RunPcieCardStatusCmd(timeout, cmd)
	if errMsg == "" && outMsg != "" {
		var dbbk map[string][]map[string]interface{}
		err = json.Unmarshal([]byte(outMsg), &dbbk)
		if err != nil {
			if this.ShannonStatuses[0].DynamicBadBlocks == 0 {
				this.ShannonStatuses[0].DynamicBadBlocks = -1
			}
		} else {
			_, ok := dbbk["shannon-stat"]
			if ok {
				_, ok := dbbk["shannon-stat"][0]["dynamic_bad_blkcnt"]
				if ok {
					tmpArr, ok := dbbk["shannon-stat"][0]["dynamic_bad_blkcnt"].([]string)
					if ok && len(tmpArr) > 0 {
						tmpInt, err := strconv.ParseInt(tmpArr[0], 10, 64)
						if err == nil && this.ShannonStatuses[0].DynamicBadBlocks > tmpInt {
							this.ShannonStatuses[0].DynamicBadBlocks = tmpInt
						}
					}
				}
			}
		}
	}

	cmd = "sudo /usr/local/zabbix/scripts/shannon/check_shannon -j -g seu"
	outMsg, errMsg = RunPcieCardStatusCmd(timeout, cmd)
	if errMsg == "" && outMsg != "" {
		var seu map[string][]map[string]interface{}
		err = json.Unmarshal([]byte(outMsg), &seu)
		if err != nil {
			this.ShannonStatuses[0].SeuCrcError = -1
			this.ShannonStatuses[0].SeuCrcErrorHistory = -1
			this.ShannonStatuses[0].SeuEccError = -1
			this.ShannonStatuses[0].SeuEccErrorHistory = -1
		} else {
			_, ok := seu["shannon-stat"]
			if ok {
				for k := range seu["shannon-stat"][0] {
					switch k {
					case "access_mode":
						tmpArr, ok := seu["shannon-stat"][0][k].([]string)
						if ok && len(tmpArr) > 0 {
							if strings.ToLower(strings.TrimSpace(tmpArr[0])) == "readwrite" {
								this.ShannonStatuses[0].IsReadWriteable = 1
							} else {
								this.ShannonStatuses[0].IsReadWriteable = 0
							}
						}
					case "seu_crc_error":
						tmpArr, ok := seu["shannon-stat"][0][k].([]string)
						if ok && len(tmpArr) > 0 {
							tmpInt, err := strconv.ParseInt(tmpArr[0], 10, 64)
							if err == nil {
								if this.ShannonStatuses[0].SeuCrcError > tmpInt {
									this.ShannonStatuses[0].SeuCrcError = tmpInt
								}
							} else if this.ShannonStatuses[0].SeuCrcError == 0 {
								this.ShannonStatuses[0].SeuCrcError = -1
							}
						}
					case "seu_crc_error_history":
						tmpStr, ok := seu["shannon-stat"][0][k].(string)
						if ok {
							tmpInt, err := strconv.ParseInt(tmpStr, 10, 64)
							if err == nil {
								if this.ShannonStatuses[0].SeuCrcErrorHistory > tmpInt {
									this.ShannonStatuses[0].SeuCrcErrorHistory = tmpInt
								}
							} else if this.ShannonStatuses[0].SeuCrcErrorHistory == 0 {
								this.ShannonStatuses[0].SeuCrcError = -1
							}
						}
					case "seu_ecc_error":
						tmpArr, ok := seu["shannon-stat"][0][k].([]string)
						if ok && len(tmpArr) > 0 {
							tmpInt, err := strconv.ParseInt(tmpArr[0], 10, 64)
							if err == nil {
								if this.ShannonStatuses[0].SeuEccError > tmpInt {
									this.ShannonStatuses[0].SeuEccError = tmpInt
								}
							} else if this.ShannonStatuses[0].SeuEccError == 0 {
								this.ShannonStatuses[0].SeuEccError = -1
							}
						}
					case "seu_ecc_error_history":
						tmpStr, ok := seu["shannon-stat"][0][k].(string)
						if ok {
							tmpInt, err := strconv.ParseInt(tmpStr, 10, 64)
							if err == nil {
								if this.ShannonStatuses[0].SeuCrcErrorHistory > tmpInt {
									this.ShannonStatuses[0].SeuCrcErrorHistory = tmpInt
								}
							} else if this.ShannonStatuses[0].SeuCrcErrorHistory == 0 {
								this.ShannonStatuses[0].SeuCrcError = -1
							}
						}
					}
				}
			}
		}
	}
}

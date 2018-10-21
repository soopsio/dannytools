package mypciecard

import (
	"dannytools/mystr"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	//"github.com/davecgh/go-spew/spew"
)

var (
	Fio_regexp_card_cnt *regexp.Regexp = regexp.MustCompile(`Found\s+(\d+)\s+ioMemory\s+device`)
)

type FusionStatusAll struct {
	Fio_error_warning_linecnt int64

	StatusJsonOut   string
	StatusNormalOut string

	FusionCardsNumb int64
	FusionStatuses  []*FusionStatusOne
}

type FusionStatusOne struct {
	MountPoint string

	Fio_pcie_correctable_errors int64
	Fio_pcie_errors             int64
	Fio_pcie_fatal_errors       int64
	Fio_pcie_nonfatal_errors    int64

	Fio_rated_writes_remaining_percent float64
	Fio_reserve_space_pct              float64
	Fio_temp_internal_deg_c            float64

	Fio_reserve_status_healthy int8
}

func (this *FusionStatusOne) SetUnknownValueJsonOutOne() {
	this.Fio_pcie_correctable_errors = -1
	this.Fio_pcie_errors = -1
	this.Fio_pcie_fatal_errors = -1
	this.Fio_pcie_nonfatal_errors = -1
	this.Fio_rated_writes_remaining_percent = -1
	this.Fio_reserve_space_pct = -1
	this.Fio_reserve_status_healthy = -1
	this.Fio_temp_internal_deg_c = -1
}

func (this *FusionStatusAll) SetUnknownValueJsonOutAll() {
	for i := range this.FusionStatuses {
		this.FusionStatuses[i].SetUnknownValueJsonOutOne()
	}
}

func (this *FusionStatusAll) SetUnknownValueNormalOut() {
	this.FusionCardsNumb = -1
	this.Fio_error_warning_linecnt = -1
}

func (this *FusionStatusAll) GetFusionStatusJson(timeOut uint32) string {
	cmd := "sudo fio-status -fj"
	errMsg := ""
	this.StatusJsonOut, errMsg = RunPcieCardStatusCmd(timeOut, cmd)
	return errMsg
}

func (this *FusionStatusAll) GetFusionStatusNormal(timeOut uint32) string {
	cmd := "sudo fio-status -a"
	errMsg := ""
	this.StatusNormalOut, errMsg = RunPcieCardStatusCmd(timeOut, cmd)
	return errMsg
}

func (this *FusionStatusAll) ParseFusionNormalStr() {
	var arr []string
	this.FusionCardsNumb = 0
	if this.StatusNormalOut == "" {
		this.SetUnknownValueNormalOut()
		return
	}
	arr = strings.Split(this.StatusNormalOut, "\n")
	for _, line := range arr {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		//Found 2 ioMemory devices in this system

		arr = Fio_regexp_card_cnt.FindStringSubmatch(line)
		if len(arr) > 1 {
			tmpInt, err := strconv.ParseInt(arr[1], 10, 64)
			if err == nil {
				this.FusionCardsNumb = tmpInt
			}
			continue
		}
		line = strings.ToLower(line)
		if strings.Contains(line, "error") {
			this.Fio_error_warning_linecnt += 1
			continue
		}
		if strings.Contains(line, "warning") {
			this.Fio_error_warning_linecnt += 1
			continue
		}
	}
}

func (this *FusionStatusAll) ParseFusionJsonStr() error {
	var (
		mp     map[string]interface{}
		err    error
		tmpStr string
	)

	err = json.Unmarshal([]byte(this.StatusJsonOut), &mp)
	if err != nil {
		this.SetUnknownValueJsonOutAll()
		return err
	}
	//s := spew.Sdump(mp)
	adapter, ok := mp["adapter"]
	if !ok {
		this.SetUnknownValueJsonOutAll()
		return fmt.Errorf("no adapter")
	}
	adaArrIface, ok := adapter.([]interface{})
	if !ok {
		this.SetUnknownValueJsonOutAll()
		return fmt.Errorf("cannot convert map['adapter'] to []interface{}")
		//return fmt.Errorf("cannot convert map['adapter'] to []interface{}: %s", s)
	}
	if len(adaArrIface) == 0 {
		return fmt.Errorf("no adapter found")
	}

	for _, oneAdaIface := range adaArrIface {
		//spew.Dump(oneAdaIface)
		oneAda, ok := oneAdaIface.(map[string]interface{})
		if !ok {
			continue
		}
		iomem, ok := oneAda["iomemory"]
		if !ok {
			continue
		}
		ioArrIface, ok := iomem.([]interface{})
		if !ok || len(ioArrIface) == 0 {
			continue
		}
		for _, oneIoArr := range ioArrIface {

			ioArrZero, ok := oneIoArr.(map[string]interface{})
			if !ok || len(ioArrZero) == 0 {
				continue
			}
			//fmt.Println("------ioArrZero------")
			//spew.Dump(ioArrZero)
			oneSts := &FusionStatusOne{}
			oneSts.Fio_pcie_correctable_errors = mystr.ParseStringInterfaceToInt64("has_pcie_correctable_errors", ioArrZero)
			oneSts.Fio_pcie_errors = mystr.ParseStringInterfaceToInt64("has_pcie_errors", ioArrZero)
			oneSts.Fio_pcie_fatal_errors = mystr.ParseStringInterfaceToInt64("has_pcie_fatal_errors", ioArrZero)
			oneSts.Fio_pcie_nonfatal_errors = mystr.ParseStringInterfaceToInt64("has_pcie_nonfatal_errors", ioArrZero)
			oneSts.Fio_temp_internal_deg_c = mystr.ParseStringInterfaceToFloat64("temp_internal_deg_c", ioArrZero)
			oneSts.Fio_rated_writes_remaining_percent = mystr.ParseStringInterfaceToFloat64("rated_writes_remaining_percent", ioArrZero)
			oneSts.Fio_reserve_space_pct = mystr.ParseStringInterfaceToFloat64("reserve_space_pct", ioArrZero)

			_, ok = ioArrZero["reserve_status"]
			if ok {
				tmpStr, ok = ioArrZero["reserve_status"].(string)
				if ok {
					tmpStr = strings.ToLower(tmpStr)
					if tmpStr != "healthy" {
						oneSts.Fio_reserve_status_healthy = 0
					} else {
						oneSts.Fio_reserve_status_healthy = 1
					}
				} else {
					oneSts.Fio_reserve_status_healthy = -1
				}
			} else {
				oneSts.Fio_reserve_status_healthy = -1
			}

			_, ok = ioArrZero["vsu"]
			if ok {
				vsuArr, ok := (ioArrZero["vsu"]).([]interface{})
				//fmt.Println("------vsuArr------")
				//spew.Dump(vsuArr)
				if ok {
					tmpArr, ok := (vsuArr[0]).(map[string]interface{})
					//fmt.Println("------tmpArr------")
					//spew.Dump(tmpArr)
					if ok {
						if _, ok := tmpArr["device_path"]; ok {
							tmpS, ok := (tmpArr["device_path"]).(string)
							if ok {
								oneSts.MountPoint = tmpS
							}
						}
					}
				}

			}
			this.FusionStatuses = append(this.FusionStatuses, oneSts)
			//spew.Dump(this.FusionStatuses)
		}
	}

	return nil
}

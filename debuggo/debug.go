package debuggo

import (
	"dannytools/constvar"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"time"

	fModel "github.com/open-falcon/falcon-plus/common/model"
)

const (
	c_data_type_gauge   = "GAUGE"
	c_data_type_counter = "COUNTER"
)

func GetProfileName(prefix string) string {
	return fmt.Sprintf("%s-%s.bin", prefix, time.Now().Format(constvar.DATETIME_FORMAT_NOSPACE_FILE))
}

func StartMemProfile(pRate int) {
	if pRate > 0 {
		runtime.MemProfileRate = pRate
	}
}

func StopMemProfile() {

	runtime.MemProfileRate = 0

}

func WriteMemProfileToFile(mFile string) {
	if mFile != "" {
		runtime.GC()
		f, err := os.Create(mFile)
		if err != nil {
			if f != nil {
				f.Close()
			}
			fmt.Fprintf(os.Stderr, "Can not create mem profile output file %s: %s\n", mFile, err)
			return
		}
		fmt.Fprintf(os.Stdout, "%s start to write memory profile to file %s\n", time.Now().Format(constvar.DATETIME_FORMAT_FRACTION), mFile)
		if err = pprof.WriteHeapProfile(f); err != nil {
			f.Close()
			fmt.Fprintf(os.Stderr, "Can not write memory profile to file %s: %s\n", mFile, err)
		} else {
			fmt.Fprintf(os.Stdout, "%s successfully write memory profile to file %s\n", time.Now().Format(constvar.DATETIME_FORMAT_FRACTION), mFile)
			f.Close()
		}

	}
}

func GetAllMemStats(ifFreeMem bool) string {
	if ifFreeMem {
		debug.FreeOSMemory()
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	str := fmt.Sprintf("%s AllocBytes=%d,SysBytes=%d,TotalAllocBytesCum=%d,LiveObj=%d,MallocObjCum=%d,FreeObjCum=%d,HeapAllocBytes=%d,HeapAllocObj=%d\n",
		time.Now().Format(constvar.DATETIME_FORMAT_FRACTION),
		ms.Alloc, ms.Sys, ms.TotalAlloc, ms.Mallocs-ms.Frees, ms.Mallocs, ms.Frees, ms.HeapAlloc, ms.HeapObjects)
	return str
}

func GetMemProfileMetrics(prefix string, ifPrint bool) []fModel.MetricValue {

	var (
		ms   runtime.MemStats
		data []fModel.MetricValue
	)
	runtime.ReadMemStats(&ms)
	if ifPrint {
		fmt.Printf("%s AllocBytes=%d,SysBytes=%d,TotalAllocBytesCum=%d,LiveObj=%d,MallocObjCum=%d,FreeObjCum=%d,HeapAllocBytes=%d,HeapAllocObj=%d\n",
			time.Now().Format(constvar.DATETIME_FORMAT_FRACTION),
			ms.Alloc, ms.Sys, ms.TotalAlloc, ms.Mallocs-ms.Frees, ms.Mallocs, ms.Frees, ms.HeapAlloc, ms.HeapObjects)
		fmt.Printf("number of goroutine=%d\n", runtime.NumGoroutine())
	}

	data = append(data,
		fModel.MetricValue{
			//Endpoint: ip,
			Metric: fmt.Sprintf("%s.GoroutineCnt", prefix),
			Value:  runtime.NumGoroutine(),
			Type:   c_data_type_gauge,
			//Tags:     tg,
		},
		fModel.MetricValue{
			//Endpoint: ip,
			Metric: fmt.Sprintf("%s.AllocBytes", prefix),
			Value:  ms.Alloc,
			Type:   c_data_type_gauge,
			//Tags:     tg,
		},
		fModel.MetricValue{
			//Endpoint: ip,
			Metric: fmt.Sprintf("%s.SysBytes", prefix),
			Value:  ms.Sys,
			Type:   c_data_type_gauge,
			//Tags:     tg,
		},
		fModel.MetricValue{
			//Endpoint: ip,
			Metric: fmt.Sprintf("%s.TotalAllocBytesCum", prefix),
			Value:  ms.TotalAlloc,
			Type:   c_data_type_counter,
			//Tags:     tg,
		},
		fModel.MetricValue{
			//Endpoint: ip,
			Metric: fmt.Sprintf("%s.MallocObjCum", prefix),
			Value:  ms.Mallocs,
			Type:   c_data_type_counter,
			//Tags:     tg,
		},
		fModel.MetricValue{
			//Endpoint: ip,
			Metric: fmt.Sprintf("%s.FreeObjCum", prefix),
			Value:  ms.Frees,
			Type:   c_data_type_counter,
			//Tags:     tg,
		},
		fModel.MetricValue{
			//Endpoint: ip,
			Metric: fmt.Sprintf("%s.LiveObj", prefix),
			Value:  ms.Mallocs - ms.Frees,
			Type:   c_data_type_gauge,
			//Tags:     tg,
		},
		fModel.MetricValue{
			//Endpoint: ip,
			Metric: fmt.Sprintf("%s.HeapAllocBytes", prefix),
			Value:  ms.HeapAlloc,
			Type:   c_data_type_gauge,
			//Tags:     tg,
		},

		fModel.MetricValue{
			//Endpoint: ip,
			Metric: fmt.Sprintf("%s.HeapAllocObj", prefix),
			Value:  ms.HeapObjects,
			Type:   c_data_type_gauge,
			//Tags:     tg,
		},

		fModel.MetricValue{
			//Endpoint: ip,
			Metric: fmt.Sprintf("%s.HeapSysBytes", prefix),
			Value:  ms.HeapSys,
			Type:   c_data_type_gauge,
			//Tags:     tg,
		},
		fModel.MetricValue{
			//Endpoint: ip,
			Metric: fmt.Sprintf("%s.HeapInuseBytes", prefix),
			Value:  ms.HeapInuse,
			Type:   c_data_type_gauge,
			//Tags:     tg,
		},
		fModel.MetricValue{
			//Endpoint: ip,
			Metric: fmt.Sprintf("%s.HeapIdleBytes", prefix),
			Value:  ms.HeapIdle,
			Type:   c_data_type_gauge,
			//Tags:     tg,
		},
		fModel.MetricValue{
			//Endpoint: ip,
			Metric: fmt.Sprintf("%s.HeapReleasedBytes", prefix),
			Value:  ms.HeapReleased,
			Type:   c_data_type_gauge,
			//Tags:     tg,
		},
	)
	return data

}

func GetMemProfileMetricsNames(prefix string) []string {
	metrics := []string{
		"GoroutineCnt",
		"AllocBytes",
		"SysBytes",
		"TotalAllocBytesCum",
		"MallocObjCum",
		"FreeObjCum",
		"LiveObj",
		"HeapAllocBytes",
		"HeapAllocObj",
		"HeapSysBytes",
		"HeapInuseBytes",
		"HeapIdleBytes",
		"HeapReleasedBytes",
	}
	for i := range metrics {
		metrics[i] = fmt.Sprintf("%s.%s", prefix, metrics[i])
	}
	return metrics
}

/*
func StartCPUProfile() {
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Can not create cpu profile output file: %s",
				err)
			return
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "Can not start cpu profile: %s", err)
			f.Close()
			return
		}
	}
}

func StopCPUProfile() {
	if *cpuProfile != "" {
		pprof.StopCPUProfile() // 把记录的概要信息写到已指定的文件
	}
}
*/

/*

func StartBlockProfile() {
	if *blockProfile != "" && *blockProfileRate > 0 {
		runtime.SetBlockProfileRate(*blockProfileRate)
	}
}

func StopBlockProfile() {
	if *blockProfile != "" && *blockProfileRate >= 0 {
		f, err := os.Create(*blockProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Can not create block profile output file: %s", err)
			return
		}
		if err = pprof.Lookup("block").WriteTo(f, 0); err != nil {
			fmt.Fprintf(os.Stderr, "Can not write %s: %s", *blockProfile, err)
		}
		f.Close()
	}
}

func SaveProfile(workDir string, profileName string, ptype ProfileType, debug int) {
	absWorkDir := getAbsFilePath(workDir)
	if profileName == "" {
		profileName = string(ptype)
	}
	profilePath := filepath.Join(absWorkDir, profileName)
	f, err := os.Create(profilePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can not create profile output file: %s", err)
		return
	}
	if err = pprof.Lookup(string(ptype)).WriteTo(f, debug); err != nil {
		fmt.Fprintf(os.Stderr, "Can not write %s: %s", profilePath, err)
	}
	f.Close()
}
*/

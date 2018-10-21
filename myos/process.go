package myos

import (
	"dannytools/ehand"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/process"
)

type CpuTimes struct {
	TheCPUTimes *cpu.TimesStat
	TheCPUTime  time.Time
}

type ProcessStats struct {
	FirstErr bool

	StartTime       int64 // milliseconds
	GetStartTimeErr bool

	MemPercent   float64
	GetMemPerErr bool

	GetMemErr bool
	MemRSS    uint64
	MemVMS    uint64
	MemSwap   uint64

	GetIoErr  bool
	IoRdcnt   uint64
	IoWrcnt   uint64
	IoRdBytes uint64
	IoWrBytes uint64

	GetCtxErr            bool
	CtxSwitchVoluntary   int64
	CtxSwitchInvoluntary int64

	/*
		NetSentBytes      uint64
		NetRecvBytes      uint64
		NetSentPackets    uint64
		NetRecvPackets    uint64
		NetErrIn          uint64
		NetErrOut         uint64
		NetDropPacketsIn  uint64
		NetDropPacketsOut uint64
		NetErrFifoIn      uint64
		NetErrFifoOut     uint64
	*/
}

func CalculateCpuPercent(t1, t2 *cpu.TimesStat, delta float64, numcpu int) float64 {
	if delta == 0 {
		return 0
	}
	delta_proc := t2.Total() - t1.Total()
	overall_percent := ((delta_proc / delta) * 100) * float64(numcpu)
	return overall_percent
}

func GetProcessCpuInterval(p *process.Process, interval uint32) (float64, error) {
	cpu, err := p.Percent(time.Duration(interval) * time.Second)
	if err != nil {
		return -1, err
	}
	return cpu, nil
}

func GetProcessCpuPercentInterval(pid int32, interval uint32) (float64, error) {

	p, err := process.NewProcess(pid)

	if err != nil {
		return -1, err
	}

	return GetProcessCpuInterval(p, interval)
}

func GetProcessCpuTimes(pid int32) (CpuTimes, error) {
	var (
		c CpuTimes
	)
	p, err := process.NewProcess(pid)

	if err != nil {
		return c, err
	}

	cpuStats, err := p.Times()
	if err != nil {
		return c, err
	} else {
		c.TheCPUTime = time.Now()
		c.TheCPUTimes = cpuStats
		return c, nil
		//return CpuTimes{TheCPUTime: time.Now(), TheCPUTimes: cpuStats}, nil
	}
}

func GetProcessStartTime(pid int32) (int64, error) {
	p, err := process.NewProcess(pid)
	if err != nil {
		return 0, err
	}
	return p.CreateTime()
}

func GetProcessStatsWithoutCpu(pid int32) (ProcessStats, error) {
	var (
		err error
		sts ProcessStats = ProcessStats{
			FirstErr:        false,
			GetCtxErr:       false,
			GetMemErr:       false,
			GetMemPerErr:    false,
			GetIoErr:        false,
			GetStartTimeErr: false,
		}
	)
	p, err := process.NewProcess(pid)

	if err != nil {
		sts.FirstErr = true
		return sts, ehand.WithStackError(err)
	}

	mem, err := p.MemoryInfo()
	if err != nil {
		sts.GetMemErr = true

	} else {
		sts.MemRSS = mem.RSS
		sts.MemVMS = mem.VMS
		sts.MemSwap = mem.Swap
	}
	/*
		sts.StartTime, err = p.CreateTime()
		if err != nil {
			sts.GetStartTimeErr = true
		}
	*/

	/*
		memPer, err := p.MemoryPercent()
		if err != nil {
			sts.GetMemPerErr = true
		} else {
			sts.MemPercent = float64(memPer)
		}

		// mysql started by sudo, need to root privilege to read
		io, err := p.IOCounters()
		if err != nil {
			sts.GetIoErr = true
		} else {
			sts.IoRdBytes = io.ReadBytes
			sts.IoRdcnt = io.ReadCount
			sts.IoWrBytes = io.WriteBytes
			sts.IoWrcnt = io.WriteCount
		}

		ctx, err := p.NumCtxSwitches()
		if err != nil {
			sts.GetCtxErr = true
		} else {
			sts.CtxSwitchInvoluntary = ctx.Involuntary
			sts.CtxSwitchVoluntary = ctx.Voluntary
		}
	*/
	return sts, nil
}

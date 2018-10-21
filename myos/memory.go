package myos

import (
	"dannytools/ehand"

	"github.com/shirou/gopsutil/mem"
)

type OSMemStats struct {
	SwapTotal       uint64
	SwapUsed        uint64
	SwapFree        uint64
	SwapUsedPercent float64
	SwapIn          uint64
	SwapOut         uint64

	MemTotal       uint64
	MemAvailable   uint64
	MemUsed        uint64
	MemUsedPercent float64
	MemBuffers     uint64
	MemCached      uint64
}

func GetOsMemStats() (OSMemStats, error) {
	ms := OSMemStats{}
	swap, err := mem.SwapMemory()
	if err != nil {
		return ms, ehand.WithStackError(err)
	}
	ms.SwapFree = swap.Free
	ms.SwapIn = swap.Sin
	ms.SwapOut = swap.Sout
	ms.SwapTotal = swap.Total
	ms.SwapUsed = swap.Used
	ms.SwapUsedPercent = swap.UsedPercent

	m, err := mem.VirtualMemory()
	if err != nil {
		return ms, ehand.WithStackError(err)
	}
	ms.MemTotal = m.Total
	ms.MemAvailable = m.Available
	ms.MemUsed = m.Used
	ms.MemUsedPercent = m.UsedPercent
	ms.MemBuffers = m.Buffers
	ms.MemCached = m.Cached

	return ms, nil
}

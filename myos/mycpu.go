package myos

import (
	"github.com/shirou/gopsutil/cpu"
)

type PercentPerCpuPerMetric struct {
	User      float64 `json:"user"`
	System    float64 `json:"system"`
	Idle      float64 `json:"idle"`
	Nice      float64 `json:"nice"`
	Iowait    float64 `json:"iowait"`
	Irq       float64 `json:"irq"`
	Softirq   float64 `json:"softirq"`
	Steal     float64 `json:"steal"`
	Guest     float64 `json:"guest"`
	GuestNice float64 `json:"guestNice"`
	Stolen    float64 `json:"stolen"`
}

type MaxPerCpuPerMetric struct {
	Name   string
	Value  float64
	CpuIdx int
}

func SumCpuTimePerCpu(t cpu.TimesStat) float64 {
	return t.User + t.System + t.Nice + t.Iowait + t.Irq +
		t.Softirq + t.Steal + t.Guest + t.GuestNice + t.Stolen + t.Idle
}

func CheckGreaterZero(f float64) float64 {
	if f < 0 {
		return 0
	} else {
		return f
	}
}

func CalculateCpuPercentPerCpuPerMetric(cpuTs1, cpuTs2 []cpu.TimesStat) []PercentPerCpuPerMetric {
	if len(cpuTs1) != len(cpuTs2) {
		return nil
	}
	percent := make([]PercentPerCpuPerMetric, len(cpuTs1))
	for i, _ := range cpuTs1 {
		sumDelta := SumCpuTimePerCpu(cpuTs2[i]) - SumCpuTimePerCpu(cpuTs1[i])
		if sumDelta <= 0 {
			return nil
		}
		percent[i] = PercentPerCpuPerMetric{
			User:      CheckGreaterZero(cpuTs2[i].User-cpuTs1[i].User) / sumDelta * 100,
			System:    CheckGreaterZero(cpuTs2[i].System-cpuTs1[i].System) / sumDelta * 100,
			Idle:      CheckGreaterZero(cpuTs2[i].Idle-cpuTs1[i].Idle) / sumDelta * 100,
			Nice:      CheckGreaterZero(cpuTs2[i].Nice-cpuTs1[i].Nice) / sumDelta * 100,
			Iowait:    CheckGreaterZero(cpuTs2[i].Iowait-cpuTs1[i].Iowait) / sumDelta * 100,
			Irq:       CheckGreaterZero(cpuTs2[i].Irq-cpuTs1[i].Irq) / sumDelta * 100,
			Softirq:   CheckGreaterZero(cpuTs2[i].Softirq-cpuTs1[i].Softirq) / sumDelta * 100,
			Steal:     CheckGreaterZero(cpuTs2[i].Steal-cpuTs1[i].Steal) / sumDelta * 100,
			Guest:     CheckGreaterZero(cpuTs2[i].Guest-cpuTs1[i].Guest) / sumDelta * 100,
			GuestNice: CheckGreaterZero(cpuTs2[i].GuestNice-cpuTs1[i].GuestNice) / sumDelta * 100,
			Stolen:    CheckGreaterZero(cpuTs2[i].Stolen-cpuTs1[i].Stolen) / sumDelta * 100,
		}
	}
	return percent
}

func GetMaxPerCpuPerMetric(p []PercentPerCpuPerMetric) map[string]*MaxPerCpuPerMetric {
	m := map[string]*MaxPerCpuPerMetric{}
	for i, _ := range p {
		if _, ok := m["User"]; !ok {
			m["User"] = &MaxPerCpuPerMetric{Name: "User", Value: p[i].User, CpuIdx: i}
		}
		if p[i].User > m["User"].Value {
			m["User"].Value = p[i].User
			m["User"].CpuIdx = i
		}

		if _, ok := m["System"]; !ok {
			m["System"] = &MaxPerCpuPerMetric{Name: "System", Value: p[i].System, CpuIdx: i}
		}
		if p[i].System > m["System"].Value {
			m["System"].Value = p[i].System
			m["System"].CpuIdx = i
		}

		if _, ok := m["Idle"]; !ok {
			m["Idle"] = &MaxPerCpuPerMetric{Name: "Idle", Value: p[i].Idle, CpuIdx: i}
		}
		// this is min
		if p[i].Idle < m["Idle"].Value {
			m["Idle"].Value = p[i].Idle
			m["Idle"].CpuIdx = i
		}

		if _, ok := m["Nice"]; !ok {
			m["Nice"] = &MaxPerCpuPerMetric{Name: "Nice", Value: p[i].Nice, CpuIdx: i}
		}
		if p[i].Nice > m["Nice"].Value {
			m["Nice"].Value = p[i].Nice
			m["Nice"].CpuIdx = i
		}

		if _, ok := m["Iowait"]; !ok {
			m["Iowait"] = &MaxPerCpuPerMetric{Name: "Iowait", Value: p[i].Iowait, CpuIdx: i}
		}
		if p[i].Iowait > m["Iowait"].Value {
			m["Iowait"].Value = p[i].Iowait
			m["Iowait"].CpuIdx = i
		}

		if _, ok := m["Irq"]; !ok {
			m["Irq"] = &MaxPerCpuPerMetric{Name: "Irq", Value: p[i].Irq, CpuIdx: i}
		}
		if p[i].Irq > m["Irq"].Value {
			m["Irq"].Value = p[i].Irq
			m["Irq"].CpuIdx = i
		}

		if _, ok := m["Softirq"]; !ok {
			m["Softirq"] = &MaxPerCpuPerMetric{Name: "Softirq", Value: p[i].Softirq, CpuIdx: i}
		}
		if p[i].Softirq > m["Softirq"].Value {
			m["Softirq"].Value = p[i].Softirq
			m["Softirq"].CpuIdx = i
		}

		if _, ok := m["Steal"]; !ok {
			m["Steal"] = &MaxPerCpuPerMetric{Name: "Steal", Value: p[i].Steal, CpuIdx: i}
		}
		if p[i].Steal > m["Steal"].Value {
			m["Steal"].Value = p[i].Steal
			m["Steal"].CpuIdx = i
		}

		if _, ok := m["Guest"]; !ok {
			m["Guest"] = &MaxPerCpuPerMetric{Name: "Guest", Value: p[i].Guest, CpuIdx: i}
		}
		if p[i].Guest > m["Guest"].Value {
			m["Guest"].Value = p[i].Guest
			m["Guest"].CpuIdx = i
		}

		if _, ok := m["GuestNice"]; !ok {
			m["GuestNice"] = &MaxPerCpuPerMetric{Name: "GuestNice", Value: p[i].GuestNice, CpuIdx: i}
		}
		if p[i].GuestNice > m["GuestNice"].Value {
			m["GuestNice"].Value = p[i].GuestNice
			m["GuestNice"].CpuIdx = i
		}

		if _, ok := m["Stolen"]; !ok {
			m["Stolen"] = &MaxPerCpuPerMetric{Name: "Stolen", Value: p[i].Stolen, CpuIdx: i}
		}
		if p[i].Stolen > m["Stolen"].Value {
			m["Stolen"].Value = p[i].Stolen
			m["Stolen"].CpuIdx = i
		}

	}
	return m
}

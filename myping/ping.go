package myping

import (
	"fmt"
	"math"
	"time"

	"github.com/sparrc/go-ping"
)

// need to run as root
// interval and timout are in seconds
func CheckPingOk(addr string, interval int, count int, timeout int, lossRateOk float64) (bool, error) {
	pinger, err := ping.NewPinger(addr)
	if err != nil {
		return false, err
	}
	pinger.Interval = time.Duration(interval) * time.Second
	pinger.Count = count
	pinger.Timeout = time.Duration(timeout) * time.Second
	pinger.SetPrivileged(true)
	pinger.Run()
	sts := pinger.Statistics()
	if math.IsNaN(sts.PacketLoss) {
		return false, fmt.Errorf("ping error, PacketLoss is NaN. Pls check if the program is run as root")
	}
	if sts.PacketLoss == 0 {
		return true, nil
	} else if sts.PacketLoss >= lossRateOk {
		return false, nil
	} else {
		return true, nil
	}
}

package mytw

import (
	"dannytools/ehand"
	"dannytools/mymc"
	"encoding/json"

	kitsSlice "github.com/toolkits/slice"

	"github.com/youtube/vitess/go/memcache"
)

var (
	g_tw_unwanted_metrics []string = []string{"service", "source", "version", "uptime", "timestamp"}
)

func GetTwStatusString(tw *memcache.Connection) (string, error) {
	result, err := tw.SendCmdsAndGetOnlyOneLineResponse("status\r\n")

	if err != nil {
		return result, ehand.WithStackError(err)
	} else {
		return result, nil
	}
}

func GetTwStatusMap(addr string, timeout uint32, cmd string, wrTimeout uint32, rdTimeout uint32, readTo string) (map[string]uint64, error) {
	var (
		result map[string]uint64
		err    error
	)
	con, err := mymc.CreateMcConnectionTelnet(addr, timeout)
	if con != nil {
		defer con.Close()
	}
	if err != nil {
		return result, err
	}

	line, err := mymc.SendCmdAndReadResultTelnet(con, cmd, wrTimeout, rdTimeout, readTo)
	if err != nil {
		return result, err
	}
	result, err = ParseTwemproxyStatus(line)
	if err != nil {
		return result, err
	}
	return result, nil
}

/*
{
	"service": "nutcrackers",
	"source": "hostname",
	"version": "1.2.1",
	"uptime": 369,
	"timestamp": 1516871921,
	"total_connections": 19,
	"curr_connections": 7,
	"twem1": {
		"client_eof": 0,
		"client_err": 2,
		"client_connections": 0,
		"server_ejects": 0,
		"forward_error": 0,
		"fragments": 0,
		"master01": {
			"server_eof": 0,
			"server_err": 4,
			"server_timedout": 0,
			"server_connections": 0,
			"server_ejected_at": 0,
			"requests": 0,
			"request_bytes": 0,
			"responses": 0,
			"response_bytes": 0,
			"in_queue": 0,
			"in_queue_bytes": 0,
			"out_queue": 0,
			"out_queue_bytes": 0
		},
		"master02": {
			"server_eof": 0,
			"server_err": 4,
			"server_timedout": 0,
			"server_connections": 0,
			"server_ejected_at": 0,
			"requests": 0,
			"request_bytes": 0,
			"responses": 0,
			"response_bytes": 0,
			"in_queue": 0,
			"in_queue_bytes": 0,
			"out_queue": 0,
			"out_queue_bytes": 0
		}
	}
}
*/

func ParseTwemproxyStatus(line string) (map[string]uint64, error) {
	var (
		mp       map[string]interface{}
		err      error
		tmpFloat float64
		result   map[string]uint64 = map[string]uint64{}
		//ok      bool
	)
	err = json.Unmarshal([]byte(line), &mp)
	if err != nil {
		return result, err
	}
	for k1, v1 := range mp {
		if kitsSlice.ContainsString(g_tw_unwanted_metrics, k1) {
			continue
		}
		mp1, ok := v1.(map[string]interface{})
		if !ok {
			tmpFloat, ok = v1.(float64)
			if !ok {
				continue
			}
			result[k1] = uint64(tmpFloat)
			continue
		}
		for k2, v2 := range mp1 {
			mp2, ok := v2.(map[string]interface{})
			if !ok {
				tmpFloat, ok = v2.(float64)
				if !ok {
					continue
				}

				result[k2] += uint64(tmpFloat)
				continue
			}

			for k3, v3 := range mp2 {
				tmpFloat, ok = v3.(float64)
				if !ok {
					continue
				}
				result[k3] += uint64(tmpFloat)
			}
		}
	}
	return result, nil
}

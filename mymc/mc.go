package mymc

import (
	"fmt"
	"time"
	//"fmt"
	"dannytools/ehand"
	"strconv"
	"strings"

	kitsSlice "github.com/toolkits/slice"
	"github.com/youtube/vitess/go/memcache"
	"github.com/ziutek/telnet"
)

// timeout: millisecond
func CreateMcConnectionTelnet(addr string, timeout uint32) (*telnet.Conn, error) {
	con, err := telnet.DialTimeout("tcp", addr, time.Duration(timeout)*time.Millisecond)

	if err != nil {
		if con != nil {
			con.Close()
		}
		return nil, ehand.WithStackError(err)
	}
	return con, nil
}

// wrTimeout, rdTimeout: millisecond
func SendCmdAndReadResultTelnet(mcClient *telnet.Conn, cmd string, wrTimeout uint32, rdTimeout uint32, readTo string) (string, error) {
	if !strings.HasSuffix(cmd, "\r\n") {
		cmd = fmt.Sprintf("%s\r\n", cmd)
	}
	err := mcClient.SetDeadline(time.Now().Add(time.Duration(wrTimeout) * time.Millisecond))
	if err != nil {
		return "", ehand.WithStackError(err)
	}
	_, err = mcClient.Write([]byte(cmd))
	if err != nil {
		return "", ehand.WithStackError(err)
	}
	err = mcClient.SetReadDeadline(time.Now().Add(time.Duration(rdTimeout) * time.Millisecond))
	if err != nil {
		return "", ehand.WithStackError(err)
	}
	result, err := mcClient.ReadUntil(readTo)
	if err != nil {
		//fmt.Printf("mc command %s: %s\n", cmd, string(result))
		return "", ehand.WithStackError(err)
	}
	return strings.TrimSpace(string(result)), nil
}

func ParseMemcacheStats(lines string, metricsWanted []string) map[string]uint64 {
	var (
		err     error
		tmpUint uint64
		result  map[string]uint64 = map[string]uint64{}
	)
	lines = strings.TrimSpace(lines)
	arr := strings.Split(lines, "\n")
	for _, line := range arr {
		line = strings.TrimSpace(line)
		tmpArr := strings.Fields(line)
		if tmpArr[0] != "STAT" {
			continue
		}
		if !kitsSlice.ContainsString(metricsWanted, tmpArr[1]) {
			continue
		}
		tmpUint, err = strconv.ParseUint(tmpArr[2], 10, 64)
		if err != nil {
			continue // only want number metric
		}
		result[tmpArr[1]] = tmpUint
	}

	return result
}

func GetMemcacheStatsTelnet(addr string, timeout uint32, cmd string, metricsWanted []string, wrTimeout uint32, rdTimeout uint32, readTo string) (map[string]uint64, error) {
	var (
		result map[string]uint64
		err    error
	)
	con, err := CreateMcConnectionTelnet(addr, timeout)
	if con != nil {
		defer con.Close()
	}
	if err != nil {
		return result, err
	}

	lines, err := SendCmdAndReadResultTelnet(con, cmd, wrTimeout, rdTimeout, readTo)
	if err != nil {
		return result, err
	}
	result = ParseMemcacheStats(lines, metricsWanted)
	return result, nil
}

func GetMemcacheVersionTelnet(addr string, timeout uint32, wrTimeout uint32, rdTimeout uint32) (string, error) {
	con, err := CreateMcConnectionTelnet(addr, timeout)
	if con != nil {
		defer con.Close()
	}
	if err != nil {
		return "", err
	}
	return GetMemcacheVersionTelnetCon(con, wrTimeout, rdTimeout)
}

func GetMemcacheVersionTelnetCon(mcClient *telnet.Conn, wrTimeout uint32, rdTimeout uint32) (string, error) {
	return SendCmdAndReadResultTelnet(mcClient, "version", wrTimeout, rdTimeout, "\n")
}

// get version of memcached
func GetMemcacheVersion(mc *memcache.Connection) (result string, err error) {
	/*
		defer handleError(&err)
		mc.setDeadline()
		mc.writestrings("version\r\n")

		mc.flush()
		result = mc.readline()
		return result, err
	*/
	return mc.SendCmdsAndGetOnlyOneLineResponse("version\r\n")
}

// this func is added into "github.com/youtube/vitess/go/memcache"
// added by danny lai
/*
func (mc *Connection) SendCmdsAndGetOnlyOneLineResponse(cmd string) (result string, err error) {
	defer handleError(&err)
	mc.setDeadline()
	mc.writestrings(cmd)

	mc.flush()
	result = mc.readline()
	return result, err
}
*/

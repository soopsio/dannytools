package myfalcon

import (
	"dannytools/ehand"
	"dannytools/logging"
	"dannytools/mystr"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	//"github.com/davecgh/go-spew/spew"
	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
)

const (
	CSqlFmtGetDbaIps string = `select h.lanip 
		from allHosts as h inner join dbtypes as t on h.type=t.id 
		where t.name in (%s) and h.lanip is not null`
)

type DownIpSendTimes struct {
	Lock      *sync.RWMutex
	DownTimes map[string]int
}

// return if key exists
func (this *DownIpSendTimes) SetValue(key string, val int) bool {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	_, ok := this.DownTimes[key]
	this.DownTimes[key] = val
	return ok
}

// return if key exists
func (this *DownIpSendTimes) DeleteValue(key string) bool {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	_, ok := this.DownTimes[key]
	if ok {
		delete(this.DownTimes, key)
	}
	return ok
}

func GetAllDbaIps(dbTypes []string, dbCon *sql.DB) ([]string, error) {
	var (
		sqlStr string = fmt.Sprintf(CSqlFmtGetDbaIps, mystr.GetQuotedStringFromArr(dbTypes, "'", ","))
		myIps  []string
		err    error
		oneIp  string
	)
	rows, err := dbCon.Query(sqlStr)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		err = rows.Scan(&oneIp)
		if err != nil {
			return nil, err
		}
		oneIp = strings.TrimSpace(oneIp)
		if oneIp == "" {
			continue
		}
		myIps = append(myIps, oneIp)
	}
	return myIps, nil
}

func GetIPsInstalledFalconMon(mLogger *logging.MyLog, rClient *redis.ClusterClient, ipArr []string) ([]string, error) {
	var (
		existIps []string
		intCmds  []*redis.IntCmd
		exists   int64
		err      error
		msg      string
	)
	rp := rClient.Pipeline()
	defer rp.Close()
	for i := range ipArr {
		intCmds = append(intCmds, rp.Exists(fmt.Sprintf("%s:0:falcon", ipArr[i])))
	}
	rp.Exec()
	for i := range intCmds {
		exists, err = intCmds[i].Result()
		if err != nil {
			msg = fmt.Sprintf("error to check exists %s in redis",
				fmt.Sprintf("%s:0:falcon", ipArr[i]))
			mLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, msg, logging.INFO, ehand.ERR_REDIS_SET)
			return existIps, fmt.Errorf("%s: %s", msg, err)
		}
		if exists > 0 {
			existIps = append(existIps, ipArr[i])
		} else {
			mLogger.WriteToLogByFieldsNormalOnlyMsg("key not exists in redis: "+fmt.Sprintf("%s:0:falcon", ipArr[i]), logging.DEBUG)
		}
	}
	return existIps, nil
}

func GetNoDataIps(mLogger *logging.MyLog, client *redis.ClusterClient, downChan chan string, existIps []string,
	fieldTime string, nodataTime int64, stopNoDataTime int64, downTimes *DownIpSendTimes) error {
	var (
		sliceCmds []*redis.StringCmd
		err       error
		tmpInt    int64
		tmpStr    string
		timeInt   int64
		msg       string
		//ok             bool
		//ifNeedPing     bool
		fieldNotExists string = "redis: nil"
	)

	rp := client.Pipeline()
	defer rp.Close()
	for i := range existIps {
		sliceCmds = append(sliceCmds, rp.HGet(fmt.Sprintf("%s:0:falcon", existIps[i]), fieldTime))
	}
	rp.Exec()
	for i := range sliceCmds {

		tmpStr, err = sliceCmds[i].Result()
		if err != nil {
			downTimes.DeleteValue(existIps[i])
			if err.Error() == fieldNotExists {
				mLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("field %s not exists in %s", fieldTime, fmt.Sprintf("%s:0:falcon",
					existIps[i])), logging.DEBUG)
				continue
			} else {
				mLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, fmt.Sprintf("error to hget %s %s", fmt.Sprintf("%s:0:falcon",
					existIps[i]), fieldTime), logging.ERROR, ehand.ERR_REDIS_GET_KEY_VALUE)
				return fmt.Errorf("%s: %s", msg, err)
			}
		}

		tmpInt, err = strconv.ParseInt(tmpStr, 10, 64)
		if err != nil {
			downTimes.DeleteValue(existIps[i])
			msg = fmt.Sprintf("error to parse string %s into int64", tmpStr)
			mLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, msg, logging.ERROR, ehand.ERR_NUMBER_PARSE)
			return fmt.Errorf("%s: %s", msg, err)
		}
		timeInt = time.Now().Unix()

		if timeInt >= tmpInt+stopNoDataTime {

			if downTimes.DeleteValue(existIps[i]) {
				mLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("%s of %s have no data for %d seconds(>= %d), stop marking it as down candidate",
					fieldTime, fmt.Sprintf("%s:0:falcon", existIps[i]), timeInt-tmpInt, stopNoDataTime), logging.WARNING)
			} else {
				mLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("%s of %s have no data for %d seconds(>= %d)",
					fieldTime, fmt.Sprintf("%s:0:falcon", existIps[i]), timeInt-tmpInt, stopNoDataTime), logging.DEBUG)
			}
			continue
		} else if timeInt >= tmpInt+nodataTime {

			if !downTimes.SetValue(existIps[i], 1) {
				downChan <- existIps[i]
				mLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("%s of %s have no data for %d seconds(>= %d), mark it as down candidate",
					fieldTime, fmt.Sprintf("%s:0:falcon", existIps[i]), timeInt-tmpInt, nodataTime), logging.WARNING)
			} else {
				mLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("%s of %s have no data for %d seconds(>= %d), but it has been sent alarm",
					fieldTime, fmt.Sprintf("%s:0:falcon", existIps[i]), timeInt-tmpInt, nodataTime), logging.DEBUG)
			}
			continue
		} else {

			downTimes.DeleteValue(existIps[i])
		}

	}

	return nil

}

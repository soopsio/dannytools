package myfalcon

import (
	"dannytools/ehand"
	"dannytools/logging"
	"dannytools/myredis"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-redis/redis"
	"github.com/toolkits/slice"
)

const (
	C_AlarmInfo     string = "info"
	C_AlarmWarning  string = "warning"
	C_AlarmAverage  string = "average"
	C_AlarmHigh     string = "high"
	C_AlarmDisaster string = "disaster"

	C_AlarmTypeDefault string = "ExtraAlarm"
	C_AlarmTypeDown    string = "MachineDown"
	C_AlarmTypeLog     string = "LogMon"
	C_AlarmTypePlugin  string = "Plugin"
)

var (
	GalarmLevel []string = []string{
		C_AlarmInfo,
		C_AlarmWarning,
		C_AlarmAverage,
		C_AlarmHigh,
		C_AlarmDisaster,
	}
)

func CheckAlarmLevelString(lv string) string {
	if slice.ContainsString(GalarmLevel, lv) {
		return lv
	} else {
		return C_AlarmWarning
	}
}

type AlarmInfo struct {
	Endpoint  string // ip
	Port      string
	Timestamp int64
	Level     string // alarm level, ie: disaster
	AlarmType string // ie: MachineDown, LogMon
	Subject   string // alarm subject
	Body      string // alarm body
	KeyName   string // send to which key in redis
}

func (this AlarmInfo) String() string {
	return fmt.Sprintf("\tEndpoint=%s\n\tPort=%s\n\tTimestamp=%d\n\tLevel=%s\n\tAlarmType=%s\n\tKeyName=%s\n\tSubject=%s\n\tBody=%s\n\t",
		this.Endpoint, this.Port, this.Timestamp, this.Level, this.AlarmType, this.KeyName, this.Subject, this.Body)
}

func (this AlarmInfo) IntLevel() int {
	switch this.Level {
	case C_AlarmDisaster:
		return 5
	case C_AlarmHigh:
		return 4
	case C_AlarmAverage:
		return 3
	case C_AlarmWarning:
		return 2
	case C_AlarmInfo:
		return 1
	default:
		return 0
	}
}

func (this AlarmInfo) JsonString() (string, error) {
	str, err := json.Marshal(this)
	if err != nil {
		return "", err
	} else {
		return string(str), nil
	}
}

func (this AlarmInfo) SendAlarmToRedisOne(mLogger *logging.MyLog, client myredis.ClusterAndRedisClient) error {
	errMsg := ""

	msg, err := this.JsonString()
	if err != nil {
		errMsg = "error to marshal into json string:\n" + this.String()
		mLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, errMsg, logging.ERROR, ehand.ERR_JSON_MARSHAL)
		return fmt.Errorf("%s:\n\t%s", errMsg, err)
	}
	if client.IsCluster {
		_, err = client.Cluster.LPush(this.KeyName, msg).Result()
	} else {
		_, err = client.Redis.LPush(this.KeyName, msg).Result()
	}

	if err != nil {
		errMsg = fmt.Sprintf("error to send alarm msg to redis %s:\n%s", this.KeyName, msg)
		mLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, errMsg, logging.ERROR, ehand.ERR_REDIS_GET_KEY_VALUE)
		return fmt.Errorf("%s:\n\t%s", errMsg, err)
	} else {
		mLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("successfully send alarm msg to redis %s:\n%s",
			this.KeyName, msg), logging.DEBUG)
	}
	return nil
}

func (this AlarmInfo) SendAlarmToRedis(mLogger *logging.MyLog, client *redis.ClusterClient, alarmKey string) error {
	errMsg := ""
	if alarmKey == "" && this.KeyName != "" {
		alarmKey = this.KeyName
	}
	msg, err := this.JsonString()
	if err != nil {
		errMsg = "error to marshal into json string:\n" + this.String()
		mLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, errMsg, logging.ERROR, ehand.ERR_JSON_MARSHAL)
		return fmt.Errorf("%s:\n\t%s", errMsg, err)
	}
	_, err = client.LPush(alarmKey, msg).Result()
	if err != nil {
		errMsg = fmt.Sprintf("error to send alarm msg to redis %s:\n%s", alarmKey, msg)
		mLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, errMsg, logging.ERROR, ehand.ERR_REDIS_GET_KEY_VALUE)
		return fmt.Errorf("%s:\n\t%s", errMsg, err)
	} else {
		mLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("successfully send alarm msg to redis %s:\n%s",
			alarmKey, msg), logging.WARNING)
	}
	return nil
}

func (this AlarmInfo) SendAlarmToRedisGen(mLogger *logging.MyLog, client myredis.ClusterAndRedisClient, alarmKey string, ips []string) error {
	var (
		errMsg string
		err    error
		msg    string
		oneIp  string
		msgArr []string
	)
	if alarmKey == "" && this.KeyName != "" {
		alarmKey = this.KeyName
	}
	rp := client.GetPipeline()
	defer rp.Close()
	for _, oneIp = range ips {
		this.Endpoint = oneIp
		msg, err = this.JsonString()
		if err != nil {
			errMsg = "error to marshal into json string:\n" + this.String()
			mLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, errMsg, logging.ERROR, ehand.ERR_JSON_MARSHAL)
			return fmt.Errorf("%s:\n\t%s", errMsg, err)
		}

		rp.LPush(alarmKey, msg)

		msgArr = append(msgArr, msg)
	}
	_, err = rp.Exec()

	if err != nil {
		errMsg = fmt.Sprintf("error to send alarm msgs to redis %s:\n%s", alarmKey, strings.Join(msgArr, "\n"))
		mLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, errMsg, logging.ERROR, ehand.ERR_REDIS_GET_KEY_VALUE)
		return fmt.Errorf("%s:\n\t%s", errMsg, err)
	} else {
		mLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("successfully send alarm msgs to redis %s:\n%s",
			alarmKey, strings.Join(msgArr, "\n")), logging.DEBUG)
	}
	return nil
}

func SendMultiAlarmToRedis(ip string, alarms []AlarmInfo, mLogger *logging.MyLog, client myredis.ClusterAndRedisClient) error {
	var (
		msg    string
		errMsg string
		err    error
		msgArr []string
	)
	rp := client.GetPipeline()
	defer rp.Close()
	for _, oneA := range alarms {
		if oneA.Endpoint == "" {
			oneA.Endpoint = ip
		}
		msg, err = oneA.JsonString()
		if err != nil {
			errMsg = "error to marshal into json string:\n" + oneA.String()
			mLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, errMsg, logging.INFO, ehand.ERR_JSON_MARSHAL)
			err = fmt.Errorf("%s:\n\t%s", errMsg, err)
			continue
		} else {
			msgArr = append(msgArr, msg)
			rp.LPush(oneA.KeyName, msg)
		}
	}
	if len(msgArr) > 0 {
		_, err = rp.Exec()
		if err != nil {
			errMsg = fmt.Sprintf("error to send alarm msgs to redis:\n%s", strings.Join(msgArr, "\n"))
			mLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, errMsg, logging.INFO, ehand.ERR_REDIS_GET_KEY_VALUE)
			return fmt.Errorf("%s:\n\t%s", errMsg, err)
		} else {
			mLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("successfully send alarm msgs to redis:\n%s",
				strings.Join(msgArr, "\n")), logging.DEBUG)
			return nil
		}
	} else if err != nil {
		return err
	} else {
		return nil
	}
}

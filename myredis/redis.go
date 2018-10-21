package myredis

import (
	//"fmt"
	"regexp"
	"time"

	"dannytools/ehand"

	"github.com/go-redis/redis"
)

type ConfRedis struct {
	ConfCommon
	//Network  string // tcp|unix
	Addr     string //host:port | socket
	Database int    // db number
	ReadOnly bool   // if read slave
}

func (this ConfRedis) setConfRedis(opt *redis.Options) {
	this.setConfCommonRedis(opt)
	opt.Addr = this.Addr
	opt.DB = this.Database

}

func (this ConfRedis) CreateNewClientRedis() (*redis.Client, error) {
	var opt *redis.Options = &redis.Options{}
	this.setConfRedis(opt)
	client := redis.NewClient(opt)
	_, err := client.Ping().Result()
	if err != nil {
		if client != nil {
			client.Close()
		}
		return nil, ehand.WithStackError(err)
	} else {
		return client, nil
	}

}

func ScanRedisKeys(client *redis.Client, scanCnt int64, matchRe *regexp.Regexp, sleepCnt int, sleepTime int, keysChan chan string) error {
	//defer wg.Done()
	//defer client.Close()
	var (
		cursor            uint64 = 0
		err               error
		keys              []string
		cnt               int           = 0
		sleepTimeDuration time.Duration = time.Duration(sleepTime) * time.Microsecond
		ifReg             bool          = false
	)

	if matchRe.String() != "" {
		ifReg = true
	}

	for {
		keys, cursor, err = client.Scan(cursor, "", scanCnt).Result()
		if err != nil {
			return ehand.WithStackError(err)

		}
		cnt += len(keys)
		for _, k := range keys {
			if ifReg {
				//fmt.Printf("reg %s\n", matchRe.String())
				if matchRe.MatchString(k) {
					keysChan <- k
					//fmt.Printf("key match %s\n", k)

				} /*else {
					fmt.Printf("key unmatch %s\n", k)
				}
				*/
			} else {
				keysChan <- k
			}
			//fmt.Printf("key: %s\n", k)
		}
		if cursor == 0 {
			break
		}

		if sleepTime > 0 && cnt >= sleepCnt {
			cnt = 0
			time.Sleep(sleepTimeDuration)
		}
	}
	return nil
}

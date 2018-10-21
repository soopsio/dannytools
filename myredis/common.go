package myredis

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"dannytools/ehand"

	//"github.com/davecgh/go-spew/spew"
	"github.com/go-redis/redis"
	kitsFile "github.com/toolkits/file"
)

type ClusterAndRedisClient struct {
	Redis     *redis.Client
	Cluster   *redis.ClusterClient
	IsCluster bool
}

type ClusterAndRedisConf struct {
	Redis     *redis.Options
	Cluster   *redis.ClusterOptions
	IsCluster bool
}

type ConfCommon struct {
	Password   string
	MaxRetries int

	DialTimeout  int
	ReadTimeout  int
	WriteTimeout int

	PoolSize           int
	PoolTimeout        int
	IdleTimeout        int
	IdleCheckFrequency int
}

func (this ConfCommon) setConfCommonRedis(opt *redis.Options) {

	opt.Password = this.Password

	if this.MaxRetries == 0 {
		opt.MaxRetries = 2
	} else {
		opt.MaxRetries = this.MaxRetries
	}

	if this.DialTimeout == 0 {
		opt.DialTimeout = 3 * time.Second
	} else {
		opt.DialTimeout = time.Duration(this.DialTimeout) * time.Second
	}

	if this.ReadTimeout == 0 {
		opt.ReadTimeout = 5 * time.Second
	} else {
		opt.ReadTimeout = time.Duration(this.ReadTimeout) * time.Second
	}

	if this.WriteTimeout == 0 {
		opt.WriteTimeout = 8 * time.Second
	} else {
		opt.WriteTimeout = time.Duration(this.WriteTimeout) * time.Second
	}

	if this.IdleCheckFrequency == 0 {
		opt.IdleCheckFrequency = 60 * time.Second
	} else {
		opt.IdleCheckFrequency = time.Duration(this.IdleCheckFrequency) * time.Second
	}

	if this.IdleTimeout == 0 {
		opt.IdleTimeout = 180 * time.Second
	} else {
		opt.IdleTimeout = time.Duration(this.IdleTimeout) * time.Second
	}

	if this.PoolTimeout == 0 {
		opt.PoolTimeout = 5 * time.Second
	} else {
		opt.PoolTimeout = time.Duration(this.PoolTimeout) * time.Second
	}

	if this.PoolSize == 0 {
		opt.PoolSize = 128
	} else {
		opt.PoolSize = this.PoolSize
	}

}

func (this ConfCommon) setConfCommonCluster(opt *redis.ClusterOptions) {

	opt.Password = this.Password

	if this.MaxRetries == 0 {
		opt.MaxRetries = 2
	} else {
		opt.MaxRetries = this.MaxRetries
	}

	if this.DialTimeout == 0 {
		opt.DialTimeout = 3 * time.Second
	} else {
		opt.DialTimeout = time.Duration(this.DialTimeout) * time.Second
	}

	if this.ReadTimeout == 0 {
		opt.ReadTimeout = 5 * time.Second
	} else {
		opt.ReadTimeout = time.Duration(this.ReadTimeout) * time.Second
	}

	if this.WriteTimeout == 0 {
		opt.WriteTimeout = 8 * time.Second
	} else {
		opt.WriteTimeout = time.Duration(this.WriteTimeout) * time.Second
	}

	if this.IdleCheckFrequency == 0 {
		opt.IdleCheckFrequency = 60 * time.Second
	} else {
		opt.IdleCheckFrequency = time.Duration(this.IdleCheckFrequency) * time.Second
	}

	if this.IdleTimeout == 0 {
		opt.IdleTimeout = 180 * time.Second
	} else {
		opt.IdleTimeout = time.Duration(this.IdleTimeout) * time.Second
	}

	if this.PoolTimeout == 0 {
		opt.PoolTimeout = 5 * time.Second
	} else {
		opt.PoolTimeout = time.Duration(this.PoolTimeout) * time.Second
	}

	if this.PoolSize == 0 {
		opt.PoolSize = 128
	} else {
		opt.PoolSize = this.PoolSize
	}

}

type RedisAddr struct {
	Host string
	Port int
}

func (this RedisAddr) AddrString() string {
	return fmt.Sprintf("%s:%d", this.Host, this.Port)
}

func (this RedisAddr) CreateConPoolRedis(db int, poolSizeEachCpu int) (*redis.Client, error) {
	return ConfRedis{ConfCommon: ConfCommon{PoolSize: poolSizeEachCpu}, Addr: this.AddrString(), Database: db}.CreateNewClientRedis()
}

func (this RedisAddr) CreateConPoolCluster(readonly bool, poolSizeEachCpu int) (*redis.ClusterClient, error) {
	return ConfCluster{ConfCommon: ConfCommon{PoolSize: poolSizeEachCpu}, Addrs: []string{this.AddrString()}, ReadOnly: readonly}.CreateNewClientCluster()
}

func (this RedisAddr) CreateRedisOrClusterConPool(isCluster bool, readonly bool, db int, poolSizeEachCpu int) (ClusterAndRedisClient, error) {
	var (
		client ClusterAndRedisClient
		err    error
	)
	client.IsCluster = isCluster
	if isCluster {
		client.Cluster, err = ConfCluster{ConfCommon: ConfCommon{PoolSize: poolSizeEachCpu}, Addrs: []string{this.AddrString()}, ReadOnly: readonly}.CreateNewClientCluster()

	} else {
		client.Redis, err = ConfRedis{ConfCommon: ConfCommon{PoolSize: poolSizeEachCpu}, Addr: this.AddrString(), Database: db}.CreateNewClientRedis()
	}
	return client, err
}

func GetGenRedisConPool(addrs []string, isCluster bool, readonly bool, db int, poolSizeEachCpu int) (ClusterAndRedisClient, error) {
	var (
		client ClusterAndRedisClient
		err    error
	)
	client.IsCluster = isCluster
	if isCluster {
		client.Cluster, err = ConfCluster{ConfCommon: ConfCommon{PoolSize: poolSizeEachCpu}, Addrs: addrs, ReadOnly: readonly}.CreateNewClientCluster()

	} else {
		client.Redis, err = ConfRedis{ConfCommon: ConfCommon{PoolSize: poolSizeEachCpu}, Addr: addrs[0], Database: db}.CreateNewClientRedis()
	}
	return client, err
}

func GetClusterOrRedisClient(cfg ConfCommon, addrs []string, readOnly bool, db int, isCluster bool) (ClusterAndRedisClient, error) {
	var (
		client ClusterAndRedisClient
		err    error
	)
	client.IsCluster = isCluster
	if isCluster {
		cfgCluster := ConfCluster{ConfCommon: cfg, Addrs: addrs, ReadOnly: readOnly}
		client.Cluster, err = cfgCluster.CreateNewClientCluster()
	} else {
		cfgRedis := ConfRedis{ConfCommon: cfg, Addr: addrs[0], Database: db}
		client.Redis, err = cfgRedis.CreateNewClientRedis()
	}
	return client, err
}

func (client ClusterAndRedisClient) GetPipeline() redis.Pipeliner {
	if client.IsCluster {
		return client.Cluster.Pipeline()
	} else {
		return client.Redis.Pipeline()
	}
}

func (client ClusterAndRedisClient) Close() {
	if client.IsCluster {
		if client.Cluster != nil {
			client.Cluster.Close()
		}
	} else {
		if client.Redis != nil {
			client.Redis.Close()
		}
	}
}

type RedisValueString struct {
	Value    string
	BytesCnt int64
}

type RedisValueList struct {
	Value           []string
	ElementCnt      int64
	BytesCnt        int64
	BiggestElement  string
	BiggestBytesCnt int64
}

func (client ClusterAndRedisClient) GetElementCntPipe(keys []string, keyType string) (map[string]int64, error) {
	var (
		err    error
		result map[string]int64 = map[string]int64{}
		cmdArr []*redis.IntCmd
		length int64
	)

	rp := client.GetPipeline()
	defer rp.Close()

	for _, k := range keys {
		switch keyType {
		case "list":
			cmdArr = append(cmdArr, rp.LLen(k))
		case "hash":
			cmdArr = append(cmdArr, rp.HLen(k))
		case "set":
			cmdArr = append(cmdArr, rp.SCard(k))
		case "zset":
			cmdArr = append(cmdArr, rp.ZCard(k))
		}
	}
	_, err = rp.Exec()
	if err != nil {
		return nil, ehand.CreateErrorWithStack(err)
	}
	for i := range keys {
		length, err = cmdArr[i].Result()
		if err != nil {
			return nil, ehand.CreateErrorWithStack(err)
		}
		result[keys[i]] = length
	}
	return result, nil
}

func (client ClusterAndRedisClient) GetOneValueByRangeSameKeyPipe(key string, keyTp string, length int64, rangeBatch int64, elementBatch int64,
	elementInterval time.Duration, ifSleep, ifBytes, ifValue bool) (RedisValueList, error) {
	var (
		err error
		i   int64 = 0

		cmdArr []*redis.StringSliceCmd
		rp     redis.Pipeliner
		stop   int64
		result RedisValueList

		alreadyCnt int64
	)
	rp = client.GetPipeline()
	defer rp.Close()

	for {
		if i >= length {
			break
		}
		stop = i + rangeBatch - 1
		if stop >= length {
			stop = length
		}
		switch keyTp {
		case "list":
			cmdArr = append(cmdArr, rp.LRange(key, i, stop))
		case "zset":
			cmdArr = append(cmdArr, rp.ZRange(key, i, stop))

		default:
			return result, fmt.Errorf("unsupport key type %s to get value by range", keyTp)
		}

		alreadyCnt += stop - i + 1
		if alreadyCnt >= elementBatch {
			_, err = rp.Exec()
			if err != nil {
				return result, ehand.CreateErrorWithStack(err)
			}
			err = ProcessArrayOfStringSliceCmdResultSameKey(cmdArr, &result, ifBytes, ifValue)
			if err != nil {
				return result, err
			}
			alreadyCnt = 0
			cmdArr = nil
			if ifSleep {
				time.Sleep(elementInterval)
			}
		}
		i = stop + 1
	}
	if len(cmdArr) > 0 {
		_, err = rp.Exec()
		if err != nil {
			return result, ehand.CreateErrorWithStack(err)
		}
		err = ProcessArrayOfStringSliceCmdResultSameKey(cmdArr, &result, ifBytes, ifValue)
		if err != nil {
			return result, err
		}
	}
	result.BytesCnt += int64(len(key))
	return result, nil

}

func (client ClusterAndRedisClient) GetMultiKeyValuesByRangeAllPipe(keys []string, keyTp string, minBytes int64, ifBytes, ifValue bool) (map[string]RedisValueList, error) {
	var (
		err    error
		cmdArr []*redis.StringSliceCmd
		result map[string]RedisValueList = map[string]RedisValueList{}
	)
	rp := client.GetPipeline()
	defer rp.Close()

	for _, k := range keys {
		switch keyTp {
		case "list":
			cmdArr = append(cmdArr, rp.LRange(k, 0, -1))
		case "zset":
			cmdArr = append(cmdArr, rp.ZRange(k, 0, -1))
		default:
			return nil, fmt.Errorf("unsupport redis type %s to get all value by range", k)
		}
	}
	_, err = rp.Exec()
	if err != nil {
		return nil, ehand.CreateErrorWithStack(err)
	}
	for i := range keys {
		oneKeyResult := RedisValueList{}
		err = ProcessOneStringSliceCmdResult(cmdArr[i], &oneKeyResult, ifBytes, ifValue)
		if err != nil {
			return nil, err
		}
		oneKeyResult.BytesCnt += int64(len(keys[i]))
		if oneKeyResult.BytesCnt < minBytes {
			continue
		}
		result[keys[i]] = oneKeyResult
	}
	return result, nil

}

func ProcessOneStringSliceCmdResult(cmd *redis.StringSliceCmd, result *RedisValueList, ifBytes, ifValue bool) error {
	var (
		bCnt   int64 = 0
		strArr []string
		err    error
		k      string
	)

	strArr, err = cmd.Result()
	if err != nil {
		return ehand.CreateErrorWithStack(err)
	}
	result.ElementCnt += int64(len(strArr))
	if ifBytes {
		for _, k = range strArr {

			bCnt = int64(len(k))
			result.BytesCnt += bCnt
			if result.BiggestBytesCnt < bCnt {
				result.BiggestBytesCnt = bCnt
				result.BiggestElement = k
			}

		}
	}
	if ifValue {
		result.Value = append(result.Value, strArr...)
	}
	return nil
}

func ProcessArrayOfStringSliceCmdResultSameKey(cmdArr []*redis.StringSliceCmd, result *RedisValueList, ifBytes, ifValue bool) error {
	var (
		err error
		j   int
	)

	for j = range cmdArr {
		err = ProcessOneStringSliceCmdResult(cmdArr[j], result, ifBytes, ifValue)
		if err != nil {
			return err
		}
	}
	return nil
}

type RedisValueHash struct {
	Value           map[string]string
	ElementCnt      int64
	BytesCnt        int64
	BiggestElement  string
	BiggestBytesCnt int64
}

type RedisValueZset struct {
	Value           map[string]float64
	ElementCnt      int64
	BytesCnt        int64
	BiggestElement  string
	BiggestBytesCnt int64
}

// if sampleRate==0, not use memory usage to sample key

func (client ClusterAndRedisClient) GetKeysValueAdaptive(keys []string, keyTp string, db uint, minBytes int64, tFormat string, minTtl int, maxTtl int,
	sampleRate uint, minSample uint, rangeBatch int64, elementBatch int64,
	elementInterval time.Duration, ifSleep, ifBytes, ifValue bool) ([]RedisKey, error) {
	var (
		ifSample   bool = false
		err        error
		results    []RedisKey
		tmpResult  []RedisKey
		targetKeys []string
		ttls       map[string]string
		k          string

		keyLengths map[string]int64
	)

	ttls, err = client.GetKeysTTLConditionPipe(keys, tFormat, minTtl, maxTtl)
	if err != nil {
		return nil, err
	}
	if len(ttls) == 0 {
		return nil, nil
	}
	for k = range ttls {
		targetKeys = append(targetKeys, k)
	}
	if sampleRate > 0 && sampleRate <= 100 && !ifValue {
		ifSample = true
		if keyTp != "string" {
			keyLengths, err = client.GetElementCntPipe(targetKeys, keyTp)
			if err != nil {
				return nil, err
			}
		}
	}

	if ifSample {
		tmpResult, err = client.GetRedisKeyArrBySample(targetKeys, keyLengths, ttls, db, keyTp, sampleRate, minSample, minBytes)
		if err != nil {
			return nil, err
		}
		results = append(results, tmpResult...)
		return results, nil
	}

	switch keyTp {
	case "string":

		strVals, err := client.GetStringValueMget(targetKeys, minBytes, ifValue, ifBytes)
		if err != nil {
			return nil, err
		}
		for k, oneStrVal := range strVals {
			results = append(results, RedisKey{
				Database: db,
				Bytes:    oneStrVal.BytesCnt,
				Type:     keyTp,
				Key:      k,
				Expire:   ttls[k],
				Value:    oneStrVal.Value})
		}

	case "list":
		listVals, err := client.GetMultiListValuesPipeAdaptive(targetKeys, minBytes, rangeBatch, elementBatch, elementInterval, ifSleep, ifBytes, ifValue)
		if err != nil {
			return nil, err
		}
		for k, oneListVal := range listVals {
			results = append(results, RedisKey{
				Database:        db,
				Bytes:           oneListVal.BytesCnt,
				ElementCnt:      oneListVal.ElementCnt,
				Type:            keyTp,
				Expire:          ttls[k],
				MaxElementBytes: oneListVal.BiggestBytesCnt,
				Key:             k,
				MaxElement:      oneListVal.BiggestElement,
				Value:           oneListVal.Value})
		}
	case "hash":

		hashVals, err := client.GetMultiHashValueAdaptive(targetKeys, minBytes, rangeBatch, elementBatch, elementInterval, ifSleep, ifBytes, ifValue)
		if err != nil {
			return nil, err
		}
		for k, oneHashVal := range hashVals {
			results = append(results, RedisKey{
				Database:        db,
				Bytes:           oneHashVal.BytesCnt,
				ElementCnt:      oneHashVal.ElementCnt,
				Type:            keyTp,
				Expire:          ttls[k],
				MaxElementBytes: oneHashVal.BiggestBytesCnt,
				Key:             k,
				MaxElement:      oneHashVal.BiggestElement,
				Value:           oneHashVal.Value})
		}
	case "set":

		listVals, err := client.GetMultiSetValueAdaptive(targetKeys, minBytes, rangeBatch, elementBatch, elementInterval, ifSleep, ifBytes, ifValue)
		if err != nil {
			return nil, err
		}
		for k, oneListVal := range listVals {
			results = append(results, RedisKey{
				Database:        db,
				Bytes:           oneListVal.BytesCnt,
				ElementCnt:      oneListVal.ElementCnt,
				Type:            keyTp,
				Expire:          ttls[k],
				MaxElementBytes: oneListVal.BiggestBytesCnt,
				Key:             k,
				MaxElement:      oneListVal.BiggestElement,
				Value:           oneListVal.Value})
		}
	case "zset":
		if ifValue {
			vals, err := client.GetMultiZsetValuesAdaptiveWithScore(targetKeys, minBytes, rangeBatch, elementBatch, elementInterval, ifSleep, ifBytes, ifValue)
			if err != nil {
				return nil, err
			}
			for k, oneVal := range vals {
				results = append(results, RedisKey{
					Database:        db,
					Bytes:           oneVal.BytesCnt,
					ElementCnt:      oneVal.ElementCnt,
					Type:            keyTp,
					Expire:          ttls[k],
					MaxElementBytes: oneVal.BiggestBytesCnt,
					Key:             k,
					MaxElement:      oneVal.BiggestElement,
					Value:           oneVal.Value})
			}
		} else {
			vals, err := client.GetMultiZsetValuesAdaptiveWithoutScore(targetKeys, minBytes, rangeBatch, elementBatch, elementInterval, ifSleep, ifBytes, ifValue)
			if err != nil {
				return nil, err
			}
			for k, oneVal := range vals {
				results = append(results, RedisKey{
					Database:        db,
					Bytes:           oneVal.BytesCnt,
					ElementCnt:      oneVal.ElementCnt,
					Type:            keyTp,
					Expire:          ttls[k],
					MaxElementBytes: oneVal.BiggestBytesCnt,
					Key:             k,
					MaxElement:      oneVal.BiggestElement,
					Value:           oneVal.Value})
			}
		}
	default:
		return nil, ehand.CreateStrErrorWithStack(fmt.Sprintf("unsupported redis data type: %s", keyTp))

	}

	return results, nil
}

func ParseAddrFromFile(adrFile string) ([]RedisAddr, error) {
	var (
		str    string
		err    error
		line   string
		arr    []string
		tmpArr []string
		addrs  []RedisAddr
		dup    map[string]bool = map[string]bool{}
		//port   int
		tmpInt uint64
		ok     bool
	)
	str, err = kitsFile.ToTrimString(adrFile)
	if err != nil {
		return nil, err
	}
	arr = strings.Split(str, "\n")
	for _, line = range arr {
		tmpArr = strings.Split(line, ":")
		if len(tmpArr) != 2 {
			continue
		}
		tmpInt, err = strconv.ParseUint(tmpArr[1], 10, 16)
		if err != nil {
			return nil, fmt.Errorf("fail to parse redis addr %s: %s", line, err)
		}
		adr := RedisAddr{Host: tmpArr[0], Port: int(tmpInt)}
		if _, ok = dup[adr.AddrString()]; ok {
			continue
		}
		dup[adr.AddrString()] = true
		addrs = append(addrs, adr)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no valid redis addr")
	}
	return addrs, nil

}

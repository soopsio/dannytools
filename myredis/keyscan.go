package myredis

import (
	"dannytools/ehand"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	//"github.com/davecgh/go-spew/spew"
	"github.com/go-redis/redis"
)

const (
	G_Redis_BigKey_Format_Data   string = "%-2d, %-10d, %-10d, %-6s, %-19s, %-10d, %-20s, %-20s\n"
	G_Redis_BigKey_Format_Header string = "%-2s, %-10s, %-10s, %-6s, %-19s, %-10s, %-20s, %-20s\n"
)

var (
	G_Redis_BigKey_Headers []string = []string{"db", "sizeInByte", "elementCnt", "type", "expire", "bigSize", "key", "big"}
)

func GetRedisBigkeyHeader() string {
	return fmt.Sprintf(G_Redis_BigKey_Format_Header, "db", "sizeInByte", "elementCnt", "type", "expire", "bigSize", "key", "big")
}

type RedisKey struct {
	Database        uint
	Bytes           int64 // bytes of key and value
	ElementCnt      int64 // for list|set|hash|zset, the count of the elements
	Type            string
	Expire          string
	MaxElementBytes int64  // bytes of max element of key
	Key             string // key
	MaxElement      string // max element of key
	Value           interface{}
}

func (this RedisKey) GetRedisKeyPrintLine(ifBigkey bool, ifPretty bool, indent string) (string, error) {
	if ifBigkey {
		return this.ToStringWithoutValue(), nil
	} else {
		return this.DumpJsonStr(ifPretty, indent)
	}
}

func (this RedisKey) ToStringWithoutValue() string {
	var (
		key        string = this.Key
		maxElement string = this.MaxElement
	)
	if strings.Contains(this.Key, ",") {
		key = fmt.Sprintf("'%s'", this.Key)
	}
	if strings.Contains(this.MaxElement, ",") {
		maxElement = fmt.Sprintf("'%s'", this.MaxElement)
	}

	return fmt.Sprintf(G_Redis_BigKey_Format_Data, this.Database, this.Bytes, this.ElementCnt, this.Type, this.Expire, this.MaxElementBytes, key, maxElement)
}

func (this RedisKey) DumpJsonStr(ifPretty bool, indent string) (string, error) {
	var (
		err error
		//result  string
		reBytes []byte
	)

	if ifPretty {
		reBytes, err = json.MarshalIndent(this, "", indent)
	} else {
		reBytes, err = json.Marshal(this)
	}
	if err != nil {
		return "", ehand.WithStackError(err)
	} else {
		return string(reBytes), nil
	}

}

// keys: keyname=>elementCnt
func GetMemUseSampleNotString(keys map[string]int64, sampleRate int64, minNumb int64) map[string]int {
	var (
		result map[string]int = map[string]int{}
		sample int64
	)
	for k, v := range keys {
		if v == -1 {
			result[k] = -1 // for string
			continue
		}
		if sampleRate == 100 || sampleRate == 0 { // sample all elements
			sample = 0
		} else {
			sample = (v / 100) * sampleRate
			if sample < minNumb {
				sample = minNumb
			}
		}
		result[k] = int(sample)
	}
	return result
}

func GetStringKeySample(keys []string) map[string]int {
	var (
		result map[string]int = map[string]int{}
	)
	for _, k := range keys {
		result[k] = -1
	}
	return result
}

// param: key:sample
//return : key:memBytes

func (client ClusterAndRedisClient) GetMemByMemUsagePipe(keys map[string]int, minBytes int64) (map[string]int64, error) {
	var (
		err      error
		rp       redis.Pipeliner
		cmdArr   []*redis.IntCmd
		result   map[string]int64 = map[string]int64{}
		arr      []string
		k        string
		i        int
		bytesCnt int64
	)
	if client.IsCluster {
		rp = client.Cluster.Pipeline()
	} else {
		rp = client.Redis.Pipeline()
	}
	defer rp.Close()
	for k = range keys {
		arr = append(arr, k)
		if keys[k] == -1 {
			cmdArr = append(cmdArr, rp.MemoryUsage(k))
		} else {
			cmdArr = append(cmdArr, rp.MemoryUsage(k, keys[k]))
		}

	}
	_, err = rp.Exec()
	if err != nil {
		return nil, ehand.CreateErrorWithStack(err)
	}
	for i = range arr {
		bytesCnt, err = cmdArr[i].Result()
		if err != nil {
			return nil, ehand.CreateErrorWithStack(err)
		}
		if bytesCnt >= minBytes {
			result[arr[i]] = bytesCnt
		}
	}
	return result, nil

}

func (client ClusterAndRedisClient) GetRedisKeyArrBySample(keys []string, keyLengths map[string]int64, ttls map[string]string, db uint, keyTp string,
	sampleRate uint, minSample uint, minBytes int64) ([]RedisKey, error) {
	var (
		results []RedisKey
		samples map[string]int
		err     error
	)
	if keyTp == "string" {
		samples = GetStringKeySample(keys)
	} else {
		samples = GetMemUseSampleNotString(keyLengths, int64(sampleRate), int64(minSample))
	}

	sampleMem, err := client.GetMemByMemUsagePipe(samples, minBytes)
	if err != nil {
		return nil, err
	}

	for k, oneBytes := range sampleMem {
		if keyTp == "string" {
			results = append(results, RedisKey{
				Database: db,
				Bytes:    oneBytes,
				Type:     keyTp,
				Expire:   ttls[k],
				Key:      k})
		} else {
			results = append(results, RedisKey{
				Database:   db,
				Bytes:      oneBytes,
				ElementCnt: keyLengths[k],
				Type:       keyTp,
				Expire:     ttls[k],
				Key:        k})
		}
	}
	return results, nil
}

func (client ClusterAndRedisClient) GetKeysTypePipe(keys []string) (map[string]string, error) {
	var (
		rp     redis.Pipeliner
		cmdArr []*redis.StatusCmd
		err    error
		result map[string]string = map[string]string{}
		tp     string
	)
	if client.IsCluster {
		rp = client.Cluster.Pipeline()
	} else {
		rp = client.Redis.Pipeline()
	}
	defer rp.Close()

	for _, k := range keys {
		cmdArr = append(cmdArr, rp.Type(k))
	}
	_, err = rp.Exec()
	if err != nil {
		return nil, ehand.CreateErrorWithStack(err)
	}
	for i := range keys {
		tp, err = cmdArr[i].Result()
		if err != nil {
			return nil, ehand.CreateErrorWithStack(err)
		}
		result[keys[i]] = tp
	}
	return result, nil

}

func (client ClusterAndRedisClient) DeleteKeysByExpireItPipe(keyes []string, expTime time.Duration) error {
	var (
		err error
		rp  redis.Pipeliner
		//cmdArr []*redis.BoolCmd
	)
	if client.IsCluster {
		rp = client.Cluster.Pipeline()
	} else {
		rp = client.Redis.Pipeline()
	}
	defer rp.Close()

	for _, key := range keyes {
		//cmdArr = append(cmdArr, rp.Expire(key, expTime))
		rp.Expire(key, expTime)
	}
	_, err = rp.Exec()

	if err != nil {
		return ehand.CreateErrorWithStack(err)
	} else {
		return nil
	}
}

// if minTtl == -1 || maxTtl == -1: only keys not set ttl
// if minTtl == 0 && maxTtl == 0: any key
// if minTtl > 0 : only key with ttl >= minTtl
// if maxTtl > 0 : only key with ttl <= maxTtl
// expired key is not return

func (client ClusterAndRedisClient) GetKeysTTLConditionPipe(keys []string, timeFmt string, minTtl, maxTtl int) (map[string]string, error) {
	var (
		ttl           time.Duration
		err           error
		cmdArr        []*redis.DurationCmd
		k             string
		i             int
		secs          float64
		result        map[string]string = map[string]string{}
		maxTtlFloat64 float64           = float64(maxTtl)
		minTtlFloat64 float64           = float64(minTtl)
		ifOk          bool
	)
	rp := client.GetPipeline()
	defer rp.Close()

	for _, k = range keys {
		cmdArr = append(cmdArr, rp.TTL(k))
	}
	_, err = rp.Exec()
	if err != nil {
		return nil, ehand.CreateErrorWithStack(err)
	}
	for i = range keys {
		ttl, err = cmdArr[i].Result()
		if err != nil {
			return nil, ehand.CreateErrorWithStack(err)
		}
		secs = ttl.Seconds()
		//fmt.Printf("%s %f maxt=%f mint=%f\n", keys[i], secs, maxTtlFloat64, minTtlFloat64)
		if secs == -2 {
			continue
		}
		if secs == -1 {
			ifOk = false
			if minTtl == -1 && maxTtl == -1 {
				ifOk = true
			}
			if minTtl == 0 && maxTtl == 0 {
				ifOk = true
			}
			if minTtl > 0 {
				ifOk = true
			}
			if maxTtl > 0 {
				ifOk = false
			}
			if ifOk {
				//fmt.Printf("%s ok\n", keys[i])
				result[keys[i]] = "" // not set
			}
		} else {
			ifOk = true
			if minTtl == -1 && maxTtl == -1 {
				ifOk = false
			}
			if minTtl > 0 {
				if secs < minTtlFloat64 {
					ifOk = false
				}
			}

			if maxTtl > 0 {
				if secs > maxTtlFloat64 {
					ifOk = false
				}
			}
			if ifOk {
				//fmt.Printf("%s ok\n", keys[i])
				result[keys[i]] = time.Now().Add(ttl).Format(timeFmt)
			}
		}

	}
	return result, nil

}

func (client ClusterAndRedisClient) GetKeysTTLPipe(keys []string, timeFmt string) (map[string]string, error) {
	var (
		ttl    time.Duration
		err    error
		rp     redis.Pipeliner
		cmdArr []*redis.DurationCmd
		k      string
		i      int
		secs   float64
		result map[string]string = map[string]string{}
	)
	if client.IsCluster {
		rp = client.Cluster.Pipeline()
	} else {
		rp = client.Redis.Pipeline()
	}
	defer rp.Close()

	for _, k = range keys {
		cmdArr = append(cmdArr, rp.TTL(k))
	}
	_, err = rp.Exec()
	if err != nil {
		return nil, ehand.CreateErrorWithStack(err)
	}
	for i = range keys {
		ttl, err = cmdArr[i].Result()
		if err != nil {
			return nil, ehand.CreateErrorWithStack(err)
		}
		secs = ttl.Seconds()
		if secs == -1 {
			result[keys[i]] = "" // not set
		} else if secs == -2 {
			continue
			//result[keys[i]] = "-2" // already expired
		} else {
			result[keys[i]] = time.Now().Add(ttl).Format(timeFmt)
		}
	}
	return result, nil

}

func (client ClusterAndRedisClient) GetStringValuePipe(keys []string, minBytes int64, ifValue, ifBytes bool) (map[string]RedisValueString, error) {
	var (
		rp     redis.Pipeliner
		err    error
		cmdArr []*redis.StringCmd
		k      string
		i      int
		result map[string]RedisValueString = map[string]RedisValueString{}
		str    string
	)

	if client.IsCluster {
		rp = client.Cluster.Pipeline()
	} else {
		rp = client.Redis.Pipeline()
	}
	defer rp.Close()

	for _, k = range keys {
		cmdArr = append(cmdArr, rp.Get(k))
	}
	_, err = rp.Exec()
	if err != nil {
		return nil, ehand.CreateErrorWithStack(err)
	}

	for i = range keys {
		str, err = cmdArr[i].Result()
		if err != nil {
			return nil, ehand.CreateErrorWithStack(err)
		}
		val := RedisValueString{}
		if ifBytes {
			val.BytesCnt = int64(len(keys[i]) + len(str))
			if val.BytesCnt < minBytes {
				continue
			}
		} else {
			val.BytesCnt = 0
		}
		if ifValue {
			val.Value = str
		} else {
			val.Value = ""
		}
		result[keys[i]] = val
	}
	return result, nil

}

func (client ClusterAndRedisClient) GetListLenPipe(keys []string) (map[string]int64, error) {
	var (
		err    error
		rp     redis.Pipeliner
		cmdArr []*redis.IntCmd
		i      int
		result map[string]int64 = map[string]int64{}
		length int64
	)

	if client.IsCluster {
		rp = client.Cluster.Pipeline()
	} else {
		rp = client.Redis.Pipeline()
	}
	defer rp.Close()

	for i = range keys {
		cmdArr = append(cmdArr, rp.LLen(keys[i]))
	}
	_, err = rp.Exec()
	if err != nil {
		return nil, ehand.CreateErrorWithStack(err)
	}
	for i = range keys {
		length, err = cmdArr[i].Result()
		if err != nil {
			return nil, ehand.CreateErrorWithStack(err)
		}
		result[keys[i]] = length
	}
	return result, nil

}

func (client ClusterAndRedisClient) GetListValueSameKeyPipe(key string, length int64, rangeBatch int64, elementBatch int64,
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
		cmdArr = append(cmdArr, rp.LRange(key, i, stop))
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
	if ifBytes {
		result.BytesCnt += int64(len(key))
	}
	return result, nil

}

func (client ClusterAndRedisClient) GetMultiListValuesPipeAdaptive(keys []string, minBytes int64, rangeBatch int64, elementBatch int64,
	elementInterval time.Duration, ifSleep, ifBytes, ifValue bool) (map[string]RedisValueList, error) {

	var (
		err        error
		keyLengths map[string]int64
		k          string
		oneKeyLen  int64

		result     map[string]RedisValueList = map[string]RedisValueList{}
		alreadyCnt int64                     = 0
		oneKeysArr []string
	)

	//keyLengths, err = client.GetListLenPipe(keys)
	keyLengths, err = client.GetElementCntPipe(keys, "list")
	if err != nil {
		return nil, err
	}

	for k = range keyLengths {
		oneKeyLen = keyLengths[k]
		if oneKeyLen > rangeBatch {
			oneListValue, err := client.GetListValueSameKeyPipe(k, oneKeyLen, rangeBatch, elementBatch,
				elementInterval, ifSleep, ifBytes, ifValue)
			if err != nil {
				return nil, err
			}
			if oneListValue.BytesCnt < minBytes {
				continue
			}
			result[k] = oneListValue
			continue
		} else {
			oneKeysArr = append(oneKeysArr, k)
			alreadyCnt += oneKeyLen
			if alreadyCnt >= elementBatch {
				rMap, err := client.GetMultiKeyValuesByRangeAllPipe(oneKeysArr, "list", minBytes, ifBytes, ifValue)
				if err != nil {
					return nil, err
				}
				for ky := range rMap {
					result[ky] = rMap[ky]
				}
				oneKeysArr = nil
				alreadyCnt = 0
			}

		}
	}
	if len(oneKeysArr) > 0 {
		rMap, err := client.GetMultiKeyValuesByRangeAllPipe(oneKeysArr, "list", minBytes, ifBytes, ifValue)
		if err != nil {
			return nil, err
		}
		for ky := range rMap {
			result[ky] = rMap[ky]
		}
	}
	return result, nil
}

func (client ClusterAndRedisClient) GetMultiHashValueAdaptive(keys []string, minBytes int64, rangeBatch int64, elementBatch int64,
	elementInterval time.Duration, ifSleep, ifBytes, ifValue bool) (map[string]RedisValueHash, error) {

	var (
		result     map[string]RedisValueHash = map[string]RedisValueHash{}
		keyArr     []string
		alreadyCnt int64 = 0
	)
	//hLengths, err := client.GetMultiHashLenPipe(keys)
	hLengths, err := client.GetElementCntPipe(keys, "hash")
	if err != nil {
		return nil, err
	}
	for k, kL := range hLengths {
		if kL > rangeBatch {
			oneKeyVal, err := client.GetOneHashValueByScan(k, rangeBatch, elementBatch, elementInterval, ifSleep, ifBytes, ifValue)
			if err != nil {
				return nil, err
			}
			if oneKeyVal.BytesCnt < minBytes {
				continue
			}
			result[k] = oneKeyVal
			continue
		} else {
			keyArr = append(keyArr, k)
			alreadyCnt += kL
		}
		if alreadyCnt >= elementBatch {
			rMap, err := client.GetMultiHashValueByHgetallPipe(keyArr, minBytes, ifBytes, ifValue)
			if err != nil {
				return nil, err
			}
			for ky := range rMap {
				result[ky] = rMap[ky]
			}
			alreadyCnt = 0
			keyArr = nil
			if ifSleep {
				time.Sleep(elementInterval)
			}
		}

	}

	if len(keyArr) > 0 {
		rMap, err := client.GetMultiHashValueByHgetallPipe(keyArr, minBytes, ifBytes, ifValue)
		if err != nil {
			return nil, err
		}
		for ky := range rMap {
			result[ky] = rMap[ky]
		}
	}
	return result, nil
}

func (client ClusterAndRedisClient) GetMultiHashLenPipe(keys []string) (map[string]int64, error) {
	var (
		err    error
		result map[string]int64 = map[string]int64{}
		cmdArr []*redis.IntCmd
		length int64
	)
	rp := client.GetPipeline()
	defer rp.Close()

	for _, k := range keys {
		cmdArr = append(cmdArr, rp.HLen(k))
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

func (client ClusterAndRedisClient) GetMultiHashValueByHgetallPipe(keys []string, minBytes int64, ifBytes, ifValue bool) (map[string]RedisValueHash, error) {
	var (
		err        error
		result     map[string]RedisValueHash = map[string]RedisValueHash{}
		cmdArr     []*redis.StringStringMapCmd
		oneByteCnt int64
	)
	rp := client.GetPipeline()
	defer rp.Close()

	for _, k := range keys {
		cmdArr = append(cmdArr, rp.HGetAll(k))
	}
	_, err = rp.Exec()
	if err != nil {
		return nil, ehand.CreateErrorWithStack(err)
	}
	for i := range keys {
		oneK, err := cmdArr[i].Result()
		if err != nil {
			return nil, ehand.CreateErrorWithStack(err)
		}
		oneVal := RedisValueHash{ElementCnt: int64(len(oneK))}
		if ifBytes {
			for k, v := range oneK {
				oneByteCnt = int64(len(k) + len(v))
				oneVal.BytesCnt += oneByteCnt
				if oneByteCnt > oneVal.BiggestBytesCnt {
					oneVal.BiggestBytesCnt = oneByteCnt
					oneVal.BiggestElement = k
				}
			}
			if oneVal.BytesCnt < minBytes {
				continue
			}
		}
		if ifValue {
			oneVal.Value = oneK
		}
		result[keys[i]] = oneVal

	}
	return result, nil
}

func (client ClusterAndRedisClient) GetOneHashValueByScan(key string, rangeBatch int64, elementBatch int64,
	elementInterval time.Duration, ifSleep, ifBytes, ifValue bool) (RedisValueHash, error) {
	var (
		err        error
		eKeys      []string
		result     RedisValueHash
		cursor     uint64 = 0
		alreadyCnt int64  = 0
		oneBytes   int64  = 0
		i          int
	)

	for {
		if client.IsCluster {
			eKeys, cursor, err = client.Cluster.HScan(key, cursor, "", rangeBatch).Result()

		} else {
			eKeys, cursor, err = client.Redis.HScan(key, cursor, "", rangeBatch).Result()

		}
		if err != nil {
			return result, ehand.CreateErrorWithStack(err)
		}

		for i = 0; i < len(eKeys); i += 2 {
			alreadyCnt++
			result.ElementCnt++
			if ifValue {
				result.Value[eKeys[i]] = eKeys[i+1]
			}
			if ifBytes {
				oneBytes = int64(len(eKeys[i]) + len(eKeys[i+1]))
				result.BytesCnt += oneBytes
				if oneBytes > result.BiggestBytesCnt {
					result.BiggestBytesCnt = oneBytes
					result.BiggestElement = eKeys[i]
				}
			}
		}

		if cursor == 0 {
			break
		}

		if ifSleep {
			if alreadyCnt > elementBatch {
				time.Sleep(elementInterval)
				alreadyCnt = 0
			}

		}

	}
	return result, nil
}

func (client ClusterAndRedisClient) GetMultiSetValueAdaptive(keys []string, minBytes int64, rangeBatch int64, elementBatch int64,
	elementInterval time.Duration, ifSleep, ifBytes, ifValue bool) (map[string]RedisValueList, error) {
	var (
		err        error
		result     map[string]RedisValueList = map[string]RedisValueList{}
		keyArr     []string
		alreadyCnt int64 = 0
	)

	//kLengths, err := client.GetMultiSetCard(keys)
	kLengths, err := client.GetElementCntPipe(keys, "set")
	if err != nil {
		return nil, err
	}

	for k, kL := range kLengths {
		if kL > rangeBatch {
			oneVal, err := client.GetOneSetValueByScan(k, rangeBatch, elementBatch, elementInterval, ifSleep, ifBytes, ifValue)
			if err != nil {
				return nil, err
			}
			if oneVal.BytesCnt < minBytes {
				continue
			}
			continue
		} else {
			keyArr = append(keyArr, k)
			alreadyCnt += kL
			if alreadyCnt >= elementBatch {
				rMap, err := client.GetMultiSetValueBySmembersPipe(keyArr, minBytes, ifBytes, ifValue)
				if err != nil {
					return nil, err
				}
				for ky := range rMap {
					result[ky] = rMap[ky]
				}
				keyArr = nil
				alreadyCnt = 0
			}
		}

	}
	if len(keyArr) > 0 {
		rMap, err := client.GetMultiSetValueBySmembersPipe(keyArr, minBytes, ifBytes, ifValue)
		if err != nil {
			return nil, err
		}
		for ky := range rMap {
			result[ky] = rMap[ky]
		}
	}
	return result, nil

}

func (client ClusterAndRedisClient) GetMultiSetCard(keys []string) (map[string]int64, error) {
	var (
		err    error
		cmdArr []*redis.IntCmd
		result map[string]int64 = map[string]int64{}
		length int64
	)
	rp := client.GetPipeline()
	defer rp.Close()
	for _, k := range keys {
		cmdArr = append(cmdArr, rp.SCard(k))
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

func (client ClusterAndRedisClient) GetMultiSetValueBySmembersPipe(keys []string, minBytes int64, ifBytes, ifValue bool) (map[string]RedisValueList, error) {
	var (
		result map[string]RedisValueList = map[string]RedisValueList{}
		err    error
		cmdArr []*redis.StringSliceCmd
	)

	rp := client.GetPipeline()
	defer rp.Close()

	for _, k := range keys {
		cmdArr = append(cmdArr, rp.SMembers(k))
	}
	_, err = rp.Exec()
	if err != nil {
		return nil, ehand.CreateErrorWithStack(err)
	}
	for i := range keys {
		oneResult := RedisValueList{}
		err = ProcessOneStringSliceCmdResult(cmdArr[i], &oneResult, ifBytes, ifValue)
		if err != nil {
			return nil, err
		}
		if oneResult.BytesCnt < minBytes {
			continue
		}
		result[keys[i]] = oneResult
	}
	return result, nil

}

func (client ClusterAndRedisClient) GetOneSetValueByScan(key string, rangeBatch int64, elementBatch int64,
	elementInterval time.Duration, ifSleep, ifBytes, ifValue bool) (RedisValueList, error) {
	var (
		err        error
		cursor     uint64 = 0
		oneVals    []string
		alreadyCnt int64 = 0
		result     RedisValueList
		oneByte    int64
	)

	for {
		if client.IsCluster {
			oneVals, cursor, err = client.Cluster.SScan(key, cursor, "", rangeBatch).Result()
		} else {
			oneVals, cursor, err = client.Redis.SScan(key, cursor, "", rangeBatch).Result()
		}
		if err != nil {
			return result, ehand.CreateErrorWithStack(err)
		}
		result.ElementCnt += int64(len(oneVals))
		if ifValue {
			result.Value = append(result.Value, oneVals...)
		}

		if ifBytes {
			for _, v := range oneVals {
				oneByte = int64(len(v))
				result.BytesCnt += oneByte
				if oneByte > result.BiggestBytesCnt {
					result.BiggestBytesCnt = oneByte
					result.BiggestElement = v
				}

			}
		}
		if cursor == 0 {
			break
		}

		if ifSleep {
			alreadyCnt += int64(len(oneVals))
			if alreadyCnt > elementBatch {
				time.Sleep(elementInterval)
			}
			alreadyCnt = 0
		}

	}
	return result, nil

}

func ProcessOneZsliceResult(cmd *redis.ZSliceCmd, result *RedisValueZset, ifBytes, ifValue bool) error {
	var (
		err      error
		arr      []redis.Z
		z        redis.Z
		ke       string
		ok       bool
		oneBytes int64
	)
	arr, err = cmd.Result()
	if err != nil {
		return err
	}
	for _, z = range arr {
		ke, ok = z.Member.(string)
		if !ok {
			return ehand.CreateStrErrorWithStack(fmt.Sprintf("error to convert zset member %v to string", z.Member))
		}
		result.ElementCnt++
		if ifValue {
			result.Value[ke] = z.Score
		}
		if ifBytes {
			oneBytes = int64(len(ke)) + 8
			result.BytesCnt += oneBytes
			if oneBytes > result.BiggestBytesCnt {
				result.BiggestBytesCnt = oneBytes
				result.BiggestElement = ke
			}
		}
	}
	return nil
}

func ProcessZsliceResultArrayButSameKey(cmdArr []*redis.ZSliceCmd, result *RedisValueZset, ifBytes, ifValue bool) error {
	var (
		err error
	)
	for _, cmd := range cmdArr {
		err = ProcessOneZsliceResult(cmd, result, ifBytes, ifValue)
		if err != nil {
			return err
		}
	}
	return nil
}

func (client ClusterAndRedisClient) GetOneZsetWithScoreSameKeyPipe(key string, length int64, rangeBatch int64, elementBatch int64,
	elementInterval time.Duration, ifSleep, ifBytes bool) (RedisValueZset, error) {
	var (
		err        error
		i          int64 = 0
		cmdArr     []*redis.ZSliceCmd
		rp         redis.Pipeliner
		stop       int64
		result     RedisValueZset = RedisValueZset{Value: map[string]float64{}}
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

		cmdArr = append(cmdArr, rp.ZRangeWithScores(key, i, stop))
		alreadyCnt += stop - i + 1
		if alreadyCnt >= elementBatch {
			_, err = rp.Exec()
			if err != nil {
				return result, ehand.CreateErrorWithStack(err)
			}
			err = ProcessZsliceResultArrayButSameKey(cmdArr, &result, ifBytes, true)
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
		err = ProcessZsliceResultArrayButSameKey(cmdArr, &result, ifBytes, true)
		if err != nil {
			return result, err
		}
	}
	result.BytesCnt += int64(len(key))
	return result, nil

}

func (client ClusterAndRedisClient) GetMultiZsetValueByRangeAllPipeWithScore(keys []string, minBytes int64, ifBytes, ifValue bool) (map[string]RedisValueZset, error) {
	var (
		err    error
		result map[string]RedisValueZset = map[string]RedisValueZset{}
		cmdArr []*redis.ZSliceCmd
	)
	rp := client.GetPipeline()
	defer rp.Close()

	for _, k := range keys {
		cmdArr = append(cmdArr, rp.ZRangeWithScores(k, 0, -1))
	}

	_, err = rp.Exec()
	if err != nil {
		return nil, ehand.CreateErrorWithStack(err)
	}

	for i := range keys {
		oneVal := RedisValueZset{Value: map[string]float64{}}
		err = ProcessOneZsliceResult(cmdArr[i], &oneVal, ifBytes, ifValue)
		if err != nil {
			return nil, err
		}
		if oneVal.BytesCnt < minBytes {
			continue
		}
		result[keys[i]] = oneVal
	}
	return result, nil

}

func (client ClusterAndRedisClient) GetMultiZsetValuesAdaptiveWithScore(keys []string, minBytes int64, rangeBatch int64, elementBatch int64,
	elementInterval time.Duration, ifSleep, ifBytes, ifValue bool) (map[string]RedisValueZset, error) {
	var (
		err        error
		result     map[string]RedisValueZset = map[string]RedisValueZset{}
		keyArr     []string
		alreadyCnt int64 = 0
	)
	lengths, err := client.GetElementCntPipe(keys, "zset")
	if err != nil {
		return nil, err
	}

	for k, kL := range lengths {
		if kL >= rangeBatch {
			oneVal, err := client.GetOneZsetWithScoreSameKeyPipe(k, kL, rangeBatch, elementBatch,
				elementInterval, ifSleep, ifBytes)
			if err != nil {
				return nil, err
			}
			if oneVal.BytesCnt < minBytes {
				continue
			}
			result[k] = oneVal
			continue
		} else {
			keyArr = append(keyArr, k)
			alreadyCnt += kL
			if alreadyCnt >= elementBatch {
				rMap, err := client.GetMultiZsetValueByRangeAllPipeWithScore(keyArr, minBytes, ifBytes, ifValue)
				if err != nil {
					return nil, err
				}
				for ky := range rMap {
					result[ky] = rMap[ky]
				}
				keyArr = nil
				alreadyCnt = 0
			}
		}
	}
	if len(keyArr) > 0 {
		rMap, err := client.GetMultiZsetValueByRangeAllPipeWithScore(keyArr, minBytes, ifBytes, ifValue)
		if err != nil {
			return nil, err
		}
		for ky := range rMap {
			result[ky] = rMap[ky]
		}
	}

	return result, nil
}

func (client ClusterAndRedisClient) GetMultiZsetValuesAdaptiveWithoutScore(keys []string, minBytes int64, rangeBatch int64, elementBatch int64,
	elementInterval time.Duration, ifSleep, ifBytes, ifValue bool) (map[string]RedisValueList, error) {
	var (
		err        error
		result     map[string]RedisValueList = map[string]RedisValueList{}
		keyArr     []string
		alreadyCnt int64 = 0
	)
	lengths, err := client.GetElementCntPipe(keys, "zset")
	if err != nil {
		return nil, err
	}

	for k, kL := range lengths {
		if kL >= rangeBatch {
			oneVal, err := client.GetOneValueByRangeSameKeyPipe(k, "zset", kL, rangeBatch, elementBatch,
				elementInterval, ifSleep, ifBytes, ifValue)
			if err != nil {
				return nil, err
			}
			if oneVal.BytesCnt < minBytes {
				continue
			}
			result[k] = oneVal
			continue
		} else {
			keyArr = append(keyArr, k)
			alreadyCnt += kL
			if alreadyCnt >= elementBatch {
				rMap, err := client.GetMultiKeyValuesByRangeAllPipe(keyArr, "zset", minBytes, ifBytes, ifValue)
				if err != nil {
					return nil, err
				}
				for ky := range rMap {
					result[ky] = rMap[ky]
				}
				keyArr = nil
				alreadyCnt = 0
			}
		}
	}
	if len(keyArr) > 0 {
		rMap, err := client.GetMultiKeyValuesByRangeAllPipe(keyArr, "zset", minBytes, ifBytes, ifValue)
		if err != nil {
			return nil, err
		}
		for ky := range rMap {
			result[ky] = rMap[ky]
		}
	}

	return result, nil
}

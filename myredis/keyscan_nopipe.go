package myredis

import (
	"dannytools/constvar"
	"dannytools/ehand"
	"fmt"
	"strconv"
	"time"
	//"github.com/davecgh/go-spew/spew"
)

func (client ClusterAndRedisClient) GetKeyType(key string) (string, error) {
	if client.IsCluster {
		return client.Cluster.Type(key).Result()
	} else {
		return client.Redis.Type(key).Result()
	}

}

/*
delete key by setting expire time of key to little seconds.
*/
func (client ClusterAndRedisClient) DeleteKeysByExpireIt(keyes []string, expTime time.Duration) error {
	var (
		err error
	)

	for _, key := range keyes {
		if client.IsCluster {
			_, err = client.Cluster.Expire(key, expTime).Result()
		} else {
			_, err = client.Redis.Expire(key, expTime).Result()
		}
		if err != nil {
			return ehand.CreateErrorWithStack(err)
		}
	}
	return nil
}

func (client ClusterAndRedisClient) DeleteKeysByDirectlyDelIt(keyes []string) error {
	var (
		err error
	)

	if client.IsCluster {
		_, err = client.Cluster.Del(keyes...).Result()
	} else {
		_, err = client.Redis.Del(keyes...).Result()
	}
	if err != nil {
		return ehand.CreateErrorWithStack(err)
	} else {
		return nil
	}

}

func (client ClusterAndRedisClient) DeleteKeysByUnlinkIt(keyes []string) error {
	var (
		err error
	)

	if client.IsCluster {
		_, err = client.Cluster.Unlink(keyes...).Result()
	} else {
		_, err = client.Redis.Unlink(keyes...).Result()
	}
	if err != nil {
		return ehand.CreateErrorWithStack(err)
	} else {
		return nil
	}

}

func (client ClusterAndRedisClient) GetKeyTTL(key string, timeFmt string) (string, error) {
	var (
		ttl time.Duration
		err error
	)
	if client.IsCluster {
		ttl, err = client.Cluster.TTL(key).Result()
	} else {
		ttl, err = client.Redis.TTL(key).Result()
	}

	if err != nil {
		return "", ehand.WithStackError(err)
	}
	secs := ttl.Seconds()
	if secs == -1 {
		return "-1", nil // expire not set
	} else if secs == -2 {
		return "-2", nil // already expired
	} else {
		return time.Now().Add(ttl).Format(timeFmt), nil
	}

}

func (client ClusterAndRedisClient) GetStringValueMget(keys []string, minBytes int64, ifValue, ifBytes bool) (map[string]RedisValueString, error) {
	var (
		vals   []interface{}
		err    error
		result map[string]RedisValueString = map[string]RedisValueString{}
		i      int
		str    string
		ok     bool
	)
	//fmt.Println(keys)
	if client.IsCluster {
		vals, err = client.Cluster.MGet(keys...).Result()
	} else {
		vals, err = client.Redis.MGet(keys...).Result()
	}
	if err != nil {
		return nil, ehand.CreateErrorWithStack(err)
	}

	for i = range keys {
		str, ok = vals[i].(string)
		if !ok {
			return nil, ehand.CreateStrErrorWithStack(fmt.Sprintf("fail to convert key %s value %v to string", keys[i], vals[i]))
		}
		strVal := RedisValueString{}
		if ifValue {
			strVal.Value = str
		} else {
			strVal.Value = ""
		}

		if ifBytes {
			strVal.BytesCnt = int64(len(keys[i]) + len(str))
			if strVal.BytesCnt < minBytes {
				continue
			}
		} else {
			strVal.BytesCnt = 0
		}
		result[keys[i]] = strVal

	}
	return result, nil
}

func (client ClusterAndRedisClient) GetStringValue(key string, ifBytes bool) (string, int, error) {
	var (
		val string
		err error
		cnt int
	)
	if client.IsCluster {
		val, err = client.Cluster.Get(key).Result()
	} else {
		val, err = client.Redis.Get(key).Result()
	}
	if err != nil {
		return "", 0, ehand.WithStackError(err)
	}

	if ifBytes {
		cnt = len(key) + len(val)
	} else {
		cnt = 0
	}

	return val, cnt, nil
}

func (client ClusterAndRedisClient) DeleteKeyString(key string) error {
	var (
		err error
	)
	if client.IsCluster {
		_, err = client.Cluster.Del(key).Result()
	} else {
		_, err = client.Redis.Del(key).Result()
	}

	if err != nil {
		return ehand.WithStackError(err)
	} else {
		return nil
	}

}

func (client ClusterAndRedisClient) GetListLen(key string) (int64, error) {
	var (
		err    error
		length int64
	)

	if client.IsCluster {
		length, err = client.Cluster.LLen(key).Result()
	} else {
		length, err = client.Redis.LLen(key).Result()
	}

	if err != nil {
		return 0, ehand.WithStackError(err)
	}

	return length, nil

}

/*
param: key name, key length, count of elements to get each time, sleep after this process elementBatch element, esleep time for elementInterval in microsecond, if sleep, if calculate memory used in bytes
return: value, bytes of the key, biggest element, bytes of the biggest element, error

*/
func (client ClusterAndRedisClient) GetListValue(key string, length int64, eachCnt int64, elementBatch int,
	elementInterval time.Duration, ifSleep bool, ifBytes bool) ([]string, int, string, int, error) {
	var (
		err        error
		val        []string
		tmpArr     []string
		i          int64 = 0
		stop       int64
		bytesCnt   int = len(key)
		maxElement string
		maxBytes   int = 0
		oneBytes   int = 0
		actualCnt  int = 0
		alreadyCnt int = 0
	)

	for {
		if i >= length {
			break
		}
		if i+eachCnt >= length {
			stop = length
		} else {
			stop = i + eachCnt
		}
		if client.IsCluster {
			tmpArr, err = client.Cluster.LRange(key, i, stop).Result()
		} else {
			tmpArr, err = client.Redis.LRange(key, i, stop).Result()
		}

		if err != nil {
			return val, bytesCnt, maxElement, maxBytes, ehand.WithStackError(err)
		}
		if ifBytes {
			for _, v := range tmpArr {
				oneBytes = len(v)
				bytesCnt += oneBytes
				if oneBytes > maxBytes {
					maxElement = v
					maxBytes = oneBytes
				}
			}
		}
		val = append(val, tmpArr...)

		actualCnt = len(tmpArr)
		i += int64(actualCnt) + 1

		if ifSleep {
			alreadyCnt += actualCnt
			if alreadyCnt > elementBatch {
				time.Sleep(elementInterval)
				alreadyCnt = 0
			}

		}

	}

	return val, bytesCnt, maxElement, maxBytes, nil
}

/*
param: 	param: key name, key length, count of elements to delete each time, sleep after this process elementBatch element,
		sleep time for elementInterval in microsecond, if sleep
return: error
*/

func (client ClusterAndRedisClient) DeleteKeyList(key string, length int64, eachCnt int64, elementBatch int64,
	elementInterval time.Duration, ifSleep bool) error {

	var (
		err         error
		i           int64 = 0
		sidx        int64 = 0
		eidx        int64
		oneBatchCnt int64 = 0
	)

	for {
		eidx = 0 - eachCnt - 1
		if client.IsCluster {
			_, err = client.Cluster.LTrim(key, sidx, eidx).Result()
		} else {
			_, err = client.Redis.LTrim(key, sidx, eidx).Result()
		}
		if err != nil {
			return ehand.WithStackError(err)
		}
		i += eachCnt
		if i > length {
			break
		}
		if ifSleep {
			oneBatchCnt += eachCnt
			if oneBatchCnt >= elementBatch {
				time.Sleep(elementInterval)
				oneBatchCnt = 0
			}
		}

	}

	//the key should be not exists now, but Maybe other client push element at the same time. delete it whatever. delete unexists key not result in error
	if client.IsCluster {
		_, err = client.Cluster.Del(key).Result()
	} else {
		_, err = client.Redis.Del(key).Result()
	}

	if err != nil {
		return ehand.WithStackError(err)
	} else {
		return nil
	}
}

/*
param:  key name, count of elements to get each time, sleep after processing elementBatch elements,
		sleep time for elementInterval in microsecond, if sleep, if calculate memory used in bytes
return: value, bytes of the key, biggest element, bytes of the biggest element, error

*/

func (client ClusterAndRedisClient) GetHashValue(key string, eachCnt int64, elementBatch int,
	elementInterval time.Duration, ifSleep bool, ifBytes bool) (map[string]interface{},
	int, string, int, int, error) {
	var (
		cursor     uint64 = 0
		eKeys      []string
		values     map[string]interface{} = map[string]interface{}{}
		bytesCnt   int                    = len(key)
		maxElement string
		maxBytes   int = 0
		oneBytes   int = 0
		alreadyCnt int = 0
		elementCnt int = 0

		err error
	)

	for {

		if client.IsCluster {
			eKeys, cursor, err = client.Cluster.HScan(key, cursor, "", eachCnt).Result()

		} else {
			eKeys, cursor, err = client.Redis.HScan(key, cursor, "", eachCnt).Result()

		}
		if err != nil {
			return values, bytesCnt, maxElement, maxBytes, elementCnt, ehand.WithStackError(err)
		}

		for i := 0; i < len(eKeys); i += 2 {
			alreadyCnt++
			elementCnt++
			values[eKeys[i]] = eKeys[i+1]
			if ifBytes {
				oneBytes = len(eKeys[i]) + len(eKeys[i+1])
				bytesCnt += oneBytes
				if oneBytes > maxBytes {
					maxBytes = oneBytes
					maxElement = eKeys[i]
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

	return values, bytesCnt, maxElement, maxBytes, elementCnt, nil
}

/*
param:  key name, count of elements to get each time, sleep after processing elementBatch elements,
		sleep for elementInterval in microsecond, if calculate memory used in bytes
return: value, bytes of the key, biggest element, bytes of the biggest element, error

*/
func (client ClusterAndRedisClient) GetSetValue(key string, eachCnt int64, elementBatch int,
	elementInterval time.Duration, ifSleep bool, ifBytes bool) ([]string, int, string, int, int, error) {
	var (
		err        error
		cursor     uint64 = 0
		oneVals    []string
		values     []string = []string{}
		bytesCnt   int      = len(key)
		maxElement string
		maxBytes   int = 0
		oneByte    int = 0
		alreadyCnt int = 0

		elementCnt int = 0
	)

	for {
		if client.IsCluster {
			oneVals, cursor, err = client.Cluster.SScan(key, cursor, "", eachCnt).Result()
		} else {
			oneVals, cursor, err = client.Redis.SScan(key, cursor, "", eachCnt).Result()
		}
		if err != nil {
			return values, bytesCnt, maxElement, maxBytes, elementCnt, ehand.WithStackError(err)
		}
		elementCnt += len(oneVals)

		values = append(values, oneVals...)

		if ifBytes {
			for _, v := range oneVals {
				oneByte = len(v)
				bytesCnt += oneByte
				if oneByte > maxBytes {
					maxBytes = oneByte
					maxElement = v
				}

			}
		}
		if cursor == 0 {
			break
		}

		if ifSleep {
			alreadyCnt += len(oneVals)
			if alreadyCnt > elementBatch {
				time.Sleep(elementInterval)
			}

		}

	}

	return values, bytesCnt, maxElement, maxBytes, elementCnt, nil
}

/*
param: key name, count of elements to get each time, sleep time for eachCnt in microsecond, if calculate memory used in bytes
return: value, bytes of the key, biggest element, bytes of the biggest element, error

*/

func (client ClusterAndRedisClient) GetZsetValue(key string, eachCnt int64, elementBatch int,
	elementInterval time.Duration, ifSleep bool, ifBytes bool) (map[string]float64, int, string, int, int, error) {
	var (
		err        error
		cursor     uint64 = 0
		oneKeys    []string
		values     map[string]float64 = map[string]float64{}
		bytesCnt   int                = len(key)
		maxElement string
		maxBytes   int = 0
		oneByte    int
		score      float64
		alreadyCnt int = 0
		elementCnt int = 0
	)

	for {
		if client.IsCluster {
			oneKeys, cursor, err = client.Cluster.ZScan(key, cursor, "", eachCnt).Result()
		} else {
			oneKeys, cursor, err = client.Redis.ZScan(key, cursor, "", eachCnt).Result()
		}
		if err != nil {
			return values, bytesCnt, maxElement, maxBytes, elementCnt, ehand.WithStackError(err)
		}

		for i := 0; i < len(oneKeys); i += 2 {
			if ifSleep {
				alreadyCnt++
			}
			elementCnt++
			score, err = strconv.ParseFloat(oneKeys[i+1], 64)
			if err != nil {
				return values, bytesCnt, maxElement, maxBytes, elementCnt, ehand.WithStackError(err)
			}
			values[oneKeys[i]] = score
			if ifBytes {
				oneByte = len(oneKeys[i])
				bytesCnt += oneByte + 8 // each float64 number takes 8 bytes
				if oneByte > maxBytes {
					maxBytes = oneByte
					maxElement = oneKeys[i]
				}
			}

		}

		if cursor == 0 {
			break
		}

		if ifSleep {
			if alreadyCnt > elementBatch {
				time.Sleep(elementInterval)
			}
		}

	}
	maxBytes += 8
	return values, bytesCnt, maxElement, maxBytes, elementCnt, nil
}

/*
 get one key info and value
*/
func (client ClusterAndRedisClient) GetKeyValue(key string, eachCnt int64, elementBatch int, elementInterval time.Duration, ifSleep bool,
	ifCountMem bool, ifNeedValue bool) (RedisKey, error) {
	var (
		kv         RedisKey = RedisKey{Key: key}
		err        error
		tp         string
		exp        string
		values     interface{}
		bytesCnt   int
		maxElement string = ""
		maxBytes   int    = 0
		elementCnt int    = 0
	)

	tp, err = client.GetKeyType(key)
	//fmt.Printf("type of %s: %s\n", key, tp)
	if err != nil {
		return kv, err
	}
	kv.Type = tp

	exp, err = client.GetKeyTTL(key, constvar.DATETIME_FORMAT_NOSPACE)
	if err != nil {
		return kv, err
	}
	kv.Expire = exp
	// already expire
	if exp == "-2" {
		return kv, nil
	}

	switch tp {
	case "string":
		values, bytesCnt, err = client.GetStringValue(key, ifCountMem)
		if err != nil {
			return kv, err
		}
		elementCnt = 1
		//kv.Bytes = bytesCnt

		//kv.Value = values

	case "list":
		length, err := client.GetListLen(key)
		if err != nil {
			return kv, err
		}
		values, bytesCnt, maxElement, maxBytes, err = client.GetListValue(key, length, eachCnt, elementBatch, elementInterval, ifSleep, ifCountMem)
		if err != nil {
			return kv, err
		}
		elementCnt = int(length)

	case "hash":
		values, bytesCnt, maxElement, maxBytes, elementCnt, err = client.GetHashValue(key, eachCnt, elementBatch, elementInterval, ifSleep, ifCountMem)
		if err != nil {
			return kv, err
		}

	case "set":
		values, bytesCnt, maxElement, maxBytes, elementCnt, err = client.GetSetValue(key, eachCnt, elementBatch, elementInterval, ifSleep, ifCountMem)
		if err != nil {
			return kv, err
		}
	case "zset":
		values, bytesCnt, maxElement, maxBytes, elementCnt, err = client.GetZsetValue(key, eachCnt, elementBatch, elementInterval, ifSleep, ifCountMem)
		if err != nil {
			return kv, err
		}

	}
	kv.Bytes = int64(bytesCnt)
	kv.MaxElement = maxElement
	kv.MaxElementBytes = int64(maxBytes)
	kv.ElementCnt = int64(elementCnt)

	if ifNeedValue {
		kv.Value = values
	} else {
		values = nil
	}
	return kv, nil

}

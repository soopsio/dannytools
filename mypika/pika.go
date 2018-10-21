package mypika

import (
	//"dannytools/ehand"
	"dannytools/myredis"
	"strconv"
	"strings"
)

type PikaInfoAll struct {
	Process_id                 int32
	Config_file                string
	Server_id                  uint64
	Db_size                    uint64
	Used_memory                uint64
	Db_memtable_usage          uint64
	Db_tablereader_usage       uint64
	Connected_clients          uint64
	Total_connections_received uint64
	Instantaneous_ops_per_sec  float64
	Accumulative_query_nums    uint64 //pika 2.3.0
	Total_commands_processed   uint64 // pika 2.2.5
	Is_bgsaving                int8
	Is_scaning_keyspace        int8
	Is_compact                 int8
	Role                       string
	Master_host                string
	Master_port                uint64
	IsSlave                    int8
	Master_link_status         int8 //1; up, 0: down
	Slave_read_only            int8 // 1:readonly, 0: read and write
	KeysCnt                    uint64
	KeysCntHash                uint64
	KeysCntList                uint64
	KeysCntZset                uint64
	KeysCntSet                 uint64
}

func GetPikaInfoAll(client myredis.ClusterAndRedisClient) (PikaInfoAll, error) {
	var (
		err    error
		result map[string]string
		info   PikaInfoAll = PikaInfoAll{}
	)
	// server
	result, err = client.GetRedisInfo("server")
	if err != nil {
		return info, err
	}
	ParsePikaInfoSever(result, &info)

	//data
	result, err = client.GetRedisInfo("data")
	if err != nil {
		return info, err
	}
	ParsePikaInfoData(result, &info)

	//clients
	result, err = client.GetRedisInfo("clients")
	if err != nil {
		return info, err
	}
	ParsePikaInfoClients(result, &info)

	//stats
	result, err = client.GetRedisInfo("stats")
	if err != nil {
		return info, err
	}
	ParsePikaInfoStats(result, &info)

	//Replication
	result, err = client.GetRedisInfo("replication")
	if err != nil {
		return info, err
	}
	ParsePikaInfoRepl(result, &info)

	//Keyspace
	result, err = client.GetRedisInfo("keyspace")
	if err != nil {
		return info, err
	}
	ParsePikaInfoKeyspace(result, &info)

	return info, nil
}

/*
# Server
pika_version:2.3.0 -------------------------------------- pika 版本信息
pika_git_sha:3668a2807a3d047ea43656b58a2130c1566eeb65 --- git的sha值
pika_build_compile_date: Nov 14 2017 -------------------- pika的编译日期
os:Linux 2.6.32-2.0.0.8-6 x86_64 ------------------------ 操作系统信息
arch_bits:64 -------------------------------------------- 操作系统位数
process_id:12969 ---------------------------------------- pika pid信息
tcp_port:9001 ------------------------------------------- pika 端口信息
thread_num:12 ------------------------------------------- pika 线程数量
sync_thread_num:6 --------------------------------------- sync线程数量
uptime_in_seconds:3074 ---------------------------------- pika 运行时间（秒）
uptime_in_days:0 ---------------------------------------- pika 运行时间（天）
config_file:/data1/pika9001/pika9001.conf --------------- pika conf文件位置
server_id:1 --------------------------------------------- pika的server id
*/

func ParsePikaInfoSever(result map[string]string, info *PikaInfoAll) {

	var (
		k       string
		v       string
		tmpUint uint64

		err error
	)

	for k, v = range result {
		switch k {
		case "process_id":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Process_id = int32(tmpUint)
			} else {
				info.Process_id = 0
			}
		case "config_file":
			info.Config_file = v
		case "server_id":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Server_id = tmpUint
			} else {
				info.Server_id = 0
			}

		}
	}
}

/*
# Data
db_size:770439 ------------------------------------------ db的大小(Byte)
db_size_human:0M ---------------------------------------- 人类可读的db大小(M)
compression:snappy -------------------------------------- 压缩方式
used_memory:4248 ---------------------------------------- 使用的内存大小(Byte)
used_memory_human:0M ------------------------------------ 人类可读的使用的内存大小(M)
db_memtable_usage:4120 ---------------------------------- memtable的使用量(Byte)
db_tablereader_usage:128 --------------------------------
tablereader的使用量(Byte)
*/

func ParsePikaInfoData(result map[string]string, info *PikaInfoAll) {

	var (
		k       string
		v       string
		tmpUint uint64

		err error
	)

	for k, v = range result {
		switch k {
		case "db_size":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Db_size = tmpUint
			} else {
				info.Db_size = 0
			}
		case "used_memory":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Used_memory = tmpUint
			} else {
				info.Used_memory = 0
			}
		case "db_memtable_usage":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Db_memtable_usage = tmpUint
			} else {
				info.Db_memtable_usage = 0
			}
		case "db_tablereader_usage":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Db_tablereader_usage = tmpUint
			} else {
				info.Db_tablereader_usage = 0
			}

		}
	}
}

/*
# Clients
connected_clients:2 ------------------------------------- 当前连接数
*/

func ParsePikaInfoClients(result map[string]string, info *PikaInfoAll) {

	var (
		k       string
		v       string
		tmpUint uint64

		err error
	)

	for k, v = range result {
		switch k {
		case "connected_clients":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Connected_clients = tmpUint
			} else {
				info.Connected_clients = 0
			}
		}
	}
}

/*
# Stats
total_connections_received:18 --------------------------- 总连接次数统计
instantaneous_ops_per_sec:1 ----------------------------- 当前qps
accumulative_query_nums:633 ----------------------------- 请求总计
is_bgsaving:No, , 0 ------------------------------------- pika 备份信息：是否在备份,备份名称，备份
is_scaning_keyspace:No ---------------------------------- 是否在执行scan操作
is_compact:No ------------------------------------------- 是否在执行数据压缩操作
compact_cron: ------------------------------------------- 定时compact(format: start-end/ratio, eg. 02-04/60)
compact_interval: --------------------------------------- compact的间隔(format: interval/ratio, eg. 6/60)

total_commands_processed:654545975485 ----2.2.5
*/

func ParsePikaInfoStats(result map[string]string, info *PikaInfoAll) {

	var (
		k        string
		v        string
		tmpUint  uint64
		tmpFloat float64

		err error
	)

	for k, v = range result {
		switch k {
		case "total_connections_received":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Total_connections_received = tmpUint
			} else {
				info.Total_connections_received = 0
			}
		case "instantaneous_ops_per_sec":
			tmpFloat, err = strconv.ParseFloat(v, 64)
			if err == nil {
				info.Instantaneous_ops_per_sec = tmpFloat
			} else {
				info.Instantaneous_ops_per_sec = 0
			}
		case "accumulative_query_nums":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Accumulative_query_nums = tmpUint
			} else {
				info.Accumulative_query_nums = 0
			}
		case "total_commands_processed":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Total_commands_processed = tmpUint
			} else {
				info.Total_commands_processed = 0
			}
		case "is_bgsaving":
			arr := strings.Split(v, ",")
			arr[0] = strings.ToUpper(strings.TrimSpace(arr[0]))
			if arr[0] == "NO" {
				info.Is_bgsaving = 0
			} else if arr[0] == "YES" {
				info.Is_bgsaving = 1
			}
		case "is_scaning_keyspace":
			v = strings.ToUpper(v)
			if v == "NO" {
				info.Is_scaning_keyspace = 0
			} else if v == "YES" {
				info.Is_scaning_keyspace = 1
			}
		case "is_compact":
			v = strings.ToUpper(v)
			if v == "NO" {
				info.Is_compact = 0
			} else if v == "YES" {
				info.Is_compact = 1
			}
		}
	}
}

/*
# Replication(MASTER)
role:master --------------------------------------------- 本实例角色
connected_slaves:1 -------------------------------------- 当前从库数量
slave0: host_port=xxx:57765 state=online -------- 当前从库连接信息，如果有多个从库则依次展示

#从库（区别仅在于同步信息的展示）：
# Replication(SLAVE)
role:slave ---------------------------------------------- 本实例角色
master_host:xxxx --------------------------------- 主库IP
master_port:9001 ---------------------------------------- 主库端口
master_link_status:up ----------------------------------- 当前同步状态
slave_read_only:1 --------------------------------------- 从库是否readonly
*/
func ParsePikaInfoRepl(result map[string]string, info *PikaInfoAll) {

	var (
		k       string
		v       string
		tmpUint uint64

		err error
	)

	for k, v = range result {
		switch k {
		case "role":
			info.Role = v
			if v == "master" {
				info.IsSlave = 0
			} else {
				info.IsSlave = 1
			}
		case "master_host":
			info.Master_host = v

		case "master_port":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Master_port = tmpUint
			} else {
				info.Master_port = 0
			}
		case "connected_slaves":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Connected_clients = tmpUint
			} else {
				info.Connected_clients = 0
			}
		case "master_link_status":
			v = strings.ToLower(v)
			if v == "up" {
				info.Master_link_status = 1
			} else {
				info.Master_link_status = 0
			}
		case "slave_read_only":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Slave_read_only = int8(tmpUint)
			} else {
				info.Slave_read_only = 0
			}

		}
	}
}

/*
# Keyspace（key数量展示，按照数据类型分类展示，默认不更新，仅在执行info keyspace 1的时候刷新该信息）
# Time:2016-04-22 17:08:33 ------------------------------ 上一次统计的时间
kv   keys:43
hash keys:503
list keys:0
zset keys:0
set  keys:0

# Keyspace
# Time:1970-01-01 08:00:00
kv keys:0
hash keys:0
list keys:0
zset keys:0
set keys:0
*/
func ParsePikaInfoKeyspace(result map[string]string, info *PikaInfoAll) {

	var (
		k       string
		v       string
		tmpUint uint64
		tmpArr  []string

		err error
	)

	for k, v = range result {
		tmpArr = strings.Fields(k)
		switch tmpArr[0] {
		case "kv":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.KeysCnt = tmpUint
			} else {
				info.KeysCnt = 0
			}
		case "hash":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.KeysCntHash = tmpUint
			} else {
				info.KeysCntHash = 0
			}
		case "list":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.KeysCntList = tmpUint
			} else {
				info.KeysCntList = 0
			}
		case "zset":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.KeysCntZset = tmpUint
			} else {
				info.KeysCntZset = 0
			}
		case "set":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.KeysCntSet = tmpUint
			} else {
				info.KeysCntSet = 0
			}
		}
	}
}

type PikaConfVar struct {
	Maxmemory          uint64
	TargetFileSizeBase uint64
	Maxclients         uint64
	//SlaveReadOnly      uint64
}

func GetPikaConfVars(client myredis.ClusterAndRedisClient, ifBreakOnErr bool) (PikaConfVar, error) {
	var (
		err     error
		confVar PikaConfVar = PikaConfVar{}
		tmpUint uint64
	)
	//result, err := client.Redis.ConfigGet("maxmemory").Result()
	tmpUint, err = client.GetRedisOneVarUint64("maxmemory")
	if err != nil {
		if ifBreakOnErr {
			return confVar, err
		}
	} else {
		confVar.Maxmemory = tmpUint
	}

	tmpUint, err = client.GetRedisOneVarUint64("maxclients")
	if err != nil {
		if ifBreakOnErr {
			return confVar, err
		}
	} else {
		confVar.Maxclients = tmpUint
	}

	tmpUint, err = client.GetRedisOneVarUint64("target-file-size-base")
	if err != nil {
		if ifBreakOnErr {
			return confVar, err
		}
	} else {
		confVar.TargetFileSizeBase = tmpUint
	}
	/*
		tmpUint, err = client.GetRedisOneVarUint64("slave-read-only")
		if err != nil {
			if ifBreakOnErr {
				return confVar, err
			}
		} else {
			confVar.SlaveReadOnly = tmpUint
		}
	*/
	return confVar, nil
}

package myredis

import (
	"dannytools/ehand"
	"dannytools/mystr"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	//"github.com/davecgh/go-spew/spew"
	"github.com/go-redis/redis"
	kitSlice "github.com/toolkits/slice"
)

const (
	CRoleMaster      uint8 = 0
	CRoleSlave       uint8 = 1
	CRoleMasterSlave uint8 = 2
)

var (
	G_regexp_info_keyspace_db       *regexp.Regexp = regexp.MustCompile(`db\d+`)
	G_regexp_info_replication_slave *regexp.Regexp = regexp.MustCompile(`slave\d+`)
)

type RedisServer struct {
	IsCluster  bool
	Addr       RedisAddr
	Role       uint8 // 0:master, 1:slave, 2:master and slave
	Version    string
	Master     RedisAddr
	Slaves     map[string]*RedisReplStatus
	ReplStatus *RedisReplStatus
}

func (this *RedisServer) IsMaster() bool {
	if this.Role == CRoleMaster {
		return true
	} else {
		return false
	}
}

func (this *RedisServer) IsSlave() bool {
	if this.Role == CRoleMaster {
		return false
	} else {
		return true
	}
}

func (this *RedisServer) IsMasterAndSlave() bool {
	if this.Role == CRoleMasterSlave {
		return true
	} else {
		return false
	}
}

func GetInfoStringOnlyOneSlaveAndMaster(ifSlaveFirst bool, slave, master *RedisServer) string {
	if ifSlaveFirst {
		return slave.InfoString() + " --> " + master.InfoString()
	} else {
		return master.InfoString() + " <-- " + slave.InfoString()
	}
}

func GetInfoStringOnlyOneNode(node *RedisServer, masters map[string]*RedisServer) string {
	infStr := node.InfoString()
	if node.IsSlave() {
		if _, ok := masters[node.Master.AddrString()]; ok {
			infStr += " --> " + masters[node.Master.AddrString()].InfoString()
		}
	}
	return infStr
}

func GetInfoStringForNodes(prefixMsg string, nodes map[string]*RedisServer, masters map[string]*RedisServer) string {
	var (
		arr []string
	)
	for _, oneNode := range nodes {

		arr = append(arr, GetInfoStringOnlyOneNode(oneNode, masters))
	}
	if prefixMsg != "" {
		return fmt.Sprintf("%s\n\t%s", prefixMsg, strings.Join(arr, "\n\t"))
	} else {
		return fmt.Sprintf("\n\t%s", strings.Join(arr, "\n\t"))
	}
}

func (this *RedisServer) CreateGenClientPool(readonly bool, db int, poolSizeEachCpu int) (ClusterAndRedisClient, error) {
	return this.Addr.CreateRedisOrClusterConPool(this.IsCluster, readonly, db, poolSizeEachCpu)
}

func (this *RedisServer) GetMasterAddrStr() string {
	return this.Master.AddrString()
}
func (this *RedisServer) GetSlavesAddrStr() []string {
	var arr []string
	for k := range this.Slaves {
		arr = append(arr, k)
	}
	return arr
}

func (this *RedisServer) GetAllNodesAddrStr() []string {
	var arr []string
	arr = append(arr, this.Addr.AddrString())
	if this.Master.Port != 0 {
		if arr[0] != this.Master.AddrString() {
			arr = append(arr, this.Master.AddrString())
		}
	}
	if len(this.Slaves) > 0 {
		for _, k := range this.GetSlavesAddrStr() {
			if !kitSlice.ContainsString(arr, k) {
				arr = append(arr, k)
			}
		}
	}
	return arr
}

func (this *RedisServer) GetLatestSlave() (RedisAddr, error) {
	var (
		latest *RedisReplStatus = &RedisReplStatus{SlaveAddr: RedisAddr{Port: 0}}
	)
	if len(this.Slaves) == 0 {
		return RedisAddr{}, fmt.Errorf("no replication slave")
	}
	//spew.Dump(*this)
	for _, rpl := range this.Slaves {
		//spew.Dump(rpl)
		if !rpl.IfOnline {
			continue
		} else if latest.SlaveAddr.Port == 0 {
			latest = rpl
		}
		if rpl.Lag < latest.Lag {
			latest = rpl
		}
	}
	if latest.SlaveAddr.Port == 0 {
		return latest.SlaveAddr, fmt.Errorf("no replication slave")
	} else {
		return latest.SlaveAddr, nil
	}
}

func (this *RedisServer) InfoString() string {
	str := ""
	if this.IsCluster {
		str += "cluster "
	} else {
		str += "redis "
	}
	str += this.Addr.AddrString()

	str += " " + this.Version
	if this.Role == CRoleMaster {
		str += " master"
	} else if this.Role == CRoleSlave {
		str += " slave"
	} else if this.Role == CRoleMasterSlave {
		str += " masterAndSlave"
	}
	return str
}

func (this *RedisServer) GetAndSetRedisInfo() error {
	var (
		client ClusterAndRedisClient
		err    error
		tmpInt int64
	)
	client, err = this.Addr.CreateRedisOrClusterConPool(false, false, 0, 2)
	defer client.Close()
	if err != nil {

		return err
	}
	infoServer, err := client.GetRedisInfo("server")
	if err != nil {
		return err
	}
	this.Version = infoServer["redis_version"]
	if infoServer["redis_mode"] == "standalone" {
		this.IsCluster = false
	} else {
		this.IsCluster = true
	}

	infoRepl, err := client.GetRedisInfo("replication")
	if err != nil {
		return err
	}

	if infoRepl["role"] == "master" {
		this.Role = CRoleMaster
	} else {
		this.Role = CRoleSlave
		tmpInt, err = strconv.ParseInt(infoRepl["master_port"], 10, 32)
		if err == nil {
			this.Master = RedisAddr{Host: infoRepl["master_host"], Port: int(tmpInt)}
			masterClient, err := this.Master.CreateRedisOrClusterConPool(false, false, 0, 2)
			defer masterClient.Close()
			if err == nil {
				masterInfo, err := masterClient.GetRedisInfo("replication")
				if err == nil {
					slaves := ParseMasterReplStatus(masterInfo)
					if len(slaves) > 0 {
						if _, ok := slaves[this.Addr.AddrString()]; ok {
							this.ReplStatus = slaves[this.Addr.AddrString()]
						}
					}
				} else {
					return err
				}
			} else {
				return err
			}

		} else {
			return err
		}
	}

	if infoRepl["connected_slaves"] != "0" {
		if this.Role == CRoleSlave {
			this.Role = CRoleMasterSlave
		}
		slaves := ParseMasterReplStatus(infoRepl)
		if len(slaves) > 0 {
			this.Slaves = slaves
		}
	}
	return nil
}

//slave0:ip=xxx,port=6683,state=online,offset=2047641,lag=1
type SlaveInfo struct {
	Addr        RedisAddr
	Replicating bool // state=online
	Lag         int
	Offset      int
}

type RedisRoleInfo struct {
	Role        string // master or slave
	MasterAddr  RedisAddr
	Replicating bool        // for slave, master_link_status:up
	Slaves      []SlaveInfo // for master
}

func (client ClusterAndRedisClient) GetRedisInfo(section string) (map[string]string, error) {
	var (
		str    string
		err    error
		result map[string]string = map[string]string{}
	)

	str, err = client.Redis.Info(section).Result()
	if err != nil {
		return result, ehand.CreateErrorWithStack(err)
	}
	str = strings.TrimSpace(str)
	arr := strings.Split(str, "\n")
	for _, line := range arr {
		if strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(line)
		tarr := strings.Split(line, ":")
		if len(tarr) != 2 {
			continue
		}
		result[tarr[0]] = tarr[1]
	}

	return result, nil

}

//slave0:ip=xxx,port=6683,state=online,offset=2047641,lag=1
func (client ClusterAndRedisClient) GetSlaveReplInfo(infoStr string) (SlaveInfo, error) {
	var (
		info SlaveInfo = SlaveInfo{}
		err  error
	)

	infoStr = strings.TrimSpace(infoStr)
	arr := strings.Split(infoStr, ",")

	for _, str := range arr {
		tarr := strings.Split(str, "=")
		switch tarr[0] {
		case "ip":
			info.Addr.Host = tarr[1]
		case "port":
			info.Addr.Port, err = strconv.Atoi(tarr[1])
			if err != nil {
				return info, ehand.WithStackError(err)
			}
		case "state":
			if tarr[1] == "online" {
				info.Replicating = true
			} else {
				info.Replicating = false
			}
		case "offset":
			info.Offset, err = strconv.Atoi(tarr[1])
			if err != nil {
				return info, ehand.WithStackError(err)
			}
		case "lag":
			info.Lag, err = strconv.Atoi(tarr[1])
			if err != nil {
				return info, ehand.WithStackError(err)
			}
		}
	}
	return info, nil
}

/*
# Replication
role:master
connected_slaves:1
slave0:ip=xxx,port=6683,state=online,offset=2047641,lag=1
master_repl_offset:2047641

role:slave
master_host:xxx
master_port:6683
master_link_status:up
master_last_io_seconds_ago:1
master_sync_in_progress:0
slave_repl_offset:2047571
slave_priority:100

*/

func (client ClusterAndRedisClient) GetRedisInfoRole() (RedisRoleInfo, error) {
	var (
		err   error
		info  RedisRoleInfo = RedisRoleInfo{}
		reMap map[string]string
		sInfo SlaveInfo
		//mtch  *regexp.Regexp = regexp.MustCompile(`slave\d+`)
	)

	reMap, err = client.GetRedisInfo("replication")
	if err != nil {
		return info, err
	}

	if reMap["role"] == "master" {
		info.Role = "master"
		info.Slaves = []SlaveInfo{}
		for k, v := range reMap {
			if G_regexp_info_replication_slave.MatchString(k) {
				//slave0:ip=10.199.203.210,port=6683,state=online,offset=2047641,lag=1
				sInfo, err = client.GetSlaveReplInfo(v)
				if err != nil {
					return info, err
				}
				info.Slaves = append(info.Slaves, sInfo)
			}
		}

	} else {
		info.Role = "slave"
		info.MasterAddr.Host = reMap["master_host"]
		info.MasterAddr.Port, err = strconv.Atoi(reMap["master_port"])
		if err != nil {
			return info, ehand.WithStackError(err)
		}
		if reMap["master_link_status"] == "up" {
			info.Replicating = true
		} else {
			info.Replicating = false
		}
	}

	return info, nil
}

/*

# Server
redis_version:3.0.3
redis_git_sha1:00000000
redis_git_dirty:0
redis_build_id:eff44f0690b88743
redis_mode:cluster

redis_version:3.0.3
redis_git_sha1:00000000
redis_git_dirty:0
redis_build_id:6d98d05e9f9e8598
redis_mode:standalone
*/

func (client ClusterAndRedisClient) CheckIfCluster() (bool, error) {
	var (
		err   error
		reMap map[string]string
	)

	reMap, err = client.GetRedisInfo("server")
	if err != nil {
		return false, err
	}

	if reMap["redis_mode"] == "cluster" {
		return true, nil
	} else {
		return false, nil
	}

}

func (client ClusterAndRedisClient) GetLatestSlave(slaves []SlaveInfo) (SlaveInfo, error) {
	var (
		lestLag int = -1
		sl      SlaveInfo
		//ok      bool = false
	)
	for _, s := range slaves {
		if !s.Replicating {
			continue
		}

		if lestLag == -1 || s.Lag <= lestLag {
			sl = s
			//ok = tru
			lestLag = s.Lag
		}
	}
	if lestLag == -1 {
		return sl, ehand.WithStackError(fmt.Errorf("no online slave found"))
	}

	return sl, nil

}

func GetRedisAddr(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

/*
param: addrs of masters
return: latest slave addrs of masters(map)， latest slave addrs of masters(list), masters without replicating slave, error
*/

func GetLatestSlaveOfMasters(masters []string) (map[string]string, []string, []string, error) {
	var (
		redisConf      ConfRedis
		redisClient    *redis.Client
		client         ClusterAndRedisClient
		err            error
		slavesMap      map[string]string = map[string]string{}
		slaveList      []string          = []string{}
		mastersNoSlave []string          = []string{}
		roleInfo       RedisRoleInfo
		slaveInfo      SlaveInfo
	)

	for _, m := range masters {
		redisConf = ConfRedis{Addr: m, Database: 0}
		redisClient, err = redisConf.CreateNewClientRedis()
		if err != nil {
			return slavesMap, slaveList, mastersNoSlave, err
		}
		client = ClusterAndRedisClient{Redis: redisClient, IsCluster: false}
		roleInfo, err = client.GetRedisInfoRole()
		if err != nil {
			return slavesMap, slaveList, mastersNoSlave, err
		}
		if len(roleInfo.Slaves) > 0 {
			slaveInfo, err = client.GetLatestSlave(roleInfo.Slaves)
			if err != nil {
				// no online slaves
				mastersNoSlave = append(mastersNoSlave, m)
				continue
			}
			oneAddr := fmt.Sprintf("%s:%d", slaveInfo.Addr.Host, slaveInfo.Addr.Port)
			slavesMap[m] = oneAddr
			slaveList = append(slaveList, oneAddr)

		} else {
			mastersNoSlave = append(mastersNoSlave, m)
		}
		redisClient.Close()

	}
	return slavesMap, slaveList, mastersNoSlave, nil

}

type RedisInfoAll struct {
	Redis_version              string
	IsCluster                  int64 // cluster:1, redis:0
	Process_id                 uint64
	Total_connections_received uint64
	Connected_clients          uint64
	Rejected_connections       uint64
	Blocked_clients            uint64
	Client_longest_output_list uint64
	Client_biggest_input_buf   uint64
	Total_commands_processed   uint64
	Instantaneous_ops_per_sec  float64
	Keyspace_hits              uint64
	Keyspace_misses            uint64
	Expired_keys               uint64
	Evicted_keys               uint64
	Keys_alldb                 uint64 // number of keys of all db
	Keys_expires               uint64 // number of keys with expiration of all db
	Total_net_input_bytes      uint64
	Total_net_output_bytes     uint64
	Instantaneous_input_kbps   float64
	Instantaneous_output_kbps  float64
	Used_memory                uint64
	Used_memory_rss            uint64
	Used_memory_peak           uint64
	Used_memory_lua            uint64
	Mem_fragmentation_ratio    float64
	LoadingRdb                 int64
	Rdb_bgsave_in_progress     int64
	Rdb_last_bgsave_status     int64 // ok: 0, error: 1
	Aof_rewrite_in_progress    int64
	Aof_last_bgrewrite_status  int64 // ok: 0, error: 1
	Aof_last_write_status      int64 // ok: 0, error: 1
	Pubsub_channels            uint64
	Pubsub_patterns            uint64
	Role                       string
	Connected_slaves           uint64
	Master_host                string
	Master_port                uint64
	Master_link_status         int64 // up: 1, else: 0
	SlaveLag                   int64
	Master_sync_in_progress    int64
}

// selfIps: the ips of the slave, used to get slave lag from master

func (client ClusterAndRedisClient) GetRedisInfoAll(selfIps []string, port int, ifGetSlaveLag bool) (RedisInfoAll, error) {
	var (
		err    error
		result map[string]string
		info   RedisInfoAll = RedisInfoAll{}
	)
	// server
	result, err = client.GetRedisInfo("server")
	if err != nil {
		return info, err
	}
	ParseInfoSever(result, &info)

	//clients
	result, err = client.GetRedisInfo("clients")
	if err != nil {
		return info, err
	}
	ParseInfoClient(result, &info)

	// memory
	result, err = client.GetRedisInfo("memory")
	if err != nil {
		return info, err
	}
	ParseInfoMemory(result, &info)

	// persistence
	result, err = client.GetRedisInfo("persistence")
	if err != nil {
		return info, err
	}
	ParseInfoPersistent(result, &info)

	//stats
	result, err = client.GetRedisInfo("stats")
	if err != nil {
		return info, err
	}
	ParseInfoStats(result, &info)

	//replication
	result, err = client.GetRedisInfo("replication")
	if err != nil {
		return info, err
	}
	ParseInfoReplication(result, &info)

	if ifGetSlaveLag && info.Role == "slave" {
		if port != 0 {
			// skip get repilication lag
			redisCnf := ConfRedis{Addr: GetRedisAddr(info.Master_host, int(info.Master_port)), Database: 0}
			redisClient, err := redisCnf.CreateNewClientRedis()
			if redisClient != nil {
				defer redisClient.Close()
			}
			if err == nil {

				genClient := ClusterAndRedisClient{Redis: redisClient, IsCluster: false}
				sIps := map[string]int{}
				for _, p := range selfIps {
					sIps[p] = port
				}
				oneline, lag, err := genClient.GetSlavesReplicationStatus(sIps)
				if err == nil {
					info.Master_link_status = oneline
					info.SlaveLag = lag
				}

			}
		}

	}

	//keyspace
	result, err = client.GetRedisInfo("keyspace")
	if err != nil {
		return info, err
	}
	//mStr := regexp.MustCompile(`db\d+`)
	ParseInfoKeyspace(result, &info)

	return info, nil
}

func ParseInfoKeyspace(result map[string]string, info *RedisInfoAll) {

	var (
		k      string
		v      string
		keyCnt uint64
		expCnt uint64

		err error
	)
	for k, v = range result {
		if G_regexp_info_keyspace_db.MatchString(k) {
			keyCnt, expCnt, _, err = ParseDBkeyspce(v)
			if err != nil {
				continue
			}
			info.Keys_alldb += keyCnt
			info.Keys_expires += expCnt
		}
	}

}

func CompareRedisVersionStr(a, b string) (int, error) {
	aArr, err := ParseRedisVersion(a)
	if err != nil {
		return -2, err
	}
	bArr, err := ParseRedisVersion(b)
	if err != nil {
		return -2, err
	}
	return CompareRedisVersionInt(aArr, bArr)
}

func ParseRedisVersion(version string) ([]uint8, error) {
	var (
		tmpInt uint64
		err    error
		v      string
		arrInt []uint8
	)
	arrStr := strings.Split(version, ".")
	for _, v = range arrStr {
		tmpInt, err = strconv.ParseUint(v, 10, 8)
		if err != nil {
			return nil, err
		}
		arrInt = append(arrInt, uint8(tmpInt))
	}
	if len(arrInt) == 0 {
		return nil, fmt.Errorf("version %s is not seperated by '.'")
	}
	return arrInt, nil
}

func CompareRedisVersionInt(a, b []uint8) (int, error) {
	if len(a) != len(b) {
		return -2, fmt.Errorf("version %s doesnot have the same format of version %s",
			strings.Join(mystr.Uint8SliceToStringSlice(a), "."),
			strings.Join(mystr.Uint8SliceToStringSlice(b), "."))
	}
	//fmt.Printf("%v %v\n", a, b)
	for i := range a {
		if a[i] > b[i] {
			return 1, nil
		} else if a[i] < b[i] {
			return -1, nil
		}
	}
	return 0, nil
}

func ParseInfoSever(result map[string]string, info *RedisInfoAll) {

	var (
		k       string
		v       string
		tmpUint uint64

		err error
	)

	for k, v = range result {
		switch k {
		//redis_version:3.2.10
		case "redis_version":
			info.Redis_version = v
		case "redis_mode":
			if v == "cluster" {
				info.IsCluster = 1
			} else {
				info.IsCluster = 0
			}
		case "process_id":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Process_id = tmpUint
			}
		}
	}
}

func ParseInfoClient(result map[string]string, info *RedisInfoAll) {

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
			}
		case "client_longest_output_list":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Client_longest_output_list = tmpUint
			}
		case "client_biggest_input_buf":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Client_biggest_input_buf = tmpUint
			}
		case "blocked_clients":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Blocked_clients = tmpUint
			}
		}
	}
}

func ParseInfoMemory(result map[string]string, info *RedisInfoAll) {

	var (
		k        string
		v        string
		tmpUint  uint64
		tmpFloat float64
		err      error
	)

	for k, v = range result {
		switch k {
		case "used_memory":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Used_memory = tmpUint
			}
		case "used_memory_rss":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Used_memory_rss = tmpUint
			}
		case "used_memory_peak":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Used_memory_peak = tmpUint
			}
		case "used_memory_lua":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Used_memory_lua = tmpUint
			}
		case "mem_fragmentation_ratio":
			tmpFloat, err = strconv.ParseFloat(v, 64)
			if err == nil {
				info.Mem_fragmentation_ratio = tmpFloat
			}
		}
	}
}

func ParseInfoPersistent(result map[string]string, info *RedisInfoAll) {

	var (
		k      string
		v      string
		tmpInt int64
		err    error
	)
	for k, v = range result {
		switch k {
		case "loading":
			tmpInt, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				info.LoadingRdb = tmpInt
			}
		case "rdb_bgsave_in_progress":
			tmpInt, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				info.Rdb_bgsave_in_progress = tmpInt
			}
		case "rdb_last_bgsave_status":
			if v == "ok" {
				info.Rdb_last_bgsave_status = 0
			} else {
				info.Rdb_last_bgsave_status = 1
			}
		case "aof_rewrite_in_progress":
			tmpInt, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				info.Aof_rewrite_in_progress = tmpInt
			}
		case "aof_last_bgrewrite_status":
			if v == "ok" {
				info.Aof_last_bgrewrite_status = 0
			} else {
				info.Aof_last_bgrewrite_status = 1
			}
		case "aof_last_write_status":
			if v == "ok" {
				info.Aof_last_write_status = 0
			} else {
				info.Aof_last_write_status = 1
			}
		}
	}

}

func ParseInfoStats(result map[string]string, info *RedisInfoAll) {

	var (
		k        string
		v        string
		tmpUint  uint64
		tmpFloat float64
		err      error
	)
	for k, v = range result {
		switch k {
		case "total_connections_received":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Total_connections_received = tmpUint
			}
		case "total_commands_processed":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Total_commands_processed = tmpUint
			}
		case "instantaneous_ops_per_sec":
			tmpFloat, err = strconv.ParseFloat(v, 64)
			if err == nil {
				info.Instantaneous_ops_per_sec = tmpFloat
			}
		case "total_net_input_bytes":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Total_net_input_bytes = tmpUint
			}
		case "total_net_output_bytes":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Total_net_output_bytes = tmpUint
			}
		case "instantaneous_input_kbps":
			tmpFloat, err = strconv.ParseFloat(v, 64)
			if err == nil {
				info.Instantaneous_input_kbps = tmpFloat
			}
		case "instantaneous_output_kbps":
			tmpFloat, err = strconv.ParseFloat(v, 64)
			if err == nil {
				info.Instantaneous_output_kbps = tmpFloat
			}
		case "rejected_connections":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Rejected_connections = tmpUint
			}
		case "expired_keys":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Expired_keys = tmpUint
			}
		case "evicted_keys":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Evicted_keys = tmpUint
			}
		case "keyspace_hits":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Keyspace_hits = tmpUint
			}
		case "keyspace_misses":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Keyspace_misses = tmpUint
			}
		case "pubsub_channels":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Pubsub_channels = tmpUint
			}
		case "pubsub_patterns":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Pubsub_patterns = tmpUint
			}
		}
	}

}

func ParseInfoReplication(result map[string]string, info *RedisInfoAll) {

	var (
		k       string
		v       string
		tmpUint uint64
		tmpInt  int64
		err     error
	)
	for k, v = range result {
		switch k {
		case "role":
			info.Role = v
		case "connected_slaves":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Connected_slaves = tmpUint
			}
		case "master_host":
			info.Master_host = v
		case "master_port":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Master_port = tmpUint
			}
		case "master_link_status":
			if v == "up" {
				info.Master_link_status = 1
			} else {
				info.Master_link_status = 0
			}
		case "master_sync_in_progress":
			tmpInt, err = strconv.ParseInt(v, 10, 64)
			if err == nil {
				info.Master_sync_in_progress = tmpInt
			}
		}
	}

}

//db0:keys=4,expires=1,avg_ttl=137614
// return: keys, expires, avg_ttl

func ParseDBkeyspce(line string) (uint64, uint64, float64, error) {

	var (
		keyCnt uint64
		expCnt uint64
		avgTTL float64
		err    error
	)
	line = strings.TrimSpace(line)
	if strings.Contains(line, ":") {
		arr := strings.Split(line, ":")
		line = arr[1]
	}
	tmpArr := strings.Split(line, ",")
	for _, v := range tmpArr {
		tArr := strings.Split(v, "=")
		if tArr[0] == "keys" {
			keyCnt, err = strconv.ParseUint(tArr[1], 10, 64)
			if err != nil {
				return keyCnt, expCnt, avgTTL, err
			}
		} else if tArr[0] == "expires" {
			expCnt, err = strconv.ParseUint(tArr[1], 10, 64)
			if err != nil {
				return keyCnt, expCnt, avgTTL, err
			}
		} else if tArr[0] == "avg_ttl" {
			avgTTL, err = strconv.ParseFloat(tArr[1], 64)
			if err != nil {
				return keyCnt, expCnt, avgTTL, err
			}
		}
	}
	return keyCnt, expCnt, avgTTL, nil
}

type RedisReplStatus struct {
	SlaveAddr RedisAddr
	IfOnline  bool
	Offset    int64
	Lag       int64
}

func ParseMasterReplStatus(sts map[string]string) map[string]*RedisReplStatus {
	var (
		result map[string]*RedisReplStatus = map[string]*RedisReplStatus{}
		tmpInt int64
		err    error
	)
	for k, v := range sts {
		if G_regexp_info_replication_slave.MatchString(k) {
			repl := &RedisReplStatus{}
			tmpArr := strings.Split(v, ",")
			for _, tv := range tmpArr {
				arr := strings.Split(tv, "=")
				if len(arr) != 2 {
					continue
				}
				switch arr[0] {
				case "ip":
					repl.SlaveAddr.Host = arr[1]
				case "port":
					tmpInt, err = strconv.ParseInt(arr[1], 10, 32)
					if err == nil {
						repl.SlaveAddr.Port = int(tmpInt)
					} else {
						repl.SlaveAddr.Port = -1
					}
				case "state":
					if arr[1] == "online" {
						repl.IfOnline = true
					} else {
						repl.IfOnline = false
					}
				case "offset":
					tmpInt, err = strconv.ParseInt(arr[1], 10, 64)
					if err == nil {
						repl.Offset = tmpInt
					} else {
						repl.Offset = -1
					}
				case "lag":

					tmpInt, err = strconv.ParseInt(arr[1], 10, 64)
					if err == nil {
						repl.Lag = tmpInt
					} else {
						repl.Lag = -1
					}
				}
			}
			if repl.SlaveAddr.Host != "" && repl.SlaveAddr.Port > 0 {
				result[repl.SlaveAddr.AddrString()] = repl
			}
		}
	}
	return result
}

//slave0:ip=xxxx,port=6683,state=online,offset=2047641,lag=1
// return : online: 1, offline: 0,
//			lag

func (client ClusterAndRedisClient) GetSlavesReplicationStatus(slaveIps map[string]int) (int64, int64, error) {
	var (
		result map[string]string
		err    error
		online int64 = 1
		lag    int64 = 0
		sInfo  SlaveInfo
		//mtch   *regexp.Regexp = regexp.MustCompile(`slave\d+`)
		//found  bool           = false
	)
	result, err = client.GetRedisInfo("replication")
	if err != nil {
		return online, lag, err
	}
	for k, v := range result {
		if G_regexp_info_replication_slave.MatchString(k) {
			sInfo, err = client.GetSlaveReplInfo(v)
			if err != nil {
				continue
			}
			for ip, pt := range slaveIps {
				if sInfo.Addr.Host == ip && sInfo.Addr.Port == pt {
					if sInfo.Replicating {
						online = 1
					} else {
						online = 0
					}
					lag = int64(sInfo.Lag)
					return online, lag, nil
				}
			}
		}
	}
	return online, lag, nil
}

type RedisConfVar struct {
	Maxmemory                     uint64
	Cluster_node_timeout          uint64
	Cluster_require_full_coverage int64 // yes: 1, no: 0
	Slave_serve_stale_data        int64 // yes:1, no:0
	Slave_read_only               int64 // yes:1, no:0
	Stop_writes_on_bgsave_error   int64 // yes:1, no:0
	Maxclients                    uint64
}

func (client ClusterAndRedisClient) GetRedisConfVars(ifErrBreak bool) (RedisConfVar, error) {
	var (
		err     error
		confVar RedisConfVar = RedisConfVar{}

		tmpUint uint64
		tmpStr  string
		//ok      bool
	)

	tmpUint, err = client.GetRedisOneVarUint64("maxmemory")
	if err != nil {
		if ifErrBreak {
			return confVar, err
		}
	} else {
		confVar.Maxmemory = tmpUint
	}

	tmpUint, err = client.GetRedisOneVarUint64("maxclients")
	if err != nil {
		if ifErrBreak {
			return confVar, err
		}
	} else {
		confVar.Maxclients = tmpUint
	}

	tmpUint, err = client.GetRedisOneVarUint64("cluster-node-timeout")
	if err != nil {
		if ifErrBreak {
			return confVar, err
		}
	} else {
		confVar.Cluster_node_timeout = tmpUint
	}

	tmpStr, err = client.GetRedisOneVarString("cluster-require-full-coverage")
	if err != nil {
		if ifErrBreak {
			return confVar, err
		}
	} else {
		//tmpStr = strings.TrimSpace(tmpStr)
		tmpStr = strings.ToLower(tmpStr)
		if tmpStr == "yes" {
			confVar.Cluster_require_full_coverage = 1
		} else {
			confVar.Cluster_require_full_coverage = 0
		}
	}

	tmpStr, err = client.GetRedisOneVarString("slave-serve-stale-data")
	if err != nil {
		if ifErrBreak {
			return confVar, err
		}
	} else {
		tmpStr = strings.ToLower(tmpStr)
		if tmpStr == "yes" {
			confVar.Slave_serve_stale_data = 1
		} else {
			confVar.Slave_serve_stale_data = 0
		}
	}

	tmpStr, err = client.GetRedisOneVarString("slave-read-only")
	if err != nil {
		if ifErrBreak {
			return confVar, err
		}
	} else {
		tmpStr = strings.ToLower(tmpStr)
		if tmpStr == "yes" {
			confVar.Slave_read_only = 1
		} else {
			confVar.Slave_read_only = 0
		}
	}

	tmpStr, err = client.GetRedisOneVarString("stop-writes-on-bgsave-error")
	if err != nil {
		if ifErrBreak {
			return confVar, err
		}
	} else {
		tmpStr = strings.ToLower(tmpStr)
		if tmpStr == "yes" {
			confVar.Stop_writes_on_bgsave_error = 1
		} else {
			confVar.Stop_writes_on_bgsave_error = 0
		}
	}

	return confVar, nil

}

func (client ClusterAndRedisClient) GetRedisOneVarString(name string) (string, error) {
	result, err := client.Redis.ConfigGet(name).Result()
	if err != nil {
		return "", ehand.WithStackError(err)
	}
	if len(result) < 2 {
		return "", ehand.WithStackError(fmt.Errorf("unsupported var %s: %v", name, result))
	}
	tmpStr, ok := result[1].(string)
	if ok {
		return strings.TrimSpace(tmpStr), nil
	} else {
		return "", ehand.WithStackError(fmt.Errorf("fail to convert %v to string", result[1]))
	}

}

func (client ClusterAndRedisClient) GetRedisOneVarUint64(name string) (uint64, error) {
	tmpStr, err := client.GetRedisOneVarString(name)
	if err != nil {
		return 0, err
	}
	tmpUint, err := strconv.ParseUint(tmpStr, 10, 64)
	if err != nil {
		return 0, ehand.WithStackError(fmt.Errorf("error to convert %s to uint64: %s", tmpStr, err))
	}
	return tmpUint, nil

}

func (client ClusterAndRedisClient) GetRedisOneVarInt64(name string) (int64, error) {
	tmpStr, err := client.GetRedisOneVarString(name)
	if err != nil {
		return 0, err
	}
	tmpInt, err := strconv.ParseInt(tmpStr, 10, 64)
	if err != nil {
		return 0, ehand.WithStackError(fmt.Errorf("error to convert %s to int64: %s", tmpStr, err))
	}
	return tmpInt, nil

}

type ClusterInfoAll struct {
	Cluster_state          int64 // OK: 1, 0: not ok
	Cluster_slots_assigned uint64
	Cluster_slots_ok       uint64
	Cluster_slots_pfail    uint64
	Cluster_slots_fail     uint64
	/*
		Cluster_known_nodes             uint64
		Cluster_size                    uint64
		Cluster_current_epoch           uint64
		Cluster_my_epoch                uint64
		Cluster_stats_messages_sent     uint64
		Cluster_stats_messages_received uint64
	*/
}

func (client ClusterAndRedisClient) GetClusterInfoAll() (ClusterInfoAll, error) {
	var (
		err  error
		info ClusterInfoAll = ClusterInfoAll{}

		tmpUint uint64
		result  map[string]string
	)

	result, err = client.GetClusterInfoString()
	//spew.Dump(result)
	if err != nil {
		return info, err
	}
	for k, v := range result {
		switch k {
		case "cluster_state":
			if v == "ok" {
				info.Cluster_state = 1
			} else {
				info.Cluster_state = 0
			}
		case "cluster_slots_assigned":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Cluster_slots_assigned = tmpUint
			}
		case "cluster_slots_ok":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Cluster_slots_ok = tmpUint
			}
		case "cluster_slots_pfail":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Cluster_slots_pfail = tmpUint
			}
		case "cluster_slots_fail":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Cluster_slots_fail = tmpUint
			}
		}
	}

	return info, nil

}

func (client ClusterAndRedisClient) GetClusterInfoString() (map[string]string, error) {
	var (
		err    error
		tmpStr string
		result map[string]string = map[string]string{}
	)

	tmpStr, err = client.Redis.ClusterInfo().Result()
	if err != nil {
		return result, ehand.WithStackError(err)
	}
	tmpStr = strings.TrimSpace(tmpStr)
	if tmpStr == "" {
		return result, ehand.WithStackError(fmt.Errorf("cluster info return empty"))
	}
	arr := strings.Split(tmpStr, "\n")
	//spew.Dump(arr)
	for _, line := range arr {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		tmpArr := strings.Split(line, ":")
		if len(tmpArr) != 2 {
			continue
		}
		//spew.Dump(tmpArr)
		result[tmpArr[0]] = tmpArr[1]
	}

	if len(result) == 0 {
		return result, ehand.WithStackError(fmt.Errorf("cluster info return none of valid result"))
	}
	return result, nil
}

func (client ClusterAndRedisClient) RedisPing() bool {
	var (
		err    error
		result string
	)
	if client.IsCluster {
		result, err = client.Cluster.Ping().Result()
	} else {
		result, err = client.Redis.Ping().Result()
	}
	if err != nil {
		return false
	}
	result = strings.ToUpper(strings.TrimSpace(result))
	if result == "PONG" {
		return true
	} else {
		return false
	}
}

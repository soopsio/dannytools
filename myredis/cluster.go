package myredis

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"dannytools/ehand"

	"github.com/go-redis/redis"
)

type SlotRehash struct {
	Slot      int
	SrcId     string
	SrcAddr   string
	DestId    string
	DestAddr  string
	IsMigrate bool // true: migrate, false: importing

}

type ClusterNodeInfo struct {
	NodeId      string
	Addr        string
	Flags       []string
	Role        string // master|slave
	PingSent    int
	PingRecv    int
	ConfEpoch   int
	LinkStatus  string //connected
	Slots       []int
	RehashSlots []SlotRehash
	MasterId    string
	MasterAddr  string
}

type ConfCluster struct {
	ConfCommon
	Addrs []string // host:port

	ReadOnly bool // if read slave

	MaxRedirects int
}

func (this ConfCluster) setConfCluster(opt *redis.ClusterOptions) {

	this.setConfCommonCluster(opt)

	opt.Addrs = this.Addrs
	opt.ReadOnly = this.ReadOnly

	if this.MaxRedirects == 0 {
		opt.MaxRedirects = 3
	} else {
		opt.MaxRedirects = this.MaxRedirects
	}

}

func (this ConfCluster) CreateNewClientCluster() (*redis.ClusterClient, error) {
	var opt *redis.ClusterOptions = &redis.ClusterOptions{}
	this.setConfCluster(opt)
	client := redis.NewClusterClient(opt)
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

func GetClusterNodesAddr(client *redis.ClusterClient) (map[string]RedisAddr, error) {
	var (
		err     error
		results map[string]RedisAddr = map[string]RedisAddr{}
		line    string
		addReg  *regexp.Regexp = regexp.MustCompile(`^(\d+)`)
		tmpInt  int64
	)

	reStr, err := client.ClusterNodes().Result()
	if err != nil {
		return nil, ehand.CreateErrorWithStack(err)
	}
	reStr = strings.TrimSpace(reStr)
	for _, line = range strings.Split(reStr, "\n") {

		arr := strings.Fields(line)

		tArr := strings.Split(arr[1], ":")
		if len(tArr) < 2 {
			continue
		}
		tStrArr := addReg.FindStringSubmatch(tArr[1])
		if tStrArr == nil || len(tStrArr) <= 0 {
			continue
		}
		tmpInt, err = strconv.ParseInt(tStrArr[0], 10, 32)
		if err != nil {
			return nil, ehand.CreateErrorWithStack(err)
		}
		addr := RedisAddr{Host: tArr[0], Port: int(tmpInt)}
		results[addr.AddrString()] = addr
	}
	return results, nil
}

/*
name addr flags role ping_sent ping_recv config_epoch link_status slots
37328e760f7c5ca1f0430cd59bb6fcae7099e3aa ip:6681 myself,master - 0 0 7 connected 2731-5460
06934f9263df8e6292d2accecef2a6cabc5193a8 ip:6683 slave 9fd094136d8ae049dcf8f8e4ff833015ce9186a9 0 1512612681090 13 connected
2725ea3036cd2948d45d82fb8b192b91805b1fbb ip:8209 myself,master - 0 0 1 connected 6823-8191 [6823->-b73527c7435d8b104709dd644c39b0e06ffce96b]
b73527c7435d8b104709dd644c39b0e06ffce96b ip:8219 myself,master - 0 0 30 connected [6823-<-2725ea3036cd2948d45d82fb8b192b91805b1fbb]

cc6ef7526eb0fda69db23fcfe035d1218e8cca72 ip:7003@17003 slave 2a1ada71bb3072fe746f1d73732cd8b094fb05c6 0 1526287370672 4 connected
c5aa4190b7990f42ef5c0789f0b3405e97e6be99 ip:7001@17001 master - 0 1526287370000 2 connected 5461-10922
*/
// key = host:port
func GetClusterNodesInfo(client *redis.ClusterClient) (map[string]*ClusterNodeInfo, error) {
	reStr, err := client.ClusterNodes().Result()
	if err != nil {
		return nil, ehand.WithStackError(err)
	}
	var (
		ndInfo map[string]*ClusterNodeInfo = map[string]*ClusterNodeInfo{} // key = host:port
		role   string                      = "master"
		addReg *regexp.Regexp              = regexp.MustCompile(`^(\d+)`)
	)

	reStr = strings.TrimSpace(reStr)
	for _, line := range strings.Split(reStr, "\n") {

		arr := strings.Fields(line)
		if arr[3] == "-" {
			role = "master"
		} else {
			role = "slave"

		}
		tArr := strings.Split(arr[1], ":")
		if len(tArr) != 2 {
			continue
		}
		tStrArr := addReg.FindStringSubmatch(tArr[1])
		if tStrArr == nil || len(tStrArr) <= 0 {
			continue
		}
		arr[1] = fmt.Sprintf("%s:%s", tArr[0], tStrArr[0])

		iSent, _ := strconv.Atoi(arr[4])
		iRecv, _ := strconv.Atoi(arr[5])
		iEpoch, _ := strconv.Atoi(arr[6])

		ndInfo[arr[1]] = &ClusterNodeInfo{NodeId: arr[0], Addr: arr[1], Flags: strings.Split(arr[2], ","), PingSent: iSent,
			PingRecv: iRecv, ConfEpoch: iEpoch, Role: role, LinkStatus: arr[7],
		}

		if role == "master" {
			slots, slotsRe := SlotsStrToSeperateInts(arr[8:])
			ndInfo[arr[1]].Slots = slots
			ndInfo[arr[1]].RehashSlots = slotsRe
		} else {
			ndInfo[arr[1]].MasterId = arr[3]
		}
	}

	for addr, trr := range ndInfo {
		if trr.Role == "slave" {
			for a, t := range ndInfo {
				if addr == a {
					continue
				}
				if trr.MasterId == t.NodeId {
					ndInfo[addr].MasterAddr = a
					break
				}
			}
		} else if len(trr.RehashSlots) > 0 {
			for i, sarr := range trr.RehashSlots {
				var (
					sid   string
					sAddr string
				)
				if sarr.IsMigrate {
					sid = sarr.DestAddr
				} else {
					sid = sarr.SrcId
				}
				for a, t := range ndInfo {
					if addr == a {
						continue
					}
					if sid == t.NodeId {
						sAddr = a
						break
					}
				}
				if sarr.IsMigrate {
					ndInfo[addr].RehashSlots[i].DestAddr = sAddr
				} else {
					ndInfo[addr].RehashSlots[i].SrcAddr = sAddr
				}

			}
		}
	}

	return ndInfo, nil

}

func SlotsStrToSeperateInts(slts []string) ([]int, []SlotRehash) {
	var (
		slots   []int
		slotsRe []SlotRehash
	)
	for _, s := range slts {
		if strings.HasPrefix(s, "[") {
			// migrating
			if strings.Contains(s, "->-") {
				arr := strings.Split(s, "->-")
				si, _ := strconv.Atoi(strings.TrimPrefix(arr[0], "["))
				id := strings.TrimSuffix(arr[1], "]")
				slotsRe = append(slotsRe, SlotRehash{Slot: si, DestId: id, IsMigrate: true})
				//importing
			} else if strings.Contains(s, "-<-") {
				arr := strings.Split(s, "-<-")
				si, _ := strconv.Atoi(strings.TrimPrefix(arr[0], "["))
				id := strings.TrimSuffix(arr[1], "]")
				slotsRe = append(slotsRe, SlotRehash{Slot: si, SrcId: id, IsMigrate: false})
			}
		} else if strings.Contains(s, "-") {
			arr := strings.Split(s, "-")
			start, _ := strconv.Atoi(arr[0])
			stop, _ := strconv.Atoi(arr[1])
			for i := start; i <= stop; i++ {
				slots = append(slots, i)
			}
		} else {
			i, _ := strconv.Atoi(s)
			slots = append(slots, i)
		}
	}

	return slots, slotsRe
}

func GetAllSlavesAddr(sarr map[string]*ClusterNodeInfo) []string {
	var slaves []string
	for a, arr := range sarr {
		if arr.Role == "slave" {
			slaves = append(slaves, a)
		}
	}

	return slaves
}

func GetAllMastersAddr(sarr map[string]*ClusterNodeInfo) []string {
	var masters []string
	for a, arr := range sarr {
		if arr.Role == "master" {
			masters = append(masters, a)
		}
	}

	return masters
}

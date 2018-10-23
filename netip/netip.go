package netip

import (
	"dannytools/mycmd"
	"fmt"
	"net"
	"strings"

	"github.com/vishvananda/netlink"
)

type IpInfo struct {
	Addr      string
	MaskBits  int
	IfaceName string
	GateWays  []string
}

func (this IpInfo) String(ifAddHeader bool) string {
	var strArr []string
	if ifAddHeader {
		strArr = append(strArr, GetPrintInfoHeader())
	}
	strArr = append(strArr, fmt.Sprintf("%-10s %-15s %-6d %-15s", this.IfaceName, this.Addr, this.MaskBits, strings.Join(this.GateWays, ",")))
	return strings.Join(strArr, "\n")
}

type IpIface struct {
	Name     string
	Ips      []IpInfo
	GateWays []string
}

func (this IpIface) String(ifAddHeader bool) string {
	var strArr []string
	if ifAddHeader {
		strArr = append(strArr, GetPrintInfoHeader())
	}
	for _, one := range this.Ips {
		strArr = append(strArr, fmt.Sprintf("%-10s %-15s %-6d %-15s", this.Name, one.Addr, one.MaskBits, strings.Join(this.GateWays, ",")))
	}
	if len(this.Ips) > 0 {
		return strings.Join(strArr, "\n")
	} else {
		return ""
	}
}

func GetPrintInfoHeader() string {
	return fmt.Sprintf("%-10s %-15s %-6s %-15s", "interface", "ip", "subnet", "gateway")
}

func GetNetIfacesNames(prefix string, skipLoopback bool) ([]string, error) {
	ifes, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var result []string
	for _, oneIf := range ifes {
		if oneIf.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if (skipLoopback) && (oneIf.Flags&net.FlagLoopback != 0) {
			continue
		}
		if prefix != "" {
			if strings.HasPrefix(oneIf.Name, prefix) {
				result = append(result, oneIf.Name)
			}
		} else {
			result = append(result, oneIf.Name)
		}
	}
	return result, nil
}

func GetNetIfacesAndIps(prefix string, skipLoopback bool) ([]IpIface, error) {
	var (
		err     error
		results []IpIface
		target  bool = false
	)
	ifes, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	for _, oneIf := range ifes {
		if oneIf.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if (skipLoopback) && (oneIf.Flags&net.FlagLoopback != 0) {
			continue
		}
		target = false
		if prefix != "" {
			if strings.HasPrefix(oneIf.Name, prefix) {
				target = true
			}
		} else {
			target = true
		}
		if !target {
			continue
		}
		link, linkErr := netlink.LinkByName(oneIf.Name)
		var gws []string
		if linkErr == nil {
			routes, rerr := netlink.RouteList(link, netlink.FAMILY_V4)
			if rerr == nil {
				for _, oneRt := range routes {
					gw := oneRt.Gw.To4()
					if gw == nil {
						continue
					}
					//fmt.Println(oneIf.Name, defaultGw)
					gws = append(gws, gw.String())
				}
			} else {
				err = rerr
				continue
			}
		} else {
			err = linkErr
			continue
		}
		if len(gws) == 0 {
			continue
		}
		addrs, oneErr := oneIf.Addrs()
		if oneErr != nil {
			err = oneErr
			continue
		}
		oneIpIface := IpIface{Name: oneIf.Name, GateWays: gws}
		for _, oneAdr := range addrs {
			var (
				oneIp   net.IP
				oneMask net.IPMask
			)

			switch v := oneAdr.(type) {
			case *net.IPNet:
				oneIp = v.IP
				oneMask = v.Mask
			case *net.IPAddr:
				oneIp = v.IP
				oneMask = oneIp.DefaultMask()
			default:
				oneIp = nil
			}
			if oneIp == nil || oneIp.IsLoopback() {
				continue
			}
			oneIp = oneIp.To4()
			if oneIp == nil {
				continue // not a ipv4 addr
			}
			maskBitCnt, _ := oneMask.Size()
			oneIpIface.Ips = append(oneIpIface.Ips, IpInfo{Addr: oneIp.String(),
				MaskBits: maskBitCnt, IfaceName: oneIf.Name, GateWays: gws})
		}
		if len(oneIpIface.Ips) > 0 {
			results = append(results, oneIpIface)
		}

	}
	if len(results) == 0 {
		if err == nil {
			err = fmt.Errorf("no ip info got")
		}
		return nil, err

	}
	return results, nil
}

func GetTargetIpInfo(prefix string, skipLoopback bool, myip string, notFoundErr bool) (IpInfo, error) {
	var (
		target IpInfo
		err    error
		ifGot  bool = false
	)
	allIps, err := GetNetIfacesAndIps(prefix, skipLoopback)
	if err != nil {
		return target, err
	}
	for _, oneIf := range allIps {
		for _, oneIp := range oneIf.Ips {
			if oneIp.Addr == myip {
				target = oneIp
				ifGot = true
				break
			}
		}
		if ifGot {
			break
		}
	}
	if ifGot {
		return target, nil
	} else {
		if notFoundErr {
			return target, fmt.Errorf("target ip %s not found", myip)
		} else {
			return target, nil
		}
	}
}

func AddIp(myip string, subNet int, dev string) error {
	link, err := netlink.LinkByName(dev)
	if err != nil {
		return err
	}
	addrStr := fmt.Sprintf("%s/%d", myip, subNet)
	addr, err := netlink.ParseAddr(addrStr)
	if err != nil {
		return err
	}
	err = netlink.AddrAdd(link, addr)
	return err
}

func DelIp(myip string, subNet int, dev string) error {
	link, err := netlink.LinkByName(dev)
	if err != nil {
		return err
	}
	addrStr := fmt.Sprintf("%s/%d", myip, subNet)
	addr, err := netlink.ParseAddr(addrStr)
	if err != nil {
		return err
	}
	err = netlink.AddrDel(link, addr)
	return err
}

func Arping(srcIp string, dev string, gw string) (string, error) {
	cmd := fmt.Sprintf("/usr/sbin/arping -I %s -s %s -c 5 -w 1 %s", dev, srcIp, gw)
	msgOut, msgErr, err := mycmd.ExecCmdTimeOutStringBash(3000, cmd)
	return msgOut + "\n" + msgErr, err
}

func CheckValidIpv4(myip string) bool {
	ipaddr := net.ParseIP(myip)
	if ipaddr == nil {
		return false
	}
	ipv4 := ipaddr.To4()
	if ipv4 == nil {
		return false
	}
	return true
}

func GetIpAndRouterInfoString() (string, error) {
	cmd := "/sbin/ip a; /sbin/ip route show"
	msgOut, msgErr, err := mycmd.ExecCmdTimeOutStringBash(2000, cmd)
	return msgOut + "\n" + msgErr, err
}

func GetAllIpInfoMsg() (string, error) {
	var (
		allIps []IpIface
		msgArr []string
		oneMsg string
		msg    string = ""
		err    error
	)
	allIps, err = GetNetIfacesAndIps("", true)
	if err == nil {
		for _, oneIf := range allIps {
			oneMsg = oneIf.String(false)
			if oneMsg == "" {
				continue
			}
			msgArr = append(msgArr, oneMsg)
		}
		if len(msgArr) > 0 {
			msg = GetPrintInfoHeader()
			msg += "\n" + strings.Join(msgArr, "\n")
			return msg, nil
		} else {
			return "", fmt.Errorf("no ip info found")
		}

	} else {
		return "", err
	}
}

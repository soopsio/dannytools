package myos

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"strings"

	"github.com/toolkits/file"
)

/*
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
  eth0: 1990350    2838    0    0    0     0          0         0   401351    2218    0    0    0     0       0          0
    lo:   26105     286    0    0    0     0          0         0    26105     286    0    0    0     0       0          0
*/
func GetNetIfaces(onlyPrefix []string) ([]string, error) {
	contents, err := ioutil.ReadFile("/proc/net/dev")
	if err != nil {
		return nil, err
	}

	ret := []string{}

	reader := bufio.NewReader(bytes.NewBuffer(contents))
	for {
		lineBytes, err := file.ReadLine(reader)
		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			return nil, err
		}

		line := string(lineBytes)
		idx := strings.Index(line, ":")
		if idx < 0 {
			continue
		}
		eth := strings.TrimSpace(line[0:idx])
		if len(onlyPrefix) > 0 {
			found := false
			for _, prefix := range onlyPrefix {
				if strings.HasPrefix(eth, prefix) {
					found = true
					break
				}
			}

			if !found {
				continue
			}
		}

		ret = append(ret, eth)
	}
	return ret, nil
}

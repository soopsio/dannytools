package plugins

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/open-falcon/falcon-plus/modules/agent/g"
	"github.com/toolkits/file"
)

// key: sys/ntp/60_ntp.py
func ListPlugins(relativePath string) map[string]*Plugin {
	ret := make(map[string]*Plugin)
	if relativePath == "" {
		return ret
	}

	dir := filepath.Join(g.Config().Plugin.Dir, relativePath)

	if !file.IsExist(dir) || file.IsFile(dir) {
		return ret
	}

	fs, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Println("can not list files under", dir)
		return ret
	}

	for _, f := range fs {
		if f.IsDir() {
			continue
		}

		filename := f.Name()
		arr := strings.Split(filename, "_")
		if len(arr) < 2 {
			continue
		}

		// filename should be: $cycle_$xx
		var cycle int
		cycle, err = strconv.Atoi(arr[0])
		if err != nil {
			continue
		}

		fpath := filepath.Join(relativePath, filename)
		plugin := &Plugin{FilePath: fpath, MTime: f.ModTime().Unix(), Cycle: cycle}
		ret[fpath] = plugin
	}

	return ret
}

// key: sys/ntp/60_ntp.py
func MyListPlugins(rootDir, relativePath, suffix string) (map[string]*Plugin, error) {
	ret := make(map[string]*Plugin)
	if relativePath == "" {
		return ret, nil
	}

	dir := filepath.Join(rootDir, relativePath)

	if !file.IsExist(dir) || file.IsFile(dir) {
		return ret, fmt.Errorf("%s not a dir", dir)
	}

	fs, err := ioutil.ReadDir(dir)
	if err != nil {
		return ret, err
	}

	for _, f := range fs {
		if f.IsDir() {
			continue
		}

		filename := f.Name()
		if strings.HasSuffix(filename, suffix) {
			continue
		}
		arr := strings.Split(filename, "_")
		if len(arr) < 2 {
			continue
		}

		// filename should be: $cycle_$xx
		var cycle int
		cycle, err = strconv.Atoi(arr[0])
		if err != nil {
			continue
		}

		fpath := filepath.Join(relativePath, filename)
		plugin := &Plugin{FilePath: fpath, MTime: f.ModTime().Unix(), Cycle: cycle}
		ret[fpath] = plugin
	}

	return ret, nil
}

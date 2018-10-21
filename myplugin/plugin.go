package myplugin

import (
	"fmt"
	"io/ioutil"

	"path/filepath"
	"strconv"
	"strings"

	"github.com/open-falcon/falcon-plus/modules/agent/plugins"
	"github.com/toolkits/file"
)

// key: sys/ntp/60_ntp.py
func MyListPlugins(rootDir, relativePath string) (map[string]*plugins.Plugin, error) {
	ret := make(map[string]*plugins.Plugin)
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
		plugin := &plugins.Plugin{FilePath: fpath, MTime: f.ModTime().Unix(), Cycle: cycle}
		ret[fpath] = plugin
	}

	return ret, nil
}

package myfilepath

import (
	"io/ioutil"
	"os"
)

func GetAllSubDirs(parentDir string) ([]string, error) {
	var (
		name string
		dirs []string
	)
	fs, err := ioutil.ReadDir(parentDir)
	if err != nil {
		return nil, err
	}

	for _, f := range fs {
		if !f.IsDir() {
			continue
		}
		name = f.Name()
		if name == "." || name == ".." {
			continue
		}
		dirs = append(dirs, name)

	}
	return dirs, nil
}

func IsDir(dir string) bool {
	fs, err := os.Stat(dir)
	if err != nil && !os.IsExist(err) {
		return false
	}
	return fs.IsDir()
}

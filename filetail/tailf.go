package filetail

import (
	"dannytools/ehand"
	"dannytools/logging"
	"dannytools/myfalcon"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"github.com/toolkits/file"
	"github.com/toolkits/slice"
)

type FilePattern struct {
	NamePattern      string   // support glob
	PortRegexp       string   // regexp to search port from file name matching NamePattern, first match sub-group as port
	Port             string   // this is prefered to PortPattern
	RegExpFiltersAnd []string //regexp to filter  lines, all regexp must match. it is exclusive with RegExpFiltersOr
	RegExpFiltersOr  []string // regexp to filter  lines, any regexp match. it is exclusive with RegExpFiltersAnd
	RegExpFiltersNot []string // regexp to filter lines, any  regexp match, the line is ignored
}

func (this FilePattern) CheckAndGetFile(maxLines uint16, maxSecsWait int64, ifSendTailErr bool, result map[string]*FileFilter) error {
	var (
		//result    map[string]*FileFilter = map[string]*FileFilter{}
		err       error
		fileArr   []string
		portArr   []string
		port      string
		portReg   *regexp.Regexp
		ifPortReg bool = false
		regAndArr []*regexp.Regexp
		regOrArr  []*regexp.Regexp
		regNotArr []*regexp.Regexp
	)

	if this.NamePattern == "" {
		return fmt.Errorf("NamePattern is empty")
	}

	if len(this.RegExpFiltersAnd) > 0 && len(this.RegExpFiltersOr) > 0 {
		return fmt.Errorf("RegExpFiltersAnd and RegExpFiltersOr is exclusive")
	}

	fileArr, err = filepath.Glob(this.NamePattern)
	if err != nil {
		return err
	}

	for _, oneR := range this.RegExpFiltersAnd {
		oneReg, err := regexp.Compile(oneR)
		if err != nil {
			return err
		} else {
			regAndArr = append(regAndArr, oneReg)
		}
	}

	for _, oneR := range this.RegExpFiltersOr {
		oneReg, err := regexp.Compile(oneR)
		if err != nil {
			return err
		} else {
			regOrArr = append(regOrArr, oneReg)
		}
	}

	for _, oneR := range this.RegExpFiltersNot {
		oneReg, err := regexp.Compile(oneR)
		if err != nil {
			return err
		} else {
			regNotArr = append(regOrArr, oneReg)
		}
	}

	if this.PortRegexp != "" {
		portReg, err = regexp.Compile(this.PortRegexp)
		if err != nil {
			return err
		}
		ifPortReg = true
	}
	for _, oneF := range fileArr {

		if !filepath.IsAbs(oneF) {
			return fmt.Errorf("%s match %s, but it is not absolute path. pls adjust %s to match files of absolute path",
				oneF, this.NamePattern, this.NamePattern)
		}

		port = ""
		if ifPortReg {
			portArr = portReg.FindStringSubmatch(oneF)
			if len(portArr) >= 2 {
				port = portArr[1] // the first submatch
			}
		}
		if port == "" && this.Port != "" {
			port = this.Port
		}

		result[oneF] = &FileFilter{
			RegExpsAnd:     regAndArr,
			RegExpsOr:      regOrArr,
			RegExpsNot:     regNotArr,
			FileFullPath:   oneF,
			Port:           port,
			MaxLinesCount:  maxLines,
			MaxWaitSeconds: maxSecsWait,
			IfSendTailErr:  ifSendTailErr,
		}

	}
	return nil

}

type FileFilter struct {
	RegExpsAnd     []*regexp.Regexp // for RegExpFiltersAnd
	RegExpsOr      []*regexp.Regexp // for RegExpFiltersOr
	RegExpsNot     []*regexp.Regexp // for RegExpFiltersNot
	FileFullPath   string           // file to tail
	Port           string
	MaxLinesCount  uint16 //no more than this lines are send once.
	MaxWaitSeconds int64  // send lines even less than MaxLinesCount when wait after MaxWaitSeconds
	ExitChan       chan struct{}
	IfSendTailErr  bool // also send tail error to redis
}

func (this *FileFilter) CheckIfEqual(dst *FileFilter) bool {
	if this.Port != dst.Port {
		return false
	}

	if len(this.RegExpsAnd) != len(dst.RegExpsAnd) {
		return false
	} else {
		return CheckRegexpArrEqual(this.RegExpsAnd, dst.RegExpsAnd)
	}
	if len(this.RegExpsOr) != len(dst.RegExpsOr) {
		return false
	} else {
		return CheckRegexpArrEqual(this.RegExpsOr, dst.RegExpsOr)
	}
	if len(this.RegExpsNot) != len(dst.RegExpsNot) {
		return false
	} else {
		return CheckRegexpArrEqual(this.RegExpsNot, dst.RegExpsNot)
	}

}

func CheckRegexpArrEqual(src, dst []*regexp.Regexp) bool {
	srcArr := make([]string, len(src))
	for i := range src {
		srcArr[i] = src[i].String()
	}
	dstArr := make([]string, len(dst))
	for i := range dst {
		dstArr[i] = dst[i].String()
	}

	for i := range srcArr {
		if !slice.ContainsString(dstArr, srcArr[i]) {
			return false
		}
	}

	for i := range dstArr {
		if !slice.ContainsString(srcArr, dstArr[i]) {
			return false
		}
	}
	return true
}

func (this *FileFilter) CheckLineIfMatch(line string) bool {

	if line == "" {
		return false
	}
	ifMatch := false
	if len(this.RegExpsAnd) > 0 {
		ifMatch = true
		for _, oneReg := range this.RegExpsAnd {
			if !oneReg.MatchString(line) {
				ifMatch = false
				break
			}
		}

	} else if len(this.RegExpsOr) > 0 {
		ifMatch = false
		for _, oneReg := range this.RegExpsOr {
			if oneReg.MatchString(line) {
				ifMatch = true
				break
			}
		}

	}
	if len(this.RegExpsNot) > 0 {
		for _, oneReg := range this.RegExpsNot {
			if oneReg.MatchString(line) {
				ifMatch = false
				break
			}
		}
	}

	return ifMatch
}

func (this *FileFilter) cleanup(mLogger *logging.MyLog, redisChan chan myfalcon.AlarmInfo) {
	err := recover()
	if err != nil {
		if this.IfSendTailErr {
			redisChan <- myfalcon.AlarmInfo{
				Port:      this.Port,
				Timestamp: time.Now().Unix(),
				Level:     myfalcon.C_AlarmAverage,
				AlarmType: myfalcon.C_AlarmTypeLog,
				Subject:   fmt.Sprintf("日志监控错误退出%s", filepath.Base(this.FileFullPath)),
				Body:      fmt.Sprintf("%s: %s", this.FileFullPath, err),
			}
		}
		mLogger.WriteToLogByFieldsErrorExtramsgExitCode(fmt.Errorf("%s", err), "exit thread to tail file "+this.FileFullPath,
			logging.ERROR, ehand.ERR_ERROR)
	} else {
		mLogger.WriteToLogByFieldsNormalOnlyMsg("exit thread to tail file "+this.FileFullPath, logging.ERROR)
	}
}

func (this *FileFilter) TailOneFileAndSendToRedisCluster(redisChan chan myfalcon.AlarmInfo, mLogger *logging.MyLog, stdoutLogger *logrus.Logger) {

	var (
		line       *tail.Line
		lineArr    []string = make([]string, this.MaxLinesCount)
		alreadyCnt uint16   = 0
		stime      int64
		err        error
		ifAnyMatch bool = false
	)
	// log to stdout, avoid conflict with mLogger
	myTail, err := StartTailfOneFile(this.FileFullPath, stdoutLogger)
	if myTail != nil {
		defer myTail.Cleanup()
		defer myTail.Stop()
	}
	if err != nil {
		mLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "exit gorouting, error to start tailing file "+this.FileFullPath,
			logging.ERROR, ehand.ERR_ERROR)
		return
	}
	defer this.cleanup(mLogger, redisChan)
	mLogger.WriteToLogByFieldsNormalOnlyMsg("start thread to tail file "+this.FileFullPath, logging.WARNING)

	for {
		select {
		case line = <-myTail.Lines:
			if stime == 0 {
				stime = time.Now().Unix()
			}
			if line.Err != nil {
				if this.IfSendTailErr {
					redisChan <- myfalcon.AlarmInfo{
						Port:      this.Port,
						Timestamp: time.Now().Unix(),
						Level:     myfalcon.C_AlarmAverage,
						AlarmType: myfalcon.C_AlarmTypeLog,
						Subject:   fmt.Sprintf("日志监控错误退出%s", filepath.Base(this.FileFullPath)),
						Body:      fmt.Sprintf("%s: %s", this.FileFullPath, line.Err),
					}
				}
				mLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "error to tail "+this.FileFullPath, logging.ERROR, ehand.ERR_ERROR)
				return
			}
			line.Text = strings.TrimSpace(line.Text)
			lineArr[alreadyCnt] = line.Text
			alreadyCnt++
			if this.CheckLineIfMatch(line.Text) {
				ifAnyMatch = true
			}
			if alreadyCnt >= this.MaxLinesCount-1 {
				if ifAnyMatch {
					redisChan <- myfalcon.AlarmInfo{
						Port:      this.Port,
						Timestamp: time.Now().Unix(),
						Level:     myfalcon.C_AlarmAverage,
						AlarmType: myfalcon.C_AlarmTypeLog,
						Subject:   fmt.Sprintf("%s有新的目标日志", filepath.Base(this.FileFullPath)),
						Body:      fmt.Sprintf("%s: %s", this.FileFullPath, lineArr[:alreadyCnt]),
					}
				}
				ifAnyMatch = false
				alreadyCnt = 0
				stime = 0
			}
		case <-this.ExitChan:
			mLogger.WriteToLogByFieldsNormalOnlyMsg("exits tail "+this.FileFullPath, logging.WARNING)
			return
		default:
			if stime > 0 && time.Now().Unix() >= stime+this.MaxWaitSeconds {
				if len(lineArr) > 0 {
					if ifAnyMatch {
						redisChan <- myfalcon.AlarmInfo{
							Port:      this.Port,
							Timestamp: line.Time.Unix(),
							Level:     myfalcon.C_AlarmAverage,
							AlarmType: myfalcon.C_AlarmTypeLog,
							Subject:   fmt.Sprintf("%s有新的目标日志", filepath.Base(this.FileFullPath)),
							Body:      fmt.Sprintf("%s: %s", this.FileFullPath, lineArr[:alreadyCnt]),
						}
					}
				}
				ifAnyMatch = false
				stime = 0
				alreadyCnt = 0
			}
			time.Sleep(2 * time.Second)
		}
	}

}

func StartTailfOneFile(fname string, logger *logrus.Logger) (*tail.Tail, error) {

	if !file.IsFile(fname) {
		return nil, fmt.Errorf("%s is not a file nor exists", fname)
	}
	return tail.TailFile(fname, tail.Config{
		Location:    &tail.SeekInfo{Offset: 0, Whence: os.SEEK_END},
		ReOpen:      true,
		Poll:        true,
		Follow:      true,
		MaxLineSize: 0,
		Logger:      logger,
	})
}

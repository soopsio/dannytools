package dlog

import (
	"fmt"
	"path/filepath"

	"github.com/juju/errors"
	"github.com/toolkits/file"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	//ClogLevelFatal   string = "fatal"
	ClogLevelError   string = "error"
	ClogLevelWarning string = "warning"
	ClogLevelInfo    string = "info"
	ClogLevelDebug   string = "debug"
	CkeyMsg          string = "msg"
	CkeyErrMsg       string = "errmsg"
	CkeyErrCode      string = "errcode"
	CkeyExtMsg       string = "extmsg"
	CkeyStack        string = "stacktrace"
)

func CheckValidLevel(level string) bool {
	if level == ClogLevelError || level == ClogLevelWarning || level == ClogLevelInfo || level == ClogLevelDebug {
		return true
	} else {
		return false
	}
}

func ValidLevelMsg() string {
	return fmt.Sprint("valid log levels are:", ClogLevelError, ClogLevelWarning, ClogLevelInfo, ClogLevelDebug)
}

func GetZapLogLevel(level string) zapcore.Level {
	switch level {
	case ClogLevelError:
		return zap.ErrorLevel
	case ClogLevelWarning:
		return zap.WarnLevel
	case ClogLevelInfo:
		return zap.InfoLevel
	case ClogLevelDebug:
		return zap.DebugLevel
	default:
		return zap.InfoLevel
	}
}

func NewDefaultLogger() (*zap.Logger, error) {
	return NewDefaultDlog().NewLogger(false)
}

func NewDefaultDlog() *Dlog {
	return &Dlog{
		IfJson:        false,
		LogFile:       "",
		Level:         ClogLevelInfo,
		IfRotate:      false,
		DisableCaller: true,
		DisableStack:  true,
	}

}

type Dlog struct {
	IfJson        bool
	LogFile       string
	Level         string // log level
	IfRotate      bool
	MaxSize       int  //MB
	MaxAge        int  //day
	MaxBackups    int  //file number
	DisableCaller bool // donnot log caller
	DisableStack  bool // donnot log stack for erro

	Logger *zap.Logger
}

func (this *Dlog) SetDefaultNotOverwrite() {
	if this.MaxSize <= 10 {
		this.MaxSize = 100
	}

	if this.MaxAge <= 1 {
		this.MaxAge = 1
	}
	if this.MaxBackups <= 3 {
		this.MaxBackups = 3
	}

	if this.Level == "" {
		this.Level = ClogLevelWarning
	}
}

func (this *Dlog) NewLogger(ifSetSelf bool) (*zap.Logger, error) {

	this.SetDefaultNotOverwrite()

	if this.LogFile == "" {
		this.IfRotate = false
	} else if !file.IsExist(filepath.Dir(this.LogFile)) {
		return nil, errors.Errorf("dir of logfile %s not exists", this.LogFile)
	}

	if !CheckValidLevel(this.Level) {
		return nil, errors.Errorf("%s is invalid log level, %s", this.Level, ValidLevelMsg())
	}

	if this.IfRotate {

		w := zapcore.AddSync(&lumberjack.Logger{
			Filename:   this.LogFile,
			MaxSize:    this.MaxSize,
			MaxBackups: this.MaxBackups,
			MaxAge:     this.MaxAge,
			LocalTime:  true,
			Compress:   true,
		})
		var (
			encoder   zapcore.Encoder
			encodeCfg zapcore.EncoderConfig = zap.NewProductionEncoderConfig()
		)
		encodeCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		encodeCfg.EncodeLevel = zapcore.CapitalLevelEncoder

		if this.IfJson {
			encoder = zapcore.NewJSONEncoder(encodeCfg)
		} else {
			encoder = zapcore.NewConsoleEncoder(encodeCfg)
		}
		core := zapcore.NewCore(
			encoder,
			w,
			GetZapLogLevel(this.Level),
		)
		var logOpts []zap.Option
		if !this.DisableCaller {
			logOpts = append(logOpts, zap.AddCaller())
		}
		if !this.DisableStack {
			logOpts = append(logOpts, zap.AddStacktrace(zap.ErrorLevel))
		}

		mylog := zap.New(core, logOpts...)

		if ifSetSelf {
			this.Logger = mylog
		}
		return mylog, nil
	} else {
		cfg := zap.NewProductionConfig()
		cfg.DisableCaller = this.DisableCaller
		cfg.DisableStacktrace = this.DisableStack
		cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
		if !this.IfJson {
			cfg.Encoding = "console"
		}
		cfg.Level.SetLevel(GetZapLogLevel(this.Level))
		if this.LogFile != "" {
			cfg.OutputPaths = []string{this.LogFile}
		}
		mylog, err := cfg.Build()
		if err != nil {
			return nil, errors.Annotate(err, "error to create zap logger")
		} else {
			if ifSetSelf {
				this.Logger = mylog
			}
			return mylog, nil
		}
	}
}

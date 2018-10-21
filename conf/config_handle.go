package conf

import (
	"dannytools/ehand"

	"fmt"

	"github.com/go-errors/errors"
	"github.com/sirupsen/logrus"

	"github.com/spf13/viper"
	kitsFile "github.com/toolkits/file"
)

func ReadAndUnmarshalConfFileViper(vip *viper.Viper, cfg interface{}, cfgFile string, logWr *logrus.Logger) interface{} {
	var msg string
	if !kitsFile.IsFile(cfgFile) {
		msg = fmt.Sprintf("config file %s is not a file nor exists", cfgFile)
		ehand.CheckErrNoExtraMsg(logWr, errors.Errorf(""), logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_FILE_NOT_EXISTS, ehand.NAME_MSG: msg}, true)
	}
	vip.SetConfigFile(cfgFile)
	err := vip.ReadInConfig()
	if err != nil {
		msg = fmt.Sprintf("error to read config file %s: %s", cfgFile, err.Error())
		ehand.CheckErrNoExtraMsg(logWr, errors.Errorf(err.Error()), logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_FILE_READ, ehand.NAME_MSG: msg}, true)
	}
	err = vip.Unmarshal(cfg)
	if err != nil {
		msg = fmt.Sprintf("error to unmarshal config file %s: %s", cfgFile, err)
		ehand.CheckErrNoExtraMsg(logWr, errors.Errorf(err.Error()), logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_VIPER_UNMARSHAL, ehand.NAME_MSG: msg}, true)
	}

	/*
		logging.WriteToLogNoExtraMsg(logWr, logrus.Fields{ehand.NAME_ERRCODE: ehand.ERR_OK,
			ehand.NAME_MSG: fmt.Sprintf("config content: %s", vip.AllSettings())}, logging.DEBUG)
	*/
	return cfg

}

func SetOptionDefaultValues(vip *viper.Viper, defaultVals map[string]interface{}) {
	for ky, val := range defaultVals {
		vip.SetDefault(ky, val)
	}

}

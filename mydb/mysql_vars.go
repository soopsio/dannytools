package mydb

import (
	"bytes"
	"dannytools/ehand"
	"database/sql"
	"fmt"
	"io/ioutil"
	"strconv"

	"strings"

	kitsFile "github.com/toolkits/file"

	_ "github.com/go-sql-driver/mysql"
)

type MysqlVars struct {
	V_read_only             uint8
	V_pid_file              string
	V_super_read_only       uint8
	V_innodb_read_only      uint8
	V_max_connections       int64
	V_max_user_connections  int64
	V_relay_log_space_limit int64
}

func MysqlShowGlobalVars(db *sql.DB) (MysqlVars, error) {
	var (
		err     error
		val     MysqlVars = MysqlVars{}
		varName string
		varVal  sql.RawBytes
	)

	rows, err := db.Query(C_mysql_sql_global_vars)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		return val, ehand.WithStackError(err)
	}

	for rows.Next() {
		err = rows.Scan(&varName, &varVal)
		switch varName {
		case "read_only":
			tmpVal := string(varVal)
			tmpVal = strings.ToUpper(tmpVal)
			if tmpVal == "ON" {
				val.V_read_only = 1
			} else {
				val.V_read_only = 0
			}

		case "pid_file":
			tmpVal := string(varVal)
			val.V_pid_file = tmpVal
		case "super_read_only":
			tmpVal := string(varVal)
			tmpVal = strings.ToUpper(tmpVal)
			if tmpVal == "ON" {
				val.V_super_read_only = 1
			} else {
				val.V_super_read_only = 0
			}
		case "innodb_read_only":
			tmpVal := string(varVal)
			tmpVal = strings.ToUpper(tmpVal)
			if tmpVal == "ON" {
				val.V_innodb_read_only = 1
			} else {
				val.V_innodb_read_only = 0
			}
		case "max_connections":
			val.V_max_connections, _ = BytesToInt64(varVal)
		case "max_user_connections":
			val.V_max_user_connections, _ = BytesToInt64(varVal)
		case "relay_log_space_limit":
			val.V_relay_log_space_limit, _ = BytesToInt64(varVal)

		}
	}

	return val, nil

}

func BytesToInt64(b []byte) (int64, error) {
	s := string(b)
	tmpInt, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return -1, err
	} else {
		return tmpInt, nil
	}
}

func GetPidOfMysql(pidFile string) (int32, error) {
	if pidFile == "" {
		return 0, ehand.WithStackError(fmt.Errorf("parameter pidFile is empty"))
	}
	if !kitsFile.IsFile(pidFile) {
		return 0, ehand.WithStackError(fmt.Errorf("%s is not a file", pidFile))
	}

	b, err := ioutil.ReadFile(pidFile)
	if err != nil {
		return 0, ehand.WithStackError(fmt.Errorf("error to pidfile %s: %s", pidFile, err))
	}
	b = bytes.TrimSpace(b)
	tmpInt, err := strconv.ParseInt(string(b), 10, 32)
	if err != nil {
		return 0, ehand.WithStackError(err)
	} else {
		return int32(tmpInt), nil
	}

}

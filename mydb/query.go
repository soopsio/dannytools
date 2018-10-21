package mydb

import (
	"dannytools/ehand"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

const (
	C_mysql_alive_query          string = "select 1"
	C_mysql_sql_global_status    string = "show global status"
	C_mysql_sql_global_vars      string = "show global variables"
	C_mysql_sql_slave_status     string = "show slave status"
	C_mysql_sql_slave_status_all string = "show all slaves status"
	C_mysql_sql_innodb_status    string = "show engine innodb status"
)

func MergeMapUint64(dst map[string]uint64, src map[string]uint64) map[string]uint64 {
	mg := map[string]uint64{}
	for k, v := range src {
		mg[k] = v
	}
	for k, v := range dst {
		mg[k] = v
	}
	return mg
}

func CheckMysqlAlive(db *sql.DB, querySql string) bool {
	rows, err := db.Query(querySql)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		if CheckIfMysqlAliveError(err) {
			return true
		} else {
			return false
		}
	} else {

		return true
	}
}

func MysqlShowGlobalStatus(db *sql.DB) (map[string]int64, error) {
	var (
		err      error
		sts      map[string]int64 = map[string]int64{}
		varName  string
		varValue int64
	)
	rows, err := db.Query(C_mysql_sql_global_status)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		return sts, ehand.WithStackError(err)
	}

	for rows.Next() {
		err = rows.Scan(&varName, &varValue)
		if err != nil {
			//ignore error, example string value
			continue
		}
		sts[varName] = varValue
	}

	if len(sts) == 0 {
		return sts, ehand.WithStackError(err)
	}
	return sts, nil

}

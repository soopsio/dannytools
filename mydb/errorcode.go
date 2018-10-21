package mydb

import (
	"github.com/VividCortex/mysqlerr"
	"github.com/go-sql-driver/mysql"
)

func GetErrorNumber(err error) (uint16, bool) {
	driverErr, ok := err.(*mysql.MySQLError)
	if ok {
		return driverErr.Number, true
	} else {
		return 0, false
	}
}

func StillAliveErrorNumb(numb uint16) bool {
	ifAlive := false
	switch numb {
	case mysqlerr.ER_ACCESS_DENIED_CHANGE_USER_ERROR,
		mysqlerr.ER_ACCESS_DENIED_ERROR,
		mysqlerr.ER_ACCESS_DENIED_NO_PASSWORD_ERROR,
		mysqlerr.ER_TOO_MANY_USER_CONNECTIONS:
		ifAlive = true
	default:
		ifAlive = false

	}

	return ifAlive
}

func CheckIfMysqlAliveError(err error) bool {
	errNumb, ok := GetErrorNumber(err)
	if !ok {
		return false
	}
	return StillAliveErrorNumb(errNumb)
}

package mydb

import (
	"dannytools/ehand"
	"database/sql"
	//"encoding/binary"
	"fmt"
	//"fmt"
	"strconv"

	"strings"

	_ "github.com/go-sql-driver/mysql"
	//"github.com/davecgh/go-spew/spew"
)

type MySQLSalveStatus struct {
	Is_Slave bool

	Master_Host string
	Master_Port int32

	Master_User     string
	Master_Password string

	Master_Log_File     string
	Read_Master_Log_Pos uint64

	Relay_Log_File string
	Relay_Log_Pos  uint64

	Relay_Master_Log_File string
	Exec_Master_Log_Pos   uint64

	Relay_Log_Space uint64

	Slave_IO_Running  bool
	Slave_SQL_Running bool

	Seconds_Behind_Master uint64
	SQL_Delay             uint64

	Master_UUID        string
	Auto_Position      int32
	Retrieved_Gtid_Set string
	Executed_Gtid_Set  string

	Replicate_Do_DB             string
	Replicate_Ignore_DB         string
	Replicate_Do_Table          string
	Replicate_Ignore_Table      string
	Replicate_Wild_Do_Table     string
	Replicate_Wild_Ignore_Table string

	Replicate_Ignore_Server_Ids string
	Master_Server_Id            int32

	Until_Condition string
	Until_Log_File  string
	Until_Log_Pos   uint64

	Last_Errno               int32
	Last_Error               string
	Last_IO_Errno            int32
	Last_IO_Error            string
	Last_IO_Error_Timestamp  int32
	Last_SQL_Errno           int32
	Last_SQL_Error           string
	Last_SQL_Error_Timestamp int32
}

/*
// DONNOT use
func MysqlShowSlaveStatusRawBytes(db *sql.DB) (MySQLSalveStatus, error) {
	var (
		err error
		i   int
		col string
		sts MySQLSalveStatus = MySQLSalveStatus{}
	)

	rows, err := db.Query(C_mysql_sql_slave_status)
	if err != nil {

		return sts, ehand.WithStackError(err)
	}
	defer rows.Close()

	colsNames, err := rows.Columns()
	if err != nil {

		return sts, ehand.WithStackError(err)
	}

	vals := make([]interface{}, len(colsNames))
	for i, _ = range colsNames {
		vals[i] = new(sql.RawBytes)
	}

	for rows.Next() {
		err = rows.Scan(vals...)
		if err != nil {
			return sts, ehand.WithStackError(err)
		}
		//spew.Dump(vals)

		sts.Is_Slave = true
		//spew.Dump(colsNames)
		for i, col = range colsNames {
			//spew.Dump(col, tmp)
			//tmpVal := *(vals[i])

			tmpVal, ok := vals[i].(*sql.RawBytes)
			if !ok {
				return sts, ehand.WithStackError(fmt.Errorf("error convert %s to *sql.RawBytes", col))
			}
			if len(*tmpVal) == 0 {
				continue
			}
			switch col {
			case "Master_Host":
				sts.Master_Host = string(*tmpVal)
			case "Master_Port":
				//sts.Master_Port = int32(*tmpVal)
				sts.Master_Port = int32(binary.BigEndian.Uint32(*tmpVal))
			case "Master_User":

				sts.Master_User = string(*tmpVal)

			case "Master_Log_File":

				sts.Master_Log_File = string(*tmpVal)

			case "Read_Master_Log_Pos":
				//sts.Read_Master_Log_Pos = uint64(*tmpVal)
				sts.Read_Master_Log_Pos = binary.BigEndian.Uint64(*tmpVal)
			case "Relay_Master_Log_File":

				sts.Relay_Master_Log_File = string(*tmpVal)

			case "Exec_Master_Log_Pos":

				//sts.Exec_Master_Log_Pos = uint64(*tmpVal)
				sts.Exec_Master_Log_Pos = binary.BigEndian.Uint64(*tmpVal)
			case "Relay_Log_File":
				sts.Relay_Log_File = string(*tmpVal)
			case "Relay_Log_Pos":
				//sts.Relay_Log_Pos = uint64(*tmpVal)
				sts.Relay_Log_Pos = binary.BigEndian.Uint64(*tmpVal)
			case "Relay_Log_Space":
				//sts.Relay_Log_Space = uint64(*tmpVal)
				sts.Relay_Log_Space = binary.BigEndian.Uint64(*tmpVal)
			case "Slave_IO_Running":
				v := string(*tmpVal)
				if strings.ToLower(v) == "yes" {
					sts.Slave_IO_Running = true
					sts.Is_Slave = true
				} else {
					sts.Slave_IO_Running = false
				}

			case "Slave_SQL_Running":
				v := string(*tmpVal)
				if strings.ToLower(v) == "yes" {
					sts.Slave_SQL_Running = true
					sts.Is_Slave = true
				} else {
					sts.Slave_SQL_Running = false
				}

			case "Seconds_Behind_Master":
				//sts.Seconds_Behind_Master = uint64(*tmpVal)
				sts.Seconds_Behind_Master = binary.BigEndian.Uint64(*tmpVal)
			case "Master_UUID":
				sts.Master_UUID = string(*tmpVal)

			case "Auto_Position":
				//sts.Auto_Position = int32(*tmpVal)
				sts.Auto_Position = int32(binary.BigEndian.Uint32(*tmpVal))
			case "Retrieved_Gtid_Set":
				sts.Retrieved_Gtid_Set = string(*tmpVal)

			case "Executed_Gtid_Set":
				sts.Executed_Gtid_Set = string(*tmpVal)

			case "Replicate_Do_DB":
				sts.Replicate_Do_DB = string(*tmpVal)

			case "Replicate_Ignore_DB":
				sts.Replicate_Do_DB = string(*tmpVal)

			case "Replicate_Do_Table":
				sts.Replicate_Do_Table = string(*tmpVal)

			case "Replicate_Ignore_Table":
				sts.Replicate_Ignore_Table = string(*tmpVal)

			case "Replicate_Wild_Do_Table":
				sts.Replicate_Wild_Do_Table = string(*tmpVal)

			case "Replicate_Wild_Ignore_Table":
				sts.Replicate_Wild_Ignore_Table = string(*tmpVal)

			case "Replicate_Ignore_Server_Ids":
				sts.Replicate_Ignore_Server_Ids = string(*tmpVal)

			case "Master_Server_Id":
				//sts.Master_Server_Id = int32(*tmpVal)
				sts.Master_Server_Id = int32(binary.BigEndian.Uint32(*tmpVal))
			case "Until_Condition":
				sts.Until_Condition = string(*tmpVal)

			case "Until_Log_File":
				sts.Until_Log_File = string(*tmpVal)

			case "Until_Log_Pos":
				//sts.Until_Log_Pos = uint64(*tmpVal)
				sts.Until_Log_Pos = binary.BigEndian.Uint64(*tmpVal)
			case "Last_Errno":
				//sts.Last_Errno = int32(*tmpVal)
				sts.Last_Errno = int32(binary.BigEndian.Uint32(*tmpVal))
			case "Last_Error":
				sts.Last_Error = string(*tmpVal)

			case "Last_IO_Errno":
				//sts.Last_IO_Errno = int32(*tmpVal)
				sts.Last_IO_Errno = int32(binary.BigEndian.Uint32(*tmpVal))
			case "Last_IO_Error":
				sts.Last_IO_Error = string(*tmpVal)

			case "Last_IO_Error_Timestamp":
				//sts.Last_IO_Error_Timestamp = int32(*tmpVal)
				sts.Last_IO_Error_Timestamp = int32(binary.BigEndian.Uint32(*tmpVal))
			case "Last_SQL_Errno":
				//sts.Last_IO_Errno = int32(*tmpVal)
				sts.Last_IO_Errno = int32(binary.BigEndian.Uint32(*tmpVal))
			case "Last_SQL_Error":
				sts.Last_IO_Error = string(*tmpVal)

			case "Last_SQL_Error_Timestamp":
				//sts.Last_IO_Error_Timestamp = int32(*tmpVal)
				sts.Last_IO_Error_Timestamp = int32(binary.BigEndian.Uint32(*tmpVal))
			}
			*tmpVal = nil
		}
	}
	spew.Dump(sts)
	return sts, nil
}
*/

func MysqlShowSlaveStatusStr(db *sql.DB) (MySQLSalveStatus, error) {
	var (
		err     error
		i       int
		col     string
		sts     MySQLSalveStatus = MySQLSalveStatus{}
		tmpUint uint64
		tmpInt  int64
		//tmpFloat float64
		tmpStr string
		//ok     bool
	)

	rows, err := db.Query(C_mysql_sql_slave_status)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {

		return sts, ehand.WithStackError(err)
	}

	colsNames, err := rows.Columns()
	if err != nil {

		return sts, ehand.WithStackError(err)
	}

	vals := make([]interface{}, len(colsNames))
	for i, _ = range colsNames {
		vals[i] = new(sql.NullString)
	}

	for rows.Next() {
		err = rows.Scan(vals...)
		if err != nil {
			return sts, ehand.WithStackError(err)
		}
		//spew.Dump(vals)
		sts.Is_Slave = true
		//spew.Dump(colsNames)

		for i, col = range colsNames {

			tmpSqlVal, ok := vals[i].(*sql.NullString)
			if !ok {
				return sts, ehand.WithStackError(fmt.Errorf("fail to convert %s to *sql.NullString\n", col))

			}
			if (*tmpSqlVal).Valid {
				tmpStr = (*tmpSqlVal).String
			} else {
				continue
			}
			tmpStr = strings.TrimSpace(tmpStr)
			if tmpStr == "" {
				continue
			}
			//spew.Dump(col, tmpSqlVal, tmpStr)
			//fmt.Printf("%s: %s\n", col, tmpStr)
			switch col {
			case "Master_Host":
				sts.Master_Host = tmpStr
			case "Master_Port":
				tmpUint, err = strconv.ParseUint(tmpStr, 10, 64)
				if err != nil {
					return sts, ehand.WithStackError(err)
				}
				sts.Master_Port = int32(tmpUint)
			case "Master_User":
				sts.Master_User = tmpStr
			case "Master_Log_File":
				sts.Master_Log_File = tmpStr
			case "Read_Master_Log_Pos":
				tmpUint, err = strconv.ParseUint(tmpStr, 10, 64)
				if err != nil {
					return sts, ehand.WithStackError(err)
				}
				sts.Read_Master_Log_Pos = tmpUint

			case "Relay_Master_Log_File":
				sts.Relay_Master_Log_File = tmpStr
			case "Exec_Master_Log_Pos":
				tmpUint, err = strconv.ParseUint(tmpStr, 10, 64)
				if err != nil {
					return sts, ehand.WithStackError(err)
				}
				sts.Exec_Master_Log_Pos = tmpUint
			case "Relay_Log_File":
				sts.Relay_Log_File = tmpStr
			case "Relay_Log_Pos":
				tmpUint, err = strconv.ParseUint(tmpStr, 10, 64)
				if err != nil {
					return sts, ehand.WithStackError(err)
				}
				sts.Relay_Log_Pos = tmpUint
			case "Relay_Log_Space":
				tmpUint, err = strconv.ParseUint(tmpStr, 10, 64)
				if err != nil {
					return sts, ehand.WithStackError(err)
				}
				sts.Relay_Log_Space = tmpUint
			case "Slave_IO_Running":

				if strings.ToLower(tmpStr) == "yes" {
					sts.Slave_IO_Running = true
					sts.Is_Slave = true
				} else {
					sts.Slave_IO_Running = false
				}

			case "Slave_SQL_Running":

				if strings.ToLower(tmpStr) == "yes" {
					sts.Slave_SQL_Running = true
					sts.Is_Slave = true
				} else {
					sts.Slave_SQL_Running = false
				}

			case "Seconds_Behind_Master":
				tmpUint, err = strconv.ParseUint(tmpStr, 10, 64)
				if err != nil {
					return sts, ehand.WithStackError(err)
				}
				sts.Seconds_Behind_Master = tmpUint
			case "SQL_Delay":
				tmpUint, err = strconv.ParseUint(tmpStr, 10, 64)
				if err != nil {
					return sts, ehand.WithStackError(err)
				}
				sts.SQL_Delay = tmpUint
			case "Master_UUID":
				sts.Master_UUID = tmpStr
			case "Auto_Position":
				tmpInt, err = strconv.ParseInt(tmpStr, 10, 64)
				if err != nil {
					return sts, ehand.WithStackError(err)
				}
				sts.Auto_Position = int32(tmpInt)
			case "Retrieved_Gtid_Set":
				sts.Retrieved_Gtid_Set = tmpStr
			case "Executed_Gtid_Set":
				sts.Executed_Gtid_Set = tmpStr
			case "Replicate_Do_DB":
				sts.Replicate_Do_DB = tmpStr
			case "Replicate_Ignore_DB":
				sts.Replicate_Ignore_DB = tmpStr
			case "Replicate_Do_Table":
				sts.Replicate_Do_Table = tmpStr
			case "Replicate_Ignore_Table":
				sts.Replicate_Ignore_Table = tmpStr

			case "Replicate_Wild_Do_Table":
				sts.Replicate_Wild_Do_Table = tmpStr
			case "Replicate_Wild_Ignore_Table":
				sts.Replicate_Wild_Ignore_Table = tmpStr
			case "Replicate_Ignore_Server_Ids":
				sts.Replicate_Ignore_Server_Ids = tmpStr
			case "Master_Server_Id":
				tmpInt, err = strconv.ParseInt(tmpStr, 10, 64)
				if err != nil {
					return sts, ehand.WithStackError(err)
				}
				sts.Master_Server_Id = int32(tmpInt)
			case "Until_Condition":
				sts.Until_Condition = tmpStr
			case "Until_Log_File":
				sts.Until_Log_File = tmpStr
			case "Until_Log_Pos":
				tmpUint, err = strconv.ParseUint(tmpStr, 10, 64)
				if err != nil {
					return sts, ehand.WithStackError(err)
				}
				sts.Until_Log_Pos = tmpUint
			case "Last_Errno":
				tmpInt, err = strconv.ParseInt(tmpStr, 10, 64)
				if err != nil {
					return sts, ehand.WithStackError(err)
				}
				sts.Last_Errno = int32(tmpInt)
			case "Last_Error":
				sts.Last_Error = tmpStr

			case "Last_IO_Errno":
				tmpInt, err = strconv.ParseInt(tmpStr, 10, 64)
				if err != nil {
					return sts, ehand.WithStackError(err)
				}
				sts.Last_IO_Errno = int32(tmpInt)
			case "Last_IO_Error":
				sts.Last_IO_Error = tmpStr
			/*
				case "Last_IO_Error_Timestamp":
					tmpInt, err = strconv.ParseInt(tmpStr, 10, 64)
					if err != nil {
						return sts, ehand.WithStackError(err)
					}
					sts.Last_IO_Error_Timestamp = int32(tmpInt)
			*/
			case "Last_SQL_Errno":
				tmpInt, err = strconv.ParseInt(tmpStr, 10, 64)
				if err != nil {
					return sts, ehand.WithStackError(err)
				}
				sts.Last_SQL_Errno = int32(tmpInt)
			case "Last_SQL_Error":
				sts.Last_IO_Error = tmpStr

				/*
					case "Last_SQL_Error_Timestamp":
						tmpInt, err = strconv.ParseInt(tmpStr, 10, 64)
						if err != nil {
							return sts, ehand.WithStackError(err)
						}
						sts.Last_SQL_Error_Timestamp = int32(tmpInt)

						//default:
						//	fmt.Printf("unwanted col %s\n", col)
				*/
			}

		}

	}

	return sts, nil
}

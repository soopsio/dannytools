package mydb

import (
	"dannytools/ehand"
	"database/sql"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func MysqlShowInnodbStatus(db *sql.DB) (map[string]uint64, error) {
	var (
		err       error
		vals      map[string]uint64 = map[string]uint64{}
		tmpVals   map[string]uint64
		outType   string
		outName   string
		outStatus string
	)

	rows, err := db.Query(C_mysql_sql_innodb_status)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		return vals, ehand.WithStackError(err)
	}

	for rows.Next() {
		err = rows.Scan(&outType, &outName, &outStatus)
		if err != nil {
			return vals, ehand.WithStackError(err)
		}
		outStatus = strings.TrimSpace(outStatus)
		lines := strings.Split(outStatus, "\n")
		for _, line := range lines {
			tmpVals = ParseInnodbStatusSemaphores(line)
			if len(tmpVals) > 0 {
				vals = MergeMapUint64(vals, tmpVals)
				continue
			}

			tmpVals = ParseInnodbStatusTransactions(line)
			if len(tmpVals) > 0 {
				vals = MergeMapUint64(vals, tmpVals)
				continue
			}

			tmpVals = ParseInnodbStatusFileIO(line)
			if len(tmpVals) > 0 {
				vals = MergeMapUint64(vals, tmpVals)
				continue
			}

			tmpVals = ParseInnodbStatusLog(line)
			if len(tmpVals) > 0 {
				vals = MergeMapUint64(vals, tmpVals)
				continue
			}

			tmpVals = ParseInnodbStatusRowOperation(line)
			if len(tmpVals) > 0 {
				vals = MergeMapUint64(vals, tmpVals)
				continue
			}
		}
	}

	return vals, nil

}

/*

	5.5
	OS WAIT ARRAY INFO: reservation count 8855, signal count 51889
	Mutex spin waits 297560, rounds 353563, OS waits 1782
	RW-shared spins 177336, rounds 834699, OS waits 4894
	RW-excl spins 25904, rounds 209353, OS waits 1976
	Spin rounds per wait: 1.19 mutex, 4.71 RW-shared, 8.08 RW-excl

	5.7
	OS WAIT ARRAY INFO: reservation count 943
	OS WAIT ARRAY INFO: signal count 806
	RW-shared spins 0, rounds 1023, OS waits 510
	RW-excl spins 0, rounds 0, OS waits 0
	RW-sx spins 0, rounds 0, OS waits 0
	Spin rounds per wait: 1023.00 RW-shared, 0.00 RW-excl, 0.00 RW-sx

*/

func ParseInnodbStatusSemaphores(line string) map[string]uint64 {
	var (
		vals   map[string]uint64 = map[string]uint64{}
		err    error
		tmpStr string
		tmpInt uint64
	)

	arr := strings.Fields(line)
	if strings.HasPrefix(line, "OS WAIT ARRAY INFO:") {
		if len(arr) > 7 {
			if arr[4] == "reservation" {
				tmpStr = strings.Trim(strings.TrimSpace(arr[6]), ",")
				tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
				if err == nil {

					vals["SemaOsWaitReserve"] = tmpInt
				}
			}
			if arr[4] == "signal" {
				tmpStr = strings.Trim(strings.TrimSpace(arr[9]), ",")
				tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
				if err == nil {
					vals["SemaOsWaitSignal"] = tmpInt
				}
			}
		} else {
			tmpStr = strings.Trim(strings.TrimSpace(arr[6]), ",")
			tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
			if err == nil {
				if arr[4] == "reservation" {
					vals["SemaOsWaitReserve"] = tmpInt
				} else if arr[4] == "signal" {
					vals["SemaOsWaitSignal"] = tmpInt
				}
			}

		}
	} else if strings.HasPrefix(line, "Mutex spin waits") {
		if arr[2] == "waits" {
			tmpStr = strings.Trim(strings.TrimSpace(arr[3]), ",")
			tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
			if err == nil {

				vals["SemaMutexSpinWaits"] = tmpInt
			}
		}
		if arr[4] == "rounds" {
			tmpStr = strings.Trim(strings.TrimSpace(arr[5]), ",")
			tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
			if err == nil {

				vals["SemaMutexRounds"] = tmpInt
			}
		}

		if arr[7] == "waits" {
			tmpStr = strings.Trim(strings.TrimSpace(arr[8]), ",")
			tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
			if err == nil {

				vals["SemaMutexOsWaits"] = tmpInt
			}
		}
	} else if strings.HasPrefix(line, "RW-shared spins") {
		if arr[1] == "spins" {
			tmpStr = strings.Trim(strings.TrimSpace(arr[2]), ",")
			tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
			if err == nil {

				vals["SemaRwshSpins"] = tmpInt
			}
		}
		if arr[3] == "rounds" {
			tmpStr = strings.Trim(strings.TrimSpace(arr[4]), ",")
			tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
			if err == nil {

				vals["SemaRwshRounds"] = tmpInt
			}
		}

		if arr[6] == "waits" {
			tmpStr = strings.Trim(strings.TrimSpace(arr[7]), ",")
			tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
			if err == nil {

				vals["SemaRwshOsWaits"] = tmpInt
			}
		}
	} else if strings.HasPrefix(line, "RW-excl spins") {
		if arr[1] == "spins" {
			tmpStr = strings.Trim(strings.TrimSpace(arr[2]), ",")
			tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
			if err == nil {

				vals["SemaRwexSpins"] = tmpInt
			}
		}
		if arr[3] == "rounds" {
			tmpStr = strings.Trim(strings.TrimSpace(arr[4]), ",")
			tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
			if err == nil {

				vals["SemaRwexRounds"] = tmpInt
			}
		}

		if arr[6] == "waits" {
			tmpStr = strings.Trim(strings.TrimSpace(arr[7]), ",")
			tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
			if err == nil {

				vals["SemaRwexOsWaits"] = tmpInt
			}
		}
	} else if strings.HasPrefix(line, "RW-sx spins") {
		if arr[1] == "spins" {
			tmpStr = strings.Trim(strings.TrimSpace(arr[2]), ",")
			tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
			if err == nil {

				vals["SemaRwsxSpins"] = tmpInt
			}
		}
		if arr[3] == "rounds" {
			tmpStr = strings.Trim(strings.TrimSpace(arr[4]), ",")
			tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
			if err == nil {

				vals["SemaRwsxRounds"] = tmpInt
			}
		}

		if arr[6] == "waits" {
			tmpStr = strings.Trim(strings.TrimSpace(arr[7]), ",")
			tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
			if err == nil {

				vals["SemaRwsxOsWaits"] = tmpInt
			}
		}
	}

	return vals
}

/*
	Trx id counter 3478
	Purge done for trx's n:o < 3478 undo n:o < 0 state: running but idle
	History list length 41
	LIST OF TRANSACTIONS FOR EACH SESSION:
	---TRANSACTION 328424497649488, not started
	0 lock struct(s), heap size 1136, 0 row lock(s)
	---TRANSACTION 328424497658608, not started
*/

func ParseInnodbStatusTransactions(line string) map[string]uint64 {
	var (
		vals   map[string]uint64 = map[string]uint64{}
		err    error
		tmpStr string
		tmpInt uint64
	)

	if strings.HasPrefix(line, "History list length") {
		arr := strings.Fields(line)
		tmpStr = strings.TrimSpace(arr[3])
		tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
		if err == nil {
			vals["TrxHistoryLength"] = tmpInt
		}
	}

	return vals
}

/*
	5.7
	Log sequence number 3539246
	Log flushed up to   3539246
	Pages flushed up to 3539246
	Last checkpoint at  3539237
	0 pending log flushes, 0 pending chkp writes
	1308 log i/o's done, 0.00 log i/o's/second

	5.5

	Log sequence number 48036708312
	Log flushed up to   48036708312
	Last checkpoint at  48036708312
	0 pending log writes, 0 pending chkp writes
	246887 log i/o's done, 0.00 log i/o's/second
*/

func ParseInnodbStatusLog(line string) map[string]uint64 {
	var (
		vals   map[string]uint64 = map[string]uint64{}
		err    error
		tmpStr string
		tmpInt uint64
	)

	if strings.Contains(line, "pending log writes") {
		arr := strings.Fields(line)

		tmpStr = strings.TrimSpace(arr[0])
		tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
		if err == nil {
			vals["LogPendingRedoLogWrite"] = tmpInt
		}

		tmpStr = strings.TrimSpace(arr[4])
		tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
		if err == nil {
			vals["LogPendingRedoLogChkpWrite"] = tmpInt
		}
	}

	return vals
}

/*
	5.5
	Pending normal aio reads: 0 [0, 0, 0, 0] , aio writes: 0 [0, 0, 0, 0] ,
	ibuf aio reads: 0, log i/o's: 0, sync i/o's: 0
	Pending flushes (fsync) log: 0; buffer pool: 0
	11990 OS file reads, 279717 OS file writes, 248633 OS fsyncs

	5.7
	Pending normal aio reads: [0, 0, 0, 0, 0, 0, 0, 0] , aio writes: [0, 0, 0, 0, 0, 0, 0, 0] ,
	ibuf aio reads:, log i/o's:, sync i/o's:
	Pending flushes (fsync) log: 0; buffer pool: 0
	446 OS file reads, 272985 OS file writes, 2974 OS fsyncs
	0.00 reads/s, 0 avg bytes/read, 0.00 writes/s, 0.00 fsyncs/s
*/
func ParseInnodbStatusFileIO(line string) map[string]uint64 {
	var (
		vals   map[string]uint64 = map[string]uint64{}
		err    error
		tmpStr string
		tmpInt uint64
	)
	arr := strings.Fields(line)
	if strings.HasPrefix(line, "Pending flushes (fsync) log:") {
		tmpStr = strings.Trim(strings.TrimSpace(arr[4]), ",")
		tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
		if err == nil {
			vals["IOPendingFsyncLog"] = tmpInt
		}

		tmpStr = strings.Trim(strings.TrimSpace(arr[7]), ",")
		tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
		if err == nil {
			vals["IOPendingFsyncData"] = tmpInt
		}
	} else if strings.Contains(line, "OS file reads") && strings.Contains(line, "OS file writes") {
		tmpStr = strings.TrimSpace(arr[0])
		tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
		if err == nil {
			vals["IOFileReads"] = tmpInt
		}

		tmpStr = strings.TrimSpace(arr[4])
		tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
		if err == nil {
			vals["IOFileWrites"] = tmpInt
		}

		tmpStr = strings.TrimSpace(arr[8])
		tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
		if err == nil {
			vals["IOOSFsync"] = tmpInt
		}
	}

	return vals

}

/*
	5.5
	0 queries inside InnoDB, 0 queries in queue
	1 read views open inside InnoDB
	Main thread process no. 110022, id 47570565850880, state: waiting for server activity
	Number of rows inserted 156484, updated 898309, deleted 77758, read 977788
	0.00 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s

	5.7
	0 queries inside InnoDB, 0 queries in queue
	0 read views open inside InnoDB
	Process ID=25736, Main thread ID=46994410272512, state: sleeping
	Number of rows inserted 1052351, updated 254, deleted 1, read 1841636
	0.00 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
*/

func ParseInnodbStatusRowOperation(line string) map[string]uint64 {
	var (
		vals   map[string]uint64 = map[string]uint64{}
		err    error
		tmpStr string
		tmpInt uint64
	)
	arr := strings.Fields(line)
	if strings.Contains(line, "queries inside InnoDB") {
		tmpStr = strings.TrimSpace(arr[0])
		tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
		if err == nil {
			vals["RowsOpQueriesInInnodb"] = tmpInt
		}

		tmpStr = strings.TrimSpace(arr[4])
		tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
		if err == nil {
			vals["RowsOpQueriesInQueue"] = tmpInt
		}
	} else if strings.Contains(line, "read views open inside InnoDB") {
		tmpStr = strings.TrimSpace(arr[0])
		tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
		if err == nil {
			vals["RowsOpReadViewsInInnodb"] = tmpInt
		}
	} else if strings.HasPrefix(line, "Number of rows inserted") {
		tmpStr = strings.TrimSpace(arr[4])
		tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
		if err == nil {
			vals["RowsOpRowsInserted"] = tmpInt
		}

		tmpStr = strings.TrimSpace(arr[6])
		tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
		if err == nil {
			vals["RowsOpRowsUpdated"] = tmpInt
		}

		tmpStr = strings.TrimSpace(arr[8])
		tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
		if err == nil {
			vals["RowsOpRowsDeleted"] = tmpInt
		}

		tmpStr = strings.TrimSpace(arr[10])
		tmpInt, err = strconv.ParseUint(tmpStr, 10, 64)
		if err == nil {
			vals["RowsOpRowsRead"] = tmpInt
		}
	}

	return vals
}

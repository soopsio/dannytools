package mybinlog

import (
	"io"
)

const (
	C_SERVER_MYSQL   uint8 = 0
	C_SERVER_MARIADB uint8 = 1
)

type BinlogManager struct {
	FileName      string    // binlog file name
	FileDir       string    // binlog dir
	NamePrefix    string    // binlog name prefix , ie mysql-bin.000101, NamePrefix=mysql-bin
	NameNumb      uint32    // ie mysql-bin.000101, NameNumb=101
	BinVersion    string    // binlog version
	ServerVersion string    // mysql/mariadb version
	ServerType    uint8     // 0: mysql, 1:mariadb
	BinFH         io.Reader //
}

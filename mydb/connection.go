package mydb

import (
	"database/sql"
	"fmt"
	"net/url"
	"time"

	"github.com/juju/errors"

	_ "github.com/go-sql-driver/mysql"
)

const (
	CdbTypeMysql   string = "mysql"
	CdbTypeMariadb string = "mariadb"
)

type MysqlAddr struct {
	Host string
	Port int
}

func (this MysqlAddr) StrAddr() string {
	return fmt.Sprintf("%s:%d", this.Host, this.Port)
}

type MysqlConCfg struct {
	Host         string
	Port         int
	Socket       string
	User         string
	Password     string
	DefaultDb    string
	Charset      string
	WriteTimeout int
	ReadTimeout  int
	Timeout      int
	ifParseTime  bool
	ParseTime    bool
	Location     string
	ifAutoCommit bool
	AutoCommit   bool

	MyUrl string
}

func (this *MysqlConCfg) GetAddrStr(sep string) string {
	return fmt.Sprintf("%s%s%d", this.Host, sep, this.Port)
}

func (this *MysqlConCfg) GetAddrStrDefaultSep() string {
	return this.GetAddrStr(":")
}

func (this *MysqlConCfg) CheckNoSocket() error {
	if this.Host == "" || this.Port <= 0 {
		return errors.Errorf("empty or invalid mysql addr")
	}
	if this.User == "" || this.Password == "" {
		return errors.Errorf("user or password is empty")
	}
	if this.Location != "" {
		_, err := time.LoadLocation(this.Location)
		if err != nil {
			return errors.Annotatef(err, "invalid time zone location %s", this.Location)
		}
	}
	return nil
}

func (this *MysqlConCfg) Check() error {
	if (this.Host == "" && this.Port <= 0) || this.Socket == "" {
		return errors.Errorf("empty or invalid mysql addr")
	}
	if this.User == "" || this.Password == "" {
		return errors.Errorf("user or password is empty")
	}
	if this.Location != "" {
		_, err := time.LoadLocation(this.Location)
		if err != nil {
			return errors.Annotatef(err, "invalid time zone location %s", this.Location)
		}
	}
	return nil
}

func (my *MysqlConCfg) SetMyConDefaultConConfOverwrite() {

	my.Host = "127.0.0.1"

	my.Port = 3306

	my.WriteTimeout = 10 // seconds
	my.ReadTimeout = 10  //seconds
	my.Timeout = 5       //seconds
	my.ParseTime = true
	my.Location = "Local"
	my.AutoCommit = true
}

func (my *MysqlConCfg) SetMyConDefaultConConfNotOverwrite() {
	if my.Host == "" {
		my.Host = "127.0.0.1"
	}
	if my.Port == 0 {
		my.Port = 3306
	}
	if my.WriteTimeout == 0 {
		my.WriteTimeout = 10 // seconds
	}

	if my.ReadTimeout == 0 {
		my.ReadTimeout = 10 //seconds
	}
	if my.Timeout == 0 {
		my.Timeout = 5 //seconds
	}
	if !my.ifParseTime {
		my.ParseTime = true
	}

	if my.Location == "" {
		my.Location = "Local"
	}

	if !my.ifAutoCommit {
		my.AutoCommit = true
	}

}

func (my *MysqlConCfg) BuildMysqlUrl() string {
	var urlStr string = fmt.Sprintf("%s:%s", my.User, my.Password)
	if my.Socket != "" {
		urlStr += fmt.Sprintf("@unix(%s)/", my.Socket)
	} else {
		urlStr += fmt.Sprintf("@tcp(%s:%d)/", my.Host, my.Port)
	}
	if my.DefaultDb != "" {
		urlStr += my.DefaultDb
	}
	if my.Charset == "" {
		urlStr += "?charset=utf8mb4,utf8"
	} else {
		urlStr += "?charset=" + my.Charset
	}

	if my.AutoCommit {
		urlStr += "&autocommit=true"
	}

	if my.WriteTimeout > 0 {
		urlStr += fmt.Sprintf("&writeTimeout=%ds", my.WriteTimeout)
	}

	if my.ReadTimeout > 0 {
		urlStr += fmt.Sprintf("&readTimeout=%ds", my.ReadTimeout)
	}

	if my.Timeout > 0 {
		urlStr += fmt.Sprintf("&timeout=%ds", my.Timeout)
	}

	if my.ParseTime {
		urlStr += "&parseTime=true"
	}

	if my.Location != "" {
		urlStr += fmt.Sprintf("&loc=%s", url.QueryEscape(my.Location))
	}
	my.MyUrl = urlStr
	return urlStr

}

/*
even error, db is not nil , you have to close it (db.close)
if db != nil {
	db.Close()
*/
func (my *MysqlConCfg) CreateMysqlCon() (*sql.DB, error) {
	db, err := sql.Open("mysql", my.MyUrl)

	if err != nil {
		if db != nil {
			db.Close()
		}
		return nil, err
	}

	err = db.Ping()

	if err != nil {
		if db != nil {
			db.Close()
		}
		return nil, err
	}

	return db, nil
}

func (my *MysqlConCfg) CreateMysqlConSafe() (*sql.DB, error) {
	db, err := sql.Open("mysql", my.MyUrl)

	if err != nil {
		if db != nil {
			db.Close()
		}
		return nil, err
	}

	err = db.Ping()

	if err != nil {
		if db != nil {
			db.Close()
		}
		return nil, err
	}

	return db, nil
}

func (my *MysqlConCfg) CreateMysqlConMultiTimes(cnt int) (*sql.DB, error) {
	var (
		db  *sql.DB
		err error
	)
	for cnt > 0 {
		db, err = my.CreateMysqlCon()
		if err == nil {
			return db, err
		}
		if db != nil {
			db.Close()
		}
		cnt--
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		if db != nil {
			db.Close()
		}
		return nil, err
	}
	return db, nil
}

func (my *MysqlConCfg) CreateMysqlConMultiTimesInterval(cnt int, interval int) (*sql.DB, error) {
	var (
		db  *sql.DB
		err error
	)
	for cnt > 0 {
		db, err = my.CreateMysqlCon()
		if err == nil {
			return db, err
		}
		if db != nil {
			db.Close()
		}
		cnt--
		time.Sleep(time.Duration(interval) * time.Second)
	}
	if err != nil {
		if db != nil {
			db.Close()
		}
		return nil, err
	}
	return db, nil

}

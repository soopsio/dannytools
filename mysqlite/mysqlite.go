package mysqlite

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

// tout: milliseconds
func CreateConnection(sFile string, tout uint32) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_timeout=%d", sFile, tout))
	if err != nil {
		if db != nil {
			db.Close()
		}
		return nil, err
	}
	//db.Ping()
	return db, nil
}

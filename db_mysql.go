package db_mysql

import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    "os"
    "fmt"
)
    
type db struct {
    m_db *sql.DB
    m_dbname string
    m_DataSourceName string
}

var Mysql db
//= &db{ nil, "mysql", "storm:9vG7WpECbszvFjye@(192.168.1.155:3306)/?charset=utf8" }

func (mydb *db)ExcuteSql(str_sql string) {
	if len(str_sql) <= 0 {
        fmt.Fprintln(os.Stderr, str_sql)
		return
	}
	stmt, err := mydb.m_db.Prepare(str_sql)
	if err != nil {
        fmt.Fprintln(os.Stderr, err, str_sql)
		return
	}
    defer stmt.Close()
	_, err_r := stmt.Exec()
	if err_r != nil {
        fmt.Fprintln(os.Stderr, err_r, str_sql)
		return
	}
	//fmt.Println("Sql:", str_sql, ",Res:", res)
	return
}

func (mydb *db)Init(dbname string, datasource string) {
    mydb.m_dbname = dbname
    mydb.m_DataSourceName = datasource
	var err error
	mydb.m_db, err = sql.Open(mydb.m_dbname, mydb.m_DataSourceName)
	if err != nil {
		fmt.Println("Connect Mysql err:", err)
		return
	} else {
		fmt.Println("Connect Mysql OK!")
	}
	mydb.m_db.SetMaxOpenConns(32)
	mydb.m_db.SetMaxIdleConns(16)
	mydb.m_db.Ping()
}
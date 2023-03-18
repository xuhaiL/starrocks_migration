package utls

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"starrocks_migration/src/clusterConf"
	"strings"
)

var SourceConn *sql.DB
var TargetConn *sql.DB

func RunExecuteSqlNoExit(conn *sql.DB, sqlStat string) (sql.Result, error) {
	result, err := conn.Exec(sqlStat)
	if err != nil {
		Error(fmt.Sprintf("query error, sql: \n %s \n error: %v", sqlStat, err))
		//os.Exit(-2011)
	}

	return result, err
}

func RunExecuteSql(conn *sql.DB, sqlStat string) (sql.Result, error) {
	result, err := conn.Exec(sqlStat)
	if err != nil {
		Error(fmt.Sprintf("query error, sql: \n %s \n error: %v", sqlStat, err))
		os.Exit(-2011)
	}

	return result, err
}

func RunQuerySql(conn *sql.DB, sqlStat string) (rows *sql.Rows, err error) {
	result, err := conn.Query(sqlStat)

	if err != nil {
		Error(fmt.Sprintf("query error, sql: \n %s \n error: %v", sqlStat, err))
		os.Exit(-2010)
	}

	return result, err
}

func InitConnAll() {
	InitConnSource()
	InitConnTarget()
}

func InitConnSource() {
	conf := clusterConf.GYamlConf
	GetConnectionByType(conf, "source")
}

func InitConnTarget() {
	conf := clusterConf.GYamlConf
	GetConnectionByType(conf, "target")
}

func GetConnectionByType(conf *clusterConf.ConfStruct, dbType string) {

	if strings.EqualFold(dbType, "source") {
		SourceConn = GetConnection(conf.SourceDB.Host, conf.SourceDB.QueryPort, conf.SourceDB.User, conf.SourceDB.Password, conf.SourceDB.SourceDatabase)
	} else {
		TargetConn = GetConnection(conf.TargetDB.Host, conf.TargetDB.QueryPort, conf.TargetDB.User, conf.TargetDB.Password, conf.TargetDB.TargetDatabase)
	}

}

func GetConnection(ip string, port string, user string, password string, database string) *sql.DB {

	jdbcUrl := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, ip, port, database)
	//root:123456@tcp(192.168.19.49:3306)/dt_wm_ods
	conn, err := sql.Open("mysql", jdbcUrl)
	if err != nil {
		//infoMess := fmt.Sprintf("ERROR, connection error ip: %s, port: %s, user: %s, password: %s, database: %s",
		infoMess := fmt.Sprintf("ERROR, connection ERROR, url: [%s]", jdbcUrl)
		Log("ERROR", infoMess)
		os.Exit(-1)
	}

	err = conn.Ping()
	if err != nil {
		infoMess := fmt.Sprintf("ERROR, connection ERROR, url: [%s]", jdbcUrl)
		Log("ERROR", infoMess)
		os.Exit(-2)
	}

	return conn
}

func CloseAll() {
	err := SourceConn.Close()
	if err != nil {
		os.Exit(-11)
	}

	err = TargetConn.Close()
	if err != nil {
		os.Exit(-12)
	}
	Log("INFO", "Connection close success")
}

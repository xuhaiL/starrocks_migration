package utls

import (
	"database/sql"
	"fmt"
	"os"
	"starrocks_migration/src/clusterConf"
	"strings"
)

var createTable string = "CREATE TABLE `"
var createTableNotExists string = "CREATE TABLE IF NOT EXISTS `%s`.`"
var splitExternalTableNNotExists string = "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`"
var splitProp string = "PROPERTIES (\n"
var externalProp string = "PROPERTIES (\n\"host\" = \"%s\",\n\"port\" = \"%s\",\n\"user\" = \"%s\",\n\"password\" = \"%s\",\n\"database\" = \"%s\",\n\"table\" = \"%s\",\n"

func TranSchemaExternalTable(conf *clusterConf.ConfStruct, tableName string, oldSchema string) string {

	targetDb := conf.TargetDB
	sourceDB := conf.SourceDB

	ct := fmt.Sprintf(splitExternalTableNNotExists, sourceDB.ExternalDatabase)
	prop := fmt.Sprintf(externalProp, targetDb.Host, targetDb.RpcPort, targetDb.User, targetDb.Password, targetDb.TargetDatabase, tableName)
	ctDDL := strings.Replace(oldSchema, createTable, ct, 1)
	return strings.Replace(ctDDL, splitProp, prop, 1)
}

func TranSchemaNotExists(database string, oldSchema string) string {
	return strings.Replace(oldSchema, createTable, fmt.Sprintf(createTableNotExists, database), 1)
}

func GetTableSchema(tableName string, conn *sql.DB) string {
	var ddl = fmt.Sprintf("SHOW CREATE TABLE %s", tableName)
	rows, err := RunQuerySql(conn, ddl)
	if err != nil {
		errMsg := fmt.Sprintf("Error, sql %s, err: %v", ddl, err)
		Error(errMsg)
		os.Exit(-1011)
	}

	var table, tableSchema string
	for rows.Next() {
		err := rows.Scan(&table, &tableSchema)
		if err != nil {
			Error(fmt.Sprintf("Error rows.getTableSchema error, %v", err))
			os.Exit(-1012)
		}
	}

	return tableSchema
}

func InitExternalDatabase() {
	RunExecuteSql(SourceConn, fmt.Sprintf("create database if not exists %s", clusterConf.GYamlConf.SourceDB.ExternalDatabase))
}

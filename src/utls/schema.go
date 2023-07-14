package utls

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"starrocks_migration/src/clusterConf"
	"strings"
)

var createMaterializedView = "CREATE MATERIALIZED VIEW `"
var createMaterializedViewNotExists = "CREATE MATERIALIZED VIEW IF NOT EXISTS `"

func GetMaterializedViewSchema(tableName string, conn *sql.DB, sourceDatabase string, targetDatabase string) string {
	var ddl = fmt.Sprintf("select TABLE_NAME, MATERIALIZED_VIEW_DEFINITION from information_schema.materialized_views where TABLE_SCHEMA  = '%s' and TABLE_NAME = '%s' ", sourceDatabase, tableName)
	rows, err := RunQuerySql(conn, ddl)
	if err != nil {
		errMsg := fmt.Sprintf("Error, get materialized ViewSchema sql %s, err: %v", ddl, err)
		Error(errMsg)
		os.Exit(-1011)
	}

	var view, ViewSchema string
	for rows.Next() {
		err := rows.Scan(&view, &ViewSchema)
		if err != nil {
			Error(fmt.Sprintf("Error rows.get materialized ViewSchema error, %v", err))
			os.Exit(-1012)
		}
	}

	cv := strings.Replace(ViewSchema, createMaterializedView, createMaterializedViewNotExists, 1)

	// 修改列名
	result := compile.FindStringSubmatch(cv)
	var oldColumns, newColumn string
	if len(result) > 1 {
		oldColumns = result[1]
		fields := strings.Split(oldColumns, ",")
		for i := range fields {
			fields[i] = fmt.Sprintf("`%s`", strings.TrimSpace(fields[i]))
		}
		newColumn = strings.Join(fields, ",")
		cv = strings.ReplaceAll(cv, oldColumns, newColumn)
	}

	return strings.ReplaceAll(cv, sourceDatabase, targetDatabase)

}

var createView = "CREATE VIEW `"
var createViewNotExists = "CREATE VIEW IF NOT EXISTS `"
var reg = "\\((.*?)\\) COMMENT|comment" // 列字段中有括号 (物料)编码
var compile = regexp.MustCompile(reg)

func GetViewSchema(tableName string, conn *sql.DB, sourceDatabase string, targetDatabase string) string {

	var ddl = fmt.Sprintf("SHOW CREATE VIEW %s", tableName)
	rows, err := RunQuerySql(conn, ddl)
	if err != nil {
		errMsg := fmt.Sprintf("Error, getViewSchema sql %s, err: %v", ddl, err)
		Error(errMsg)
		os.Exit(-1011)
	}

	var view, ViewSchema, characterSetClient, collationConnection string
	for rows.Next() {
		err := rows.Scan(&view, &ViewSchema, &characterSetClient, &collationConnection)
		if err != nil {
			Error(fmt.Sprintf("Error rows.getViewSchema error, %v", err))
			os.Exit(-1012)
		}
	}

	cv := strings.Replace(ViewSchema, createView, createViewNotExists, 1)
	// 修改列名
	result := compile.FindStringSubmatch(cv)
	var oldColumns, newColumn string
	if len(result) > 1 {
		oldColumns = result[1]
		fields := strings.Split(oldColumns, ",")
		for i := range fields {
			fields[i] = fmt.Sprintf("`%s`", strings.TrimSpace(fields[i]))
		}
		newColumn = strings.Join(fields, ",")
		cv = strings.ReplaceAll(cv, oldColumns, newColumn)
	}

	return strings.ReplaceAll(cv, sourceDatabase, targetDatabase)

}

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

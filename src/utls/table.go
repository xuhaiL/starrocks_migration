package utls

import (
	"container/list"
	"fmt"
	"os"
)

func GetAllMaterializedView(database string) *list.List {

	result, err := RunQuerySql(SourceConn, fmt.Sprintf("select TABLE_NAME from information_schema.materialized_views where TABLE_SCHEMA  = '%s'", database))
	if err != nil {
		Error("getAllTable error")
	}

	views := list.New()

	for result.Next() {
		var tableName string
		if err := result.Scan(&tableName); err != nil {
			Error("query table error")
			os.Exit(-1001)
		}
		views.PushBack(tableName)
	}

	return views
}

func GetAllView(database string) *list.List {

	result, err := RunQuerySql(SourceConn, fmt.Sprintf("select TABLE_NAME from information_schema.views where TABLE_SCHEMA  = '%s'", database))
	if err != nil {
		Error("getAllTable error")
	}

	views := list.New()

	for result.Next() {
		var tableName string
		if err := result.Scan(&tableName); err != nil {
			Error("query table error")
			os.Exit(-1001)
		}
		views.PushBack(tableName)
	}

	return views
}

func GetAllTable(database string) *list.List {

	result, err := RunQuerySql(SourceConn, fmt.Sprintf("select table_name from information_schema.tables where TABLE_SCHEMA = '%s' and TABLE_TYPE = '%s'", database, "BASE TABLE"))
	if err != nil {
		Error("getAllTable error")
	}

	tables := list.New()

	for result.Next() {
		var tableName string
		if err := result.Scan(&tableName); err != nil {
			Error("query table error")
			os.Exit(-1001)
		}
		tables.PushBack(tableName)
	}

	return tables

}

package utls

import (
	"container/list"
	"fmt"
	"os"
)

func GetAllTable(database string) *list.List {

	result, err := RunQuerySql(SourceConn, fmt.Sprintf("select table_name from information_schema.tables where TABLE_SCHEMA = '%s' and TABLE_TYPE != '%s'", database, "VIEW"))
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

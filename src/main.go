package main

import (
	"container/list"
	"fmt"
	"starrocks_migration/src/clusterConf"
	"starrocks_migration/src/utls"
	"starrocks_migration/src/valid"
	"time"
)

func main() {

	clusterConf.InitConf("resource.yaml")
	conf := clusterConf.GYamlConf
	sourceConf := conf.SourceDB
	targetConf := conf.TargetDB

	valid.ConnectionValid()
	utls.InitExternalDatabase()

	tables := utls.GetAllTable(sourceConf.SourceDatabase)

	insertList := list.New()

	utls.Info(fmt.Sprintf("获取到的表数量 %d", tables.Len()))
	for e := tables.Front(); e != nil; e = e.Next() {
		tableName := fmt.Sprintf("%v", e.Value)
		tableSchema := utls.GetTableSchema(tableName, utls.SourceConn)

		targetDDL := utls.TranSchemaNotExists(targetConf.TargetDatabase, tableSchema)
		utls.RunExecuteSql(utls.TargetConn, targetDDL)

		externalDDL := utls.TranSchemaExternalTable(conf, tableName, tableSchema)
		utls.RunExecuteSql(utls.SourceConn, externalDDL)
		utls.Info(fmt.Sprintf("Table %s schema sync success", tableName))
		queryDML := fmt.Sprintf("insert into %s.%s select * from %s.%s", sourceConf.ExternalDatabase, tableName, sourceConf.SourceDatabase, tableName)
		insertList.PushBack(queryDML)
	}

	utls.Info(fmt.Sprintf("源表数量 %d, 表结构同步完成数量 %d", tables.Len(), insertList.Len()))

	// 为了让 sr 自动同步元数据，所以这里 sleep 一下
	time.Sleep(time.Duration(30) * time.Second)
	index := 0

	for e := insertList.Front(); e != nil; e = e.Next() {
		sql := fmt.Sprintf("%v", e.Value)
		utls.RunExecuteSql(utls.SourceConn, sql)
		index = index + 1
		utls.Info(fmt.Sprintf("总表数据量 %d, 数据已同步完成数 %d", insertList.Len(), index))
	}

	utls.CloseAll()

}

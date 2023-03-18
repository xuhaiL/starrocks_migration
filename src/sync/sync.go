package sync

import (
	"container/list"
	"fmt"
	"starrocks_migration/src/clusterConf"
	"starrocks_migration/src/utls"
)

func MaterializedVIEWSync(conf *clusterConf.ConfStruct) {
	sourceConf := conf.SourceDB
	TargetConf := conf.TargetDB
	views := utls.GetAllMaterializedView(sourceConf.SourceDatabase)

	utls.Info(fmt.Sprintf("获取到的物化视图数量 %d", views.Len()))

	cnt := 0
	for e := views.Front(); e != nil; e = e.Next() {
		viewName := fmt.Sprintf("%v", e.Value)
		viewSchema := utls.GetMaterializedViewSchema(viewName, utls.SourceConn, sourceConf.SourceDatabase, TargetConf.TargetDatabase)
		utls.RunExecuteSql(utls.TargetConn, viewSchema)
		cnt++
		utls.Info(fmt.Sprintf("Materialized view count %d, sync for %d, %s success", views.Len(), cnt, viewName))
	}

	utls.Info(fmt.Sprintf("获取到的物化视图数量 %d, 已同步完成数量 %d", views.Len(), cnt))
}

func VIEWSync(conf *clusterConf.ConfStruct) {
	sourceConf := conf.SourceDB
	TargetConf := conf.TargetDB
	views := utls.GetAllView(sourceConf.SourceDatabase)

	utls.Info(fmt.Sprintf("获取到的视图数量 %d", views.Len()))

	cnt := 0
	for e := views.Front(); e != nil; e = e.Next() {
		viewName := fmt.Sprintf("%v", e.Value)
		viewSchema := utls.GetViewSchema(viewName, utls.SourceConn, sourceConf.SourceDatabase, TargetConf.TargetDatabase)
		utls.RunExecuteSql(utls.TargetConn, viewSchema)
		cnt++
		utls.Info(fmt.Sprintf("view count %d, sync for %d, %s success", views.Len(), cnt, viewName))
	}

	utls.Info(fmt.Sprintf("获取到的视图数量 %d, 已同步完成数量 %d", views.Len(), cnt))
}

func DataSync(insertList *list.List) {
	index := 0
	utls.Info("开始同步数据")
	for e := insertList.Front(); e != nil; e = e.Next() {
		sql := fmt.Sprintf("%v", e.Value)
		utls.RunExecuteSql(utls.SourceConn, sql)
		index = index + 1
		utls.Info(fmt.Sprintf("总表数据量 %d, 数据已同步完成数 %d", insertList.Len(), index))
	}
}

func TableSchemaSync(conf *clusterConf.ConfStruct) *list.List {
	sourceConf := conf.SourceDB
	targetConf := conf.TargetDB

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
	return insertList
}

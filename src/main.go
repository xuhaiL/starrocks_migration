package main

import (
	"starrocks_migration/src/clusterConf"
	"starrocks_migration/src/sync"
	"starrocks_migration/src/utls"
	"starrocks_migration/src/valid"
	"time"
)

/**
sr 跨集群迁移数据
1. 获取原表数据结构
2. 在原表创建迁移库，方便统一迁移
3. 基于原表结构，做目标表结构输出，外部表结构输出
	3.1. 目标表结构输出，无需变更 ddl 语句即可替换
	3.2. 外部表
		3.2.1 CREATE TABLE => CREATE EXTERNAL TABLE
		3.2.2 添加对应的 properties 配置
				"host" = "192.168.12.49",						 -- 目标表sr 地址
				"port" = "9020",								 -- rpc_port 地址
				"user" = "root",								 -- 登录用户名
				"password" = "123456",							 -- 登录密码
				"database" = "aa_1249_db",						 -- 登录数据库
				"table" = "hailong_sync_2_MSATEST2_1000000"		 -- 目标对应表名

4. 创建 insert into back_db.back_table select * from source_table
*/
func main() {

	clusterConf.InitConf("resource.yaml")
	conf := clusterConf.GYamlConf
	valid.ConnectionValid()
	utls.InitExternalDatabase()

	insertList := sync.TableSchemaSync(conf)

	// 为了让 sr 自动同步元数据，所以这里 sleep 一下
	time.Sleep(time.Duration(30) * time.Second)
	sync.DataSync(insertList)

	if conf.ViewSync.VIEW {
		sync.VIEWSync(conf)
	}

	if conf.ViewSync.MATERIALIZED {
		sync.MaterializedVIEWSync(conf)
	}

	utls.CloseAll()

}

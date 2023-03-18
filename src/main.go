package main

import (
	"starrocks_migration/src/clusterConf"
	"starrocks_migration/src/sync"
	"starrocks_migration/src/utls"
	"starrocks_migration/src/valid"
	"time"
)

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

build
```shell
go build  -o starRocksMigrationUtil src\main.go
```

> 使用方法
1. 解压后 chmod +x starRocksMigrationUtil
2. 修改resource.yaml 为对应的集群地址
3. source_database，target_datasource 必须存在，需要手动创建,且名称必须一致
4. rpc_port 需要与 fe.conf 保持一致
5. 注册外部表会自动创建，无需用户从黄健
6. 视图同步可能会有异常情况，直接跳过了
   6.1 base 表已经删除，视图无法正常同步
   6.2 字段名非法，修改下字段名后重新执行

> 思路

sr外部表 https://docs.starrocks.io/zh-cn/2.4/data_source/External_table

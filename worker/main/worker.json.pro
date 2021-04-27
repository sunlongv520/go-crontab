{
  "etcd的集群列表": "配置多个, 避免单点故障",
  "etcdEndpoints": ["http://172.18.137.23:2379"],

  "etcd的连接超时": "单位毫秒",
  "etcdDialTimeout": 5000,

  "mongodb地址": "采用mongodb URI",
  "mongodbUri": "mongodb://crons:ichuntcrons@172.18.137.23:27017/crons?authMechanism=SCRAM-SHA-1",

  "mongodb连接超时时间": "单位毫秒",
  "mongodbConnectTimeout": 5000,

  "日志批次大小": "为了减少mongodb网络往返, 打包成一批写入",
  "jobLogBatchSize": 100,

  "日志自动提交超时": "在批次未达到阀值之前, 超时会自动提交batch",
  "jobLogCommitTimeout": 1000,

  "execCommand": "/bin/bash",

  "etcdUserName":"root",

  "etcdPasswd":"huntmon66499",

  "mongodbDatabases": "crons"
}
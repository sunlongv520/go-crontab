{
  "API接口服务端口": "提供任务增删改查服务",
  "apiPort": 1989,

  "API接口读超时": "单位是毫秒",
  "apiReadTimeout": 5000,

  "API接口写超时": "单位是毫秒",
  "apiWriteTimeout": 5000,

  "etcd的集群列表": "配置多个, 避免单点故障",
  "etcdEndpoints": ["http://172.18.137.23:2379"],

  "etcd的连接超时": "单位毫秒",
  "etcdDialTimeout": 5000,

  "web页面根目录": "静态页面，前后端分离开发",
  "webroot": "./webroot",

  "mongodb地址": "采用mongodb URI",
  "mongodbUri":  "mongodb://crons:ichuntcrons@172.18.137.23:27017/crons?authMechanism=SCRAM-SHA-1",

  "mongodb连接超时时间": "单位毫秒",
  "mongodbConnectTimeout": 5000,

  "etcdUserName":"root",

  "etcdPasswd":"huntmon66499",

  "mongodbDatabases": "crons"
}
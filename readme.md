** master：**

 主要负责接收后台对任务的管理 提供api接口
 
  go run .\master\main\master.go  -config=.\master\main\master.json
 
 
 
** worker：**

 监听任务，执行任务，任务调度
 
 go run .\worker\main\worker.go  -config=./worker\main\worker.json -logDir=./logs/worker
 
 
 
 项目框架解析：
 	供学习爱好者
	![Image text](http://img.ichunt.com/images/ichunt/github/7284e422b8f388375017149554441ec1.jpeg)

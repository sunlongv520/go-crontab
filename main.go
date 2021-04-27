package main

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"time"
)

func main(){
	var (
		config clientv3.Config
		err error
		client *clientv3.Client
		kv clientv3.KV
		getResp *clientv3.GetResponse

	)
	//配置
	config = clientv3.Config{
		Endpoints:[]string{"192.168.2.232:2379"},
		DialTimeout:time.Second*5,
	}
	//连接 床见一个客户端
	if client,err = clientv3.New(config);err != nil{
		fmt.Println(err)
		return
	}


	//用于读写etcd的键值对
	kv = clientv3.NewKV(client)

	//删除key

	//kv.Delete(context.TODO(),"/cron/oncejobs/192.168.2.251/a6ef861b6e298f503310b979990f3ecc",clientv3.WithPrefix())
	//kv.Delete(context.TODO(),"/cron/jobs",clientv3.WithPrefix())
	//kv.Delete(context.TODO(),"/cron/oncejobs",clientv3.WithPrefix())
	//
	//return


	//新增定时任务
	//putResp, err := kv.Put(context.TODO(),"/cron/jobs/192.168.2.232/job2","{\"name\":\"job2\",\"command\":\"D:/phpstudy/PHPTutorial/php/php-5.6.27-nts/php E:/WWW/a.php\",\"cronExpr\":\"*/7 * * * * * *\"}",clientv3.WithPrevKV())
	//putResp, err := kv.Put(context.TODO(),"/cron/jobs/192.168.2.246/job2","{\"name\":\"job2\",\"command\":\" echo hello world\",\"cronExpr\":\"*/5 * * * * * *\"}",clientv3.WithPrevKV())
	//putResp, err := kv.Put(context.TODO(),"/cron/jobs/192.168.2.246/job3","{\"name\":\"job3\",\"command\":\" echo hello boy\",\"cronExpr\":\"*/10 * * * * * *\"}",clientv3.WithPrevKV())
//fmt.Println(putResp)
//fmt.Println(err)

	//新增一次性任务
	//putResp, err := kv.Put(context.TODO(),"/cron/oncejobs/192.168.2.232/job10","{\"name\":\"job10\",\"command\":\" echo hello world  \"}",clientv3.WithPrevKV())


	//强杀任务
	//putResp, err := kv.Put(context.TODO(),"/cron/killer/192.168.2.246/job10","")

	//
	//if err != nil{
	//	fmt.Println(err)
	//}else{
	//	fmt.Println("Revision:",putResp.Header.Revision)
	//	if putResp.PrevKv != nil{
	//		fmt.Println("key:",string(putResp.PrevKv.Key))
	//		fmt.Println("Value:",string(putResp.PrevKv.Value))
	//		fmt.Println("Version:",string(putResp.PrevKv.Version))
	//	}
	//}

	//查询
	getResp,err = kv.Get(context.TODO(),"/cron/workers",clientv3.WithPrefix())
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, kvpair := range getResp.Kvs {
		fmt.Println(kvpair)
	}


	getResp,err = kv.Get(context.TODO(),"/cron/jobs",clientv3.WithPrefix())
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, kvpair := range getResp.Kvs {
		fmt.Println(kvpair)
	}

	getResp,err = kv.Get(context.TODO(),"/cron/oncejobs",clientv3.WithPrefix())
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, kvpair := range getResp.Kvs {
		fmt.Println(kvpair)
	}
}


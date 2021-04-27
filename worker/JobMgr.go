package worker

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/ichunt2019/logger"
	"go-crontab/common"
	"time"
)

// 任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	watcher clientv3.Watcher
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 监听任务变化
func (jobMgr *JobMgr) watchJobs() (err error) {
	var (
		getResp *clientv3.GetResponse
		kvpair *mvccpb.KeyValue
		job *common.Job
		watchStartRevision int64
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobName string
		jobEvent *common.JobEvent
		localIp string
	)


	if localIp, err = getLocalIP(); err != nil {
		return
	}

	//fmt.Println(localIp)

	// 1, get一下/cron/jobs/IP/目录下的所有任务，并且获知当前集群的revision
	if getResp, err = jobMgr.kv.Get(context.TODO(), common.JOB_SAVE_DIR+localIp, clientv3.WithPrefix()); err != nil {
		return
	}

	//fmt.Println(getResp)

	// 当前有哪些任务
	//key:"/cron/jobs/192.168.2.246/job1"
	//create_revision:61
	//mod_revision:61
	//version:1
	//value:"{"name":"","command":" ","cronExpr":"*/5 * * * * * *"}"
	for _, kvpair = range getResp.Kvs {
		// 反序列化json得到Job
		if job, err = common.UnpackJob(kvpair.Value); err == nil {
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
			// 同步给scheduler(调度协程)
			G_scheduler.PushJobEvent(jobEvent)
		}
	}

	// 2, 从该revision向后监听变化事件
	go func() { // 监听协程
		// 从GET时刻的后续版本开始监听变化
		watchStartRevision = getResp.Header.Revision + 1
		// 监听/cron/jobs/目录的后续变化
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR+localIp, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		// 处理监听事件
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: // 任务保存事件 新增或者修改
					logger.Info(fmt.Sprintf("添加或者修改任务 %+v",watchEvent.Kv))
					//反序列化job 推送一个更新事件给scheduler
					if job, err = common.UnpackJob(watchEvent.Kv.Value); err != nil {
						continue
					}
					//fmt.Printf("修改了任务%s",job)
					// 构建一个更新Event
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
				case mvccpb.DELETE: // 任务被删除了
					logger.Info(fmt.Sprintf("删除任务 %+v",watchEvent.Kv))
					//推送一个删除事件给scheduler
					// Delete /cron/jobs/job10
					jobName = common.ExtractJobName(string(watchEvent.Kv.Key))
					jobName = common.TrimIp(localIp,jobName)

					//fmt.Println("删除任务了")
					//fmt.Println(jobName)

					job = &common.Job{Name: jobName}

					// 构建一个删除Event
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)
				}
				// 变化推给scheduler
				//推送到channel里面 管道 jobEventChan<-
				G_scheduler.PushJobEvent(jobEvent)
			}
		}
	}()
	return
}

//监听一次性任务
func(jobMgr *JobMgr) watchOnceJobs(){
	var (
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobEvent *common.JobEvent
		job *common.Job
		localIp string
		err error
	)

	if localIp, err = getLocalIP(); err != nil {
		return
	}

	// 监听/cron/oncejobs/目录
	go func() { // 监听协程
		// 监听/cron/oncejobs/目录的变化
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_ONCE_SAVE_DIR+localIp, clientv3.WithPrefix())
		// 处理监听事件
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: // 新增或者修改任务
					logger.Info(fmt.Sprintf("添加或者修改任务 %+v",watchEvent.Kv))
					//反序列化job 推送一个更新事件给scheduler
					if job, err = common.UnpackJob(watchEvent.Kv.Value); err != nil {
						continue
					}
					//fmt.Println("监听一次性任务")
					//fmt.Println(job)
					// 构建一个更新Event
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
					// 事件推给scheduler
					G_scheduler.PushOnceJobEvent(jobEvent)
				case mvccpb.DELETE: // killer标记过期, 被自动删除
					//一次性任务执行完成后 就不会继续执行 所以此处不需要监听删除任务
					//想要删除任务可以 监听一个强杀任务
				}
			}
		}
	}()
}

// 监听强杀任务通知
func (jobMgr *JobMgr) watchKiller() {
	var (
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobEvent *common.JobEvent
		jobName string
		job *common.Job
		localIp string
		err error
	)

	if localIp, err = getLocalIP(); err != nil {
		return
	}


	// 监听/cron/killer目录
	go func() { // 监听协程
		// 监听/cron/killer/目录的变化
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_KILLER_DIR+localIp, clientv3.WithPrefix())
		// 处理监听事件
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: // 杀死任务事件
					logger.Info(fmt.Sprintf("强杀任务 %+v",watchEvent.Kv))
					jobName = common.ExtractKillerName(string(watchEvent.Kv.Key))
					fmt.Println("监听到了强杀任务")
					fmt.Println(jobName)
					jobName = common.TrimIp(localIp,jobName)
					job = &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL, job)
					// 事件推给scheduler
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: // killer标记过期, 被自动删除
				}
			}
		}
	}()
}

// 初始化管理器
func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		watcher clientv3.Watcher
	)

	// 初始化配置
	config = clientv3.Config{
		Endpoints: G_config.EtcdEndpoints, // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, // 连接超时
		Username:G_config.EtcdUserName,
		Password:G_config.EtcdPasswd,
	}

	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}

	// 得到KV和Lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	// 赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv: kv,
		lease: lease,
		watcher: watcher,
	}

	// 启动任务监听 定时任务
	G_jobMgr.watchJobs()

	//启动一次性任务监听
	G_jobMgr.watchOnceJobs()


	// 启动监听killer 强杀任务
	G_jobMgr.watchKiller()

	return
}

// 创建任务执行锁
func (jobMgr *JobMgr) CreateJobLock(jobName string) (jobLock *JobLock){
	jobLock = InitJobLock(jobName, jobMgr.kv, jobMgr.lease)
	return
}
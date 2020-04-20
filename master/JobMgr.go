package master

import (
	"context"
	"encoding/json"
	"fmt"
	_ "fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go-crontab/common"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"net/http"
	"strconv"
	"time"
)

// 任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	mongoClient *mongo.Client
	jobCollection *mongo.Collection
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 初始化管理器
func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		mongoClient *mongo.Client
		jobCollection *mongo.Collection
	)

	// 初始化配置
	config = clientv3.Config{
		Endpoints: G_config.EtcdEndpoints, // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, // 连接超时
	}

	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}

	// 得到KV和Lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	// 建立mongodb连接
	mongoOptions := options.Client().ApplyURI(G_config.MongodbUri)
	if mongoClient, err = mongo.Connect(context.TODO(),mongoOptions); err != nil {
		return
	}

	// 获取数据库和集合
	jobCollection = mongoClient.Database("ichunt").Collection("cron_jobs")

	// 赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv: kv,
		lease: lease,
		mongoClient: mongoClient,
		jobCollection: jobCollection,
	}
	return
}

// 保存任务
func (jobMgr *JobMgr) SaveJob(req *http.Request) (oldJob *common.Job, err error) {
	// 把任务保存到/cron/jobs/任务名 -> json
	var (
		job common.Job
		jobKey string
		jobValue []byte
		putResp *clientv3.PutResponse
		oldJobObj common.Job
		cronJob common.CronJobs
	)

	// 赋值给job
	job.Name = req.FormValue("job_name");
	job.Command = req.FormValue("command");
	job.CronExpr = req.FormValue("cron_expr");
	nodeIp := req.FormValue("node");

	// 任务类型：1-普通任务，2-一次性任务
	job_type, err := strconv.ParseInt(req.FormValue("job_type"), 10, 64)

	if job_type == 1 {
		jobKey = common.JOB_SAVE_DIR + nodeIp + "/" + job.Name // etcd的保存key: 目录 + IP + 任务名称
	} else {
		jobKey = common.JOB_ONCE_SAVE_DIR + nodeIp + "/" + job.Name // etcd的保存key: 目录 + IP + 任务名称
	}

	// 任务信息json
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}
	// 保存到etcd
	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}
	// 如果是更新, 那么返回旧值
	if putResp.PrevKv != nil {
		// 对旧值做一个反序列化
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}

	// 同步保存到mongo
	cron_job_id := req.FormValue("id")
	cronJob.JobName = req.FormValue("job_name")
	cronJob.EtcdKey = jobKey
	cronJob.Node = req.FormValue("node")
	cronJob.Group, err = strconv.ParseInt(req.FormValue("group"), 10, 64)
	cronJob.Command = req.FormValue("command")
	cronJob.CronExpr = req.FormValue("cron_expr")
	cronJob.ConcurrencyNum, err = strconv.ParseInt(req.FormValue("concurrency_num"), 10, 64)
	cronJob.JobType = job_type
	cronJob.Status = 1

	if cron_job_id == "" { // 新增
		cronJob.Creator, err = strconv.ParseInt(req.FormValue("creator"), 10, 64)
		cronJob.CreateTime = time.Now().Unix()
		cronJob.UpdateTime = time.Now().Unix()

		_, err = jobMgr.jobCollection.InsertOne(context.TODO(), &cronJob)
	} else { // 修改
		modifier := req.FormValue("modifier")

		if modifier != "" {
			cronJob.Modifier, err = strconv.ParseInt(modifier, 10, 64)
		}

		cronJob.UpdateTime = time.Now().Unix()

		// 修改部分值
		update := bson.M{"$set": bson.M{"node": cronJob.Node, "group": cronJob.Group, "command": cronJob.Command,
			"cron_expr": cronJob.CronExpr, "concurrency_num": cronJob.ConcurrencyNum, "update_time": cronJob.UpdateTime}}
		objectId, err := primitive.ObjectIDFromHex(cron_job_id)

		_, err = jobMgr.jobCollection.UpdateOne(context.TODO(), bson.M{"_id": objectId}, update)
		fmt.Println(err)
	}

	if err != nil {
		return
	}

	return
}

// 删除任务
func (jobMgr *JobMgr) DeleteJob(job_name string, node_ip string) (oldJob *common.Job, err error) {
	var (
		jobKey string
		delResp *clientv3.DeleteResponse
		oldJobObj common.Job
	)

	// etcd中保存任务的key
	jobKey = common.JOB_SAVE_DIR + node_ip + "/" + job_name

	// 从etcd中删除它
	if delResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	// 返回被删除的任务信息
	if len(delResp.PrevKvs) != 0 {
		// 解析一下旧值, 返回它
		if err =json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}

	return
}

// 列举任务
func (jobMgr *JobMgr) ListJobs() (jobList []*common.Job, err error) {
	var (
		dirKey string
		getResp *clientv3.GetResponse
		kvPair *mvccpb.KeyValue
		job *common.Job
	)

	// 任务保存的目录
	dirKey = common.JOB_DIR

	// 获取目录下所有任务信息
	if getResp, err = jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	// 初始化数组空间
	jobList = make([]*common.Job, 0)
	// len(jobList) == 0

	// 遍历所有任务, 进行反序列化
	for _, kvPair = range getResp.Kvs {
		job = &common.Job{}
		if err =json.Unmarshal(kvPair.Value, job); err != nil {
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}
	return
}

// 杀死任务
func (jobMgr *JobMgr) KillJob(name string) (err error) {
	// 更新一下key=/cron/killer/任务名
	var (
		killerKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
	)

	// 通知worker杀死对应任务
	killerKey = common.JOB_KILLER_DIR + name

	// 让worker监听到一次put操作, 创建一个租约让其稍后自动过期即可
	if leaseGrantResp, err = jobMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	// 租约ID
	leaseId = leaseGrantResp.ID

	// 设置killer标记
	if _, err = jobMgr.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}
	return
}
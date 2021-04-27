package master

import (
	"go.mongodb.org/mongo-driver/mongo"
	"context"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go-crontab/common"
)

// mongodb日志管理
type LogMgr struct {
	client *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

func InitLogMgr() (err error) {
	var (
		client *mongo.Client
	)

	// 建立mongodb连接
	// 建立mongodb连接

	clientOptions := options.Client().ApplyURI(G_config.MongodbUri)
	if client, err = mongo.Connect(
		context.TODO(),clientOptions); err != nil {
		return
	}

	G_logMgr = &LogMgr{
		client: client,
		logCollection: client.Database(G_config.MondbDatabases).Collection("log"),
	}
	return
}

// 查看任务日志
func (logMgr *LogMgr) ListLog(name string, skip int, limit int) (logArr []*common.JobLog, err error){
	//var (
	//	filter *common.JobLogFilter
	//	logSort *common.SortLogByStartTime
	//	cursor mongo.Cursor
	//	jobLog *common.JobLog
	//)
	//
	//// len(logArr)
	//logArr = make([]*common.JobLog, 0)
	//
	//// 过滤条件
	//filter = &common.JobLogFilter{JobName: name}
	//
	//// 按照任务开始时间倒排
	//logSort = &common.SortLogByStartTime{SortOrder: -1}

	// 查询
	//if cursor, err = logMgr.logCollection.Find(context.TODO(), filter, findopt.Sort(logSort), findopt.Skip(int64(skip)), findopt.Limit(int64(limit))); err != nil {
	//	return
	//}
	// 延迟释放游标
	//defer cursor.Close(context.TODO())
	//
	//for cursor.Next(context.TODO()) {
	//	jobLog = &common.JobLog{}
	//
	//	// 反序列化BSON
	//	if err = cursor.Decode(jobLog); err != nil {
	//		continue // 有日志不合法
	//	}
	//
	//	logArr = append(logArr, jobLog)
	//}
	return
}
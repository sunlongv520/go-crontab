package worker

import (
	"go-crontab/common"
	"time"
	"fmt"
)

// 任务调度
type Scheduler struct {
	jobEventChan chan *common.JobEvent	//  etcd任务事件队列 把定时任务任务放入channel管道
	oncejobEventChan chan *common.JobEvent	//  etcd任务事件队列 把一次性任务放入channel管道
	jobPlanTable map[string]*common.JobSchedulePlan // 任务调度计划表
	jobExecutingTable map[string]*common.JobExecuteInfo // 任务执行表
	jobResultChan chan *common.JobExecuteResult	// 任务结果队列
}

var (
	G_scheduler *Scheduler
)

// 处理任务事件 定时任务
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		jobSchedulePlan *common.JobSchedulePlan
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
		jobExisted bool
		err error
	)
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE:	// 保存任务事件
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case common.JOB_EVENT_DELETE: // 删除任务事件
		if jobSchedulePlan, jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL: // 强杀任务事件
		// 取消掉Command执行, 判断任务是否在执行中
		fmt.Println("在任务执行表中查看是否有改任务进行中,有就取消正在执行的任务")
		if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobEvent.Job.Name]; jobExecuting {
			fmt.Println("找到正在执行的任务了，开始取消任务",jobEvent.Job.Name)
			fmt.Printf("%+v",jobExecuteInfo)
			jobExecuteInfo.CancelFunc()	// 触发command杀死shell子进程, 任务得到退出
		}
	}
}



// 尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {
	// 调度 和 执行 是2件事情
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
	)

	// 执行的任务可能运行很久, 1分钟会调度60次，但是只能执行1次, 防止并发！

	// 如果任务正在执行，跳过本次调度
	if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		// fmt.Println("尚未退出,跳过执行:", jobPlan.Job.Name)
		return
	}

	// 构建执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	// 保存执行状态
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	// 执行任务
	fmt.Println("执行任务:", jobExecuteInfo.Job.Name," 计划执行时间：", jobExecuteInfo.PlanTime.Format("2006-01-02 15:04:05")," 实际执行时间：", jobExecuteInfo.RealTime.Format("2006-01-02 15:04:05"))
	G_executor.ExecuteJob(jobExecuteInfo)
}

// 尝试执行任务  一次性任务
func (scheduler *Scheduler) TryStartOnceJob(job *common.Job) {
	// 调度 和 执行 是2件事情
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
	)

	// 执行的任务可能运行很久, 1分钟会调度60次，但是只能执行1次, 防止并发！

	// 如果任务正在执行，跳过本次调度
	if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[job.Name]; jobExecuting {
		// fmt.Println("尚未退出,跳过执行:", jobPlan.Job.Name)
		return
	}

	// 构建执行状态信息
	jobExecuteInfo = common.BuildOnceJobExecuteInfo(job)

	// 保存执行状态
	scheduler.jobExecutingTable[job.Name] = jobExecuteInfo

	// 执行任务
	fmt.Println("执行一次性任务:", jobExecuteInfo.Job.Name," 计划执行时间：", jobExecuteInfo.PlanTime.Format("2006-01-02 15:04:05")," 实际执行时间：", jobExecuteInfo.RealTime.Format("2006-01-02 15:04:05"))
	G_executor.ExecuteJob(jobExecuteInfo)
}

// 重新计算任务调度状态
//计算出最近要过期的任务  下次要执行的任务 还有多久
//5秒后一个任务要执行
func (scheduler *Scheduler) TrySchedule() (scheduleAfter time.Duration) {
	var (
		jobPlan *common.JobSchedulePlan
		now time.Time
		nearTime *time.Time
	)

	// 如果任务表为空话，随便睡眠多久
	if len(scheduler.jobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}

	// 当前时间
	now = time.Now()

	// 遍历所有任务
	for _, jobPlan = range scheduler.jobPlanTable {
		//到期的任务 尝试执行任务
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			//尝试执行任务：有可能任务到期要执行了 但是上一次任务还没有结束,那么这次就不会执行
			//更新下下次执行时间即可
			scheduler.TryStartJob(jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now) // 更新下次执行时间
		}

		//没有过期的任务
		// 统计最近一个要过期的任务时间
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}
	// 下次调度间隔（最近要执行的任务调度时间 - 当前时间）
	//假如最近一次任务是5秒后执行 那么久睡眠5秒
	scheduleAfter = (*nearTime).Sub(now)
	return
}

// 处理任务结果
func (scheduler *Scheduler) handleJobResult(result *common.JobExecuteResult) {
	var (
		jobLog *common.JobLog
	)
	// 删除执行状态
	delete(scheduler.jobExecutingTable, result.ExecuteInfo.Job.Name)

	// 生成执行日志
	if result.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog = &common.JobLog{
			JobName: result.ExecuteInfo.Job.Name,
			Command: result.ExecuteInfo.Job.Command,
			Output: string(result.Output),
			PlanTime: result.ExecuteInfo.PlanTime.Unix() ,
			ScheduleTime: result.ExecuteInfo.RealTime.Unix() ,
			StartTime: result.StartTime.Unix() ,
			EndTime: result.EndTime.Unix() ,
		}
		if result.Err != nil {
			jobLog.Err = result.Err.Error()
		} else {
			jobLog.Err = ""
		}
		G_logSink.Append(jobLog)
	}

	// fmt.Println("任务执行完成:", result.ExecuteInfo.Job.Name, string(result.Output), result.Err)
}

// 调度协程定时任务
func (scheduler *Scheduler) scheduleLoop() {
	var (
		jobEvent *common.JobEvent
		scheduleAfter time.Duration //下次执行任务时间还有多久
		scheduleTimer *time.Timer
		jobResult *common.JobExecuteResult
	)

	// 初始化一次(1秒) 得到下次调度的时间
	scheduleAfter = scheduler.TrySchedule()

	// 调度的延迟定时器
	scheduleTimer = time.NewTimer(scheduleAfter)

	// 定时任务common.Job
	// 变化事件

	for {
		select {
		case jobEvent = <- scheduler.jobEventChan:	//监听任务变化事件 新增job或者修改job操作会插入到该管道
			// 对内存中维护的任务列表做增删改查 当有事件来了后 就把任务放入内存中
			//当有删除操作 就把该任务从内存中删掉
			scheduler.handleJobEvent(jobEvent)
		case <- scheduleTimer.C:	// 最近的任务到期了
		case jobResult = <- scheduler.jobResultChan: // 监听任务执行结果
			scheduler.handleJobResult(jobResult)
		}
		// 调度一次任务 最近要过期 要执行的任务
		scheduleAfter = scheduler.TrySchedule()
		// 重置调度间隔
		scheduleTimer.Reset(scheduleAfter)
	}
}

//调度一次性执行任务
func (scheduler *Scheduler) onceScheduleLoop(){
	var (
		jobEvent *common.JobEvent
	)
	for {
		select {
		case jobEvent = <- scheduler.oncejobEventChan:	//监听任务变化事件 新增job或者修改job操作会插入到该管道 (一次性执行任务)
			scheduler.TryStartOnceJob(jobEvent.Job)
		}

	}
}

// 推送任务变化事件
//投递一个jobEvent 到jobEventChan 管道里面
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

// 推送任务变化事件  一次性任务
//投递一个jobEvent 到jobEventChan 管道里面
func (scheduler *Scheduler) PushOnceJobEvent(jobEvent *common.JobEvent) {
	scheduler.oncejobEventChan <- jobEvent
}

// 初始化调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan: make(chan *common.JobEvent, 1000),//变化事件 定时任务
		oncejobEventChan: make(chan *common.JobEvent, 1000),//变化事件 一次性执行任务
		jobPlanTable: make(map[string]*common.JobSchedulePlan),//存放着所有要执行的计划任务
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),//任务执行状态
		jobResultChan: make(chan *common.JobExecuteResult, 1000),// 任务执行结果
	}
	// 启动调度协程定时任务
	go G_scheduler.scheduleLoop()
	//启动一次性任务调度协程
	go G_scheduler.onceScheduleLoop()
	return
}

// 回传任务执行结果
func (scheduler *Scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	scheduler.jobResultChan <- jobResult
}
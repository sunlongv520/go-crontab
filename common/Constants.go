package common

const (
	// 定时任务保存目录
	JOB_DIR = "/cron/"

	// 定时任务任务保存目录
	JOB_SAVE_DIR = "/cron/jobs/"

	// 定时任务任务保存目录
	JOB_ONCE_SAVE_DIR = "/cron/oncejobs/"

	// 任务强杀目录
	JOB_KILLER_DIR = "/cron/killer/"

	// 任务锁目录
	JOB_LOCK_DIR = "/cron/lock/"

	// 服务注册目录
	JOB_WORKER_DIR = "/cron/workers/"

	// 保存任务事件
	JOB_EVENT_SAVE = 1

	// 删除任务事件
	JOB_EVENT_DELETE = 2

	// 强杀任务事件
	JOB_EVENT_KILL = 3
)
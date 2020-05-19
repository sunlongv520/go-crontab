package main

import (
	"flag"
	"fmt"
	"go-crontab/worker"
	"runtime"
	"time"
	"github.com/ichunt2019/logger"
)

var (
	confFile string // 配置文件路径
	logDir string // 日志文件路径
)

// 解析命令行参数
func initArgs() {
	// worker -config ./worker.json
	// worker -h
	flag.StringVar(&confFile, "config", "./worker.json", "worker.json")
	flag.StringVar(&logDir, "logDir", "./logs", "日志文件目录")
	flag.Parse()
}

// 初始化线程数量
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var (
		err error
	)

	// 初始化命令行参数
	initArgs()

	logConfig := make(map[string]string)
	logConfig["log_path"] = logDir
	logConfig["log_chan_size"] = "2"
	logger.InitLogger("file",logConfig)
	logger.Init()
	logger.Info("ceshi")



	// 初始化线程
	initEnv()

	// 加载配置
	if err = worker.InitConfig(confFile); err != nil {
		goto ERR
	}

	// 服务注册
	//  key:/cron/workers/192.168.2.246
	if err = worker.InitRegister(); err != nil {
		goto ERR
	}
	//
	//// 启动日志协程
	if err = worker.InitLogSink(); err != nil {
		goto ERR
	}
	//
	//启动执行器
	if err = worker.InitExecutor(); err != nil {
		goto ERR
	}
	//
	//// 启动调度器定时任务
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}
	//
	//// 初始化任务管理器 监听job
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}
	//
	//// 正常退出
	for {
		time.Sleep(1 * time.Second)
	}

	return

ERR:
	fmt.Println(err)
}

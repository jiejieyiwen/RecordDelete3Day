package main

import (
	"Config"
	"StorageMaintainer1/DataManager"
	"StorageMaintainer1/MongoDB"
	"StorageMaintainer1/Redis"
	"StorageMaintainer1/TaskDispatch"
	cron "github.com/robfig/cron"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
	"os"
	"strconv"
)

func init() {
	EnvLoad.GetCmdLineConfig()
}

func main() {
	logger := LoggerModular.GetLogger()

	config := Config.GetConfig()
	if err := Config.ReadConfig(); err != nil {
		logger.Error(err)
		return
	}
	logger.Infof("Config is [%v]", config)

	if len(os.Args) > 1 {
		for index, k := range os.Args {
			if k == "-Data" {
				MongoDB.Date = os.Args[index+1]
			}
		}
	}

	//开启定时器
	crontab := cron.New()
	task := func() {
		ndate, err := strconv.Atoi(MongoDB.Date)
		if err != nil {
			logger.Errorf("日期转换失败: [%v]", err)
			return
		}
		ndate++
		date1 := strconv.Itoa(ndate)
		MongoDB.GetMongoRecordManager().Table = MongoDB.GetMongoRecordManager().Table + date1
		logger.Infof("Mongo Table is: [%v]", MongoDB.GetMongoRecordManager().Table)
	}
	// 添加定时任务
	crontab.AddFunc("0 0 * * *", task)
	// 启动定时器
	crontab.Start()

	//redis
	if err := Redis.Init(); err != nil {
		logger.Error(err)
		return
	}

	//data
	if err := DataManager.GetDataManager().Init(); err != nil {
		logger.Error(err)
		return
	}

	//mongo
	if err := MongoDB.Init(); err != nil {
		logger.Error(err)
		return
	} else {
		TaskDispatch.GetTaskManager().Init()
	}

	a := make(chan bool)
	<-a
}

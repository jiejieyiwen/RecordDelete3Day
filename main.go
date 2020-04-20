package main

import (
	"Config"
	"RecordDelete3Day/DataManager"
	"RecordDelete3Day/MongoDB"
	"RecordDelete3Day/Redis"
	"RecordDelete3Day/TaskDispatch"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
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
	if err := MongoDB.GetMongoRecordManager().Init(); err != nil {
		logger.Error(err)
		return
	} else {
		TaskDispatch.GetTaskManager().Init()
	}

	a := make(chan bool)
	<-a
}

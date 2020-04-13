package main

import (
	"Config"
	"StorageMaintainer1/DataManager"
	"StorageMaintainer1/MongoDB"
	"StorageMaintainer1/Redis"
	"StorageMaintainer1/TaskDispatch"
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
	if err := MongoDB.Init(); err != nil {
		logger.Error(err)
		return
	} else {
		TaskDispatch.GetTaskManager().Init()
	}

	a2 := make(chan bool)
	<-a2
}

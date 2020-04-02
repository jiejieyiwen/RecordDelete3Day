package main

import (
	"StorageMaintainer1/Config"
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

	//conf := EnvLoad.GetConf()
	//if err := conf.InitConfig(); err != nil {
	//	logger.Error(err)
	//	return
	//}

	if err := Config.ReadConfig(); err != nil {
		logger.Error(err)
		return
	}

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

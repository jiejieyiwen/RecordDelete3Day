package main

import (
	"StorageMaintainer1/DataManager"
	"StorageMaintainer1/MongoDB"
	"StorageMaintainer1/Redis"
	"StorageMaintainer1/TaskDispatch"
	"fmt"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
	"runtime"
	"strconv"
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

	//redis
	if err := Redis.Init(); err != nil {
		logger.Error(err)
		return
	}

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
	num := strconv.FormatInt(int64(runtime.NumGoroutine()), 10)
	fmt.Printf("协程数量：%v\n", num)
	a := make(chan bool)
	<-a
	//if err := StorageMaintainerGRpcServer.GetServer().InitServer(); err != nil {
	//	logger.Error(err)
	//	return
	//}
}

func help() {
	fmt.Println("--single-limit   single time deal limit. ")
	fmt.Println("--ntsc-url   	   Network time check URL, eg: http://www.ntsc.ac.cn/  ")
}

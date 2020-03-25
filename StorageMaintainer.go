package main

import (
	"StorageMaintainer1/DataDefine"
	"StorageMaintainer1/DataManager"
	"StorageMaintainer1/MongoDB"
	"StorageMaintainer1/Redis"
	"StorageMaintainer1/TaskDispatch"
	"fmt"
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
	conf := EnvLoad.GetConf()
	if err := conf.InitConfig(); err != nil {
		logger.Error(err)
		return
	}
	if len(os.Args) > 1 {
		for index, k := range os.Args {
			if k == "--single-limit" {
				if a, err := strconv.Atoi(os.Args[index+1]); err == nil {
					DataDefine.SingleDealLimit = a
				}
			} else if k == "--ntsc-url" {
				if len(os.Args[index+1]) == 0 {
					fmt.Println("Please Check NTSC Config. break! ")
					return
				} else {
					DataManager.NTSC_URL = os.Args[index+1]
				}
			} else if k == "-h" {
				help()
			} else if k == "-recovery-time" {
				time, _ := strconv.Atoi(os.Args[index+1])
				MongoDB.RecoveryMongoDBTime = int64(time)
			} else if k == "-SingleDealLimit" {
				DataDefine.SingleDealLimit, _ = strconv.Atoi(os.Args[index+1])
			}
		}
	}
	if err := Redis.Init(); err != nil {
		logger.Error(err)
		return
	}
	if err := DataManager.GetDataManager().Init(); err != nil {
		logger.Error(err)
		return
	}
	if err := MongoDB.Init(); err != nil {
		logger.Error(err)
		return
	} else {
		TaskDispatch.GetTaskManager().Init()
	}
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

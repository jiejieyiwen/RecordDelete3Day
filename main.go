package main

import (
	"Config"
	"RecordDelete3Day/DataManager"
	"RecordDelete3Day/MongoDB"
	"RecordDelete3Day/Redis"
	"RecordDelete3Day/TaskDispatch"
	"fmt"
	"github.com/robfig/cron"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
	"iPublic/RedisModular"
	"net/url"
	"os"
	"strconv"
	"time"
)

func init() {
	EnvLoad.GetCmdLineConfig()
}

var Day int

func main() {
	logger := LoggerModular.GetLogger()

	config := Config.GetConfig()
	if err := Config.ReadConfig(); err != nil {
		logger.Error(err)
		return
	}
	logger.Infof("Config is [%v]", config)

	conf := EnvLoad.GetConf()
	conf.RedisAppName = "imccp-mediacore-media-SearchTask"
	RedisModular.GetBusinessMap().SetBusinessRedis(EnvLoad.PublicName, Config.GetConfig().PublicConfig.RedisURL)
	EnvLoad.GetServiceManager().SetStatus(EnvLoad.ServiceStatusOK)
	go EnvLoad.GetServiceManager().RegSelf()

	DataManager.CurDay = 1
	currentTime := time.Now()
	oldTime := currentTime.AddDate(0, 0, -3)
	MongoDB.Date = oldTime.Format("2006-01-2")
	TaskDispatch.Date2 = oldTime.Format("2006-01-02")

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

	if err := MongoDB.GetMongoRecordManager().Init(); err != nil {
		logger.Error(err)
		return
	}
	//mongo
	TaskDispatch.GetTaskManager().Init()

	c := cron.New()
	_, err := c.AddFunc("00 10 * * *", initDataManager3)
	if err != nil {
		logger.Error(err)
		return
	}

	_, err = c.AddFunc("00 6 * * *", initDataManager7)
	if err != nil {
		logger.Error(err)
		return
	}

	_, err = c.AddFunc("00 15 * * *", initDataManager30)
	if err != nil {
		logger.Error(err)
		return
	}

	_, err = c.AddFunc("00 14 * * *", initDataManager1)
	if err != nil {
		logger.Error(err)
		return
	}

	c.Start()
	defer c.Stop()

	a1 := make(chan bool)
	<-a1
}

func initDataManager1() {
	DataManager.CurDay = 1
	currentTime := time.Now()
	oldTime := currentTime.AddDate(0, 0, -2)
	MongoDB.Date = oldTime.Format("2006-01-2")
	TaskDispatch.Date2 = oldTime.Format("2006-01-02")
	//data
	if err := DataManager.GetDataManager().Init(); err != nil {
		return
	}
	//mongo
	TaskDispatch.GetTaskManager().Init()
}

func initDataManager3() {
	DataManager.CurDay = 3
	currentTime := time.Now()
	oldTime := currentTime.AddDate(0, 0, -4)
	MongoDB.Date = oldTime.Format("2006-01-2")
	TaskDispatch.Date2 = oldTime.Format("2006-01-02")
	//data
	if err := DataManager.GetDataManager().Init(); err != nil {
		return
	}
	//mongo
	TaskDispatch.GetTaskManager().Init()
}

func initDataManager7() {
	DataManager.CurDay = 7
	currentTime := time.Now()
	oldTime := currentTime.AddDate(0, 0, -8)
	MongoDB.Date = oldTime.Format("2006-01-2")
	TaskDispatch.Date2 = oldTime.Format("2006-01-02")
	//data
	if err := DataManager.GetDataManager().Init(); err != nil {
		return
	}
	//mongo
	TaskDispatch.GetTaskManager().Init()
}

func initDataManager30() {
	DataManager.CurDay = 30
	currentTime := time.Now()
	oldTime := currentTime.AddDate(0, 0, -31)
	MongoDB.Date = oldTime.Format("2006-01-2")
	TaskDispatch.Date2 = oldTime.Format("2006-01-02")
	//data
	if err := DataManager.GetDataManager().Init(); err != nil {
		return
	}
	//mongo
	TaskDispatch.GetTaskManager().Init()
}

func main22() {
	logger := LoggerModular.GetLogger()

	config := Config.GetConfig()
	if err := Config.ReadConfig(); err != nil {
		logger.Error(err)
		return
	}
	logger.Infof("Config is [%v]", config)

	if len(os.Args) > 1 {
		for index, k := range os.Args {
			switch k {
			case "-Date":
				{
					MongoDB.Date = os.Args[index+1]
				}
			case "-Date2":
				{
					TaskDispatch.Date2 = os.Args[index+1]
				}
			case "-Day":
				{
					Day, _ = strconv.Atoi(os.Args[index+1])
				}
			}
		}
	}

	DataManager.CurDay = int32(Day)

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
	}
	//mongo
	TaskDispatch.GetTaskManager().Init()
}

func main3() {
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
}

func main123() {
	s := "redis://:nAgzyy7sIc1@10.0.1.228:6381,10.0.1.210:6381,10.0.1.229:6381/3?PoolSize=5"
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	p, _ := u.User.Password()
	fmt.Println(u.Host)
	fmt.Println(p)
	fmt.Println(u.Path)
	fmt.Println(u.Fragment)
}

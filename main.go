package main

import (
	"Config"
	"RecordDelete3Day/DataManager"
	"RecordDelete3Day/MongoDB"
	"RecordDelete3Day/Redis"
	"RecordDelete3Day/TaskDispatch"
	"github.com/robfig/cron"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
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
	if len(os.Args) > 1 {
		for index, k := range os.Args {
			switch k {
			case "-SearchNum":
				{
					TaskDispatch.SearchNum, _ = strconv.Atoi(os.Args[index+1])
				}
			}
		}
	}

	config := Config.GetConfig()
	if err := Config.ReadConfig(); err != nil {
		logger.Error(err)
		return
	}
	logger.Infof("Config is [%v]", config)

	DataManager.CurDay = 7
	currentTime := time.Now()
	oldTime := currentTime.AddDate(0, 0, -9)
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
	_, err := c.AddFunc("00 12 * * *", initDataManager3)
	if err != nil {
		logger.Error(err)
		return
	}

	_, err = c.AddFunc("00 6 * * *", initDataManager7)
	if err != nil {
		logger.Error(err)
		return
	}

	_, err = c.AddFunc("00 19 * * *", initDataManager10)
	if err != nil {
		logger.Error(err)
		return
	}

	_, err = c.AddFunc("00 18 * * *", initDataManager1)
	if err != nil {
		logger.Error(err)
		return
	}

	_, err = c.AddFunc("00 17 * * *", initDataManager30)
	if err != nil {
		logger.Error(err)
		return
	}

	c.Start()
	defer c.Stop()

	a := make(chan bool)
	<-a
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

func initDataManager10() {
	DataManager.CurDay = 10
	currentTime := time.Now()
	oldTime := currentTime.AddDate(0, 0, -11)
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

func main4() {
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
			case "-SearchNum":
				{
					TaskDispatch.SearchNum, _ = strconv.Atoi(os.Args[index+1])
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

	if err := MongoDB.GetMongoRecordManager().Init(); err != nil {
		logger.Error(err)
		return
	}
	//mongo
	TaskDispatch.GetTaskManager().Init()
}

package TaskDispatch

import (
	"RecordDelete3Day/DataDefine"
	"RecordDelete3Day/DataManager"
	"RecordDelete3Day/MongoDB"
	"RecordDelete3Day/Redis"
	"RecordDelete3Day/StorageMaintainerGRpc/StorageMaintainerGRpcClient"
	"errors"
	"github.com/sirupsen/logrus"
	"iPublic/LoggerModular"
	"iPublic/MongoModular"
	"strings"
	_ "strings"
	"sync"
	"sync/atomic"
	"time"
)

var MongoCount int32
var NoMongoCount int32
var AppendCount int32

var MongoDeleteFailCount int32
var MongoDeleteCount int32

var MongoWriteFailCount int32
var MongoWriteCount int32

var MongoFileCount int32

var Date2 string

func (manager *DeleteTask) Init() {
	manager.logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})

	num := strings.Split(MongoDB.Date, "-")
	MongoDB.Date = num[2]

	manager.DeleteServerList = make(map[string]*DeleteServerInfo)
	manager.NotifyFailedList = make(map[string][]DataDefine.RecordFileInfo)
	manager.ServerList = make(map[string]string)

	manager.ServerList["10.0.2.131"] = "192.168.2.131"
	manager.ServerList["10.0.2.132"] = "192.168.2.132"
	manager.ServerList["10.0.2.133"] = "192.168.2.133"
	manager.ServerList["10.0.2.134"] = "192.168.2.134"
	manager.ServerList["10.0.2.79"] = "192.168.2.79"
	manager.ServerList["10.0.2.84"] = "192.168.2.84"

	manager.getDeleteServer()

	//防止服务器时间出问题，取时间时和互联网对时一次，误差超过1小时当次不处理
	for !DataManager.CheckNetworkTimeWithNTSC() {
		manager.logger.Errorf("Check Time Sync Error, will continue. ")
		time.Sleep(time.Second * 15)
	}

	manager.goStartQueryMongoByMountPoint()

	if err := manager.goConnectDeleteServer(); err != nil {
		manager.logger.Error(err)
		return
	}

	manager.SendTask()

	manager.logger.Infof("DeviceCount: [%v]", DataManager.DeviceCount)
	manager.logger.Infof("NoMongoCount: [%v]", NoMongoCount)
	manager.logger.Infof("MongoCount: [%v]", MongoCount)
	manager.logger.Infof("AppendCount: [%v]", AppendCount)
	manager.logger.Infof("MongoDeleteFailCount: [%v]", MongoDeleteFailCount)
	manager.logger.Infof("MongoDeleteCount: [%v]", MongoDeleteCount)
	manager.logger.Infof("MongoWriteFailCount: [%v]", MongoWriteFailCount)
	manager.logger.Infof("MongoWriteCount: [%v]", MongoWriteCount)
	manager.logger.Infof("MongoFileCount: [%v]", MongoFileCount)

	DataManager.DeviceCount = 0
	NoMongoCount = 0
	MongoCount = 0
	AppendCount = 0
	MongoDeleteFailCount = 0
	MongoDeleteCount = 0
	MongoWriteFailCount = 0
	MongoWriteCount = 0
	MongoFileCount = 0

	manager.TaskList = []DataDefine.RecordFileInfo{}
}

func (manager *DeleteTask) getDeleteServer() {
	tempDeleteServerList := Redis.GetRedisRecordManager().GetDeleteServerConfig()
	for key, v := range tempDeleteServerList {
		srv := &DeleteServerInfo{
			Con:        &StorageMaintainerGRpcClient.GRpcClient{},
			Mountponit: "",
		}
		err := srv.Con.GRpcDial(key)
		if err != nil {
			manager.logger.Errorf("Dial Grpc Error: [%v]", err)
			continue
		}
		srv.Mountponit = v
		manager.DeleteServerList[key] = srv
		manager.logger.Infof("DeleteServer Has Connected: [%v]", key)
	}
}

func (manager *DeleteTask) goStartQueryMongoByMountPoint() {
	taskmap := DataManager.GetDataManager().GetMountPointMap()
	var wg sync.WaitGroup
	index := 0
	srv := MongoDB.GetMongoRecordManager()
	srv1 := MongoDB.GetMongoRecordManager1()
	lens := len(srv.Srv)
	for key, v := range taskmap {
		wg.Add(1)
		go manager.getNeedDeleteTask(key, v, &wg, srv.Srv[index], srv1.Srv[index])
		if index >= lens-1 {
			index = 0
		} else {
			index++
		}
	}
	wg.Wait()
	manager.logger.Info("查询结束")
}

func (manager *DeleteTask) goSearchTaskOnMongo2(v DataManager.StorageDaysInfo, mountpoint string, srv MongoModular.MongoDBServ, chxianliu chan int) {
	manager.logger.Infof("开启挂载点[%v]查询子协程: [%v]", mountpoint, v.ChannelInfo)
	_, err := DataManager.GetSubDayMorningTimeStamp(int(v.StorageDays))
	if err != nil {
		manager.logger.Errorf("Get SubDay MorningTimeStamp err: [%v] ", err)
		<-chxianliu
		return
	}
	var dbResults []DataDefine.RecordFileInfo

	s := strings.Split(Date2, "-")
	dates := s[0] + s[1] + s[2]

	t1 := time.Now()
	err1, table, date := MongoDB.QueryRecordbydate(v.ChannelInfo, dates, &dbResults, 0, srv)
	t2 := time.Now()

	if err1 != nil {
		manager.logger.Errorf("Get MongoDB Record Error: [%v], ChannelId: [%v], 协程: [%v], Table: [%v], Date: [%v]", err1, v.ChannelInfo, mountpoint, table, date)
		time.Sleep(time.Second * 3)
		for {
			err3, _, _ := MongoDB.QueryRecordbydate(v.ChannelInfo, dates, &dbResults, 0, srv)
			if err3 != nil {
				time.Sleep(time.Second * 3)
				continue
			} else {
				break
			}
		}
	}

	if len(dbResults) == 0 {
		atomic.AddInt32(&NoMongoCount, 1)
		manager.logger.Infof("No DBResult For ChannelID:[%v], 协程: [%v], 查询耗时: [%v], Table: [%v], Date: [%v]", v.ChannelInfo, mountpoint, t2.Sub(t1).Seconds(), table, date)
		<-chxianliu
		return
	} else {
		atomic.AddInt32(&MongoCount, 1)
		manager.logger.Infof("Get DBResult For ChannelID:[%v], len: [%v], 协程: [%v], 查询耗时: [%v], Table: [%v], Date: [%v]", v.ChannelInfo, len(dbResults), mountpoint, t2.Sub(t1).Seconds(), table, date)
		temkeymp := make(map[string]string)
		var temp []DataDefine.RecordFileInfo
		atomic.AddInt32(&AppendCount, 1)
		manager.TaskListLock.Lock()
		dbResults[0].LockStatus = int(v.Type)
		manager.TaskList = append(manager.TaskList, dbResults[0])
		manager.TaskListLock.Unlock()
		temp = append(temp, dbResults[0])
		temkeymp[dbResults[0].MountPoint] = dbResults[0].MountPoint
		for _, task := range dbResults {
			if _, ok := temkeymp[task.MountPoint]; !ok {
				atomic.AddInt32(&AppendCount, 1)
				manager.TaskListLock.Lock()
				task.LockStatus = int(v.Type)
				manager.TaskList = append(manager.TaskList, task)
				manager.TaskListLock.Unlock()
				temp = append(temp, dbResults[0])
				temkeymp[task.MountPoint] = task.MountPoint
			}
		}
		t := time.Now()
		info, err2, table, riqi := MongoDB.DeleteMongoTsAll(v.ChannelInfo, dates, srv)
		if err2 != nil {
			atomic.AddInt32(&MongoDeleteFailCount, 1)
			manager.logger.Errorf("Delete MongoDB Record Error: [%v], ChannelId: [%v], 协程: [%v], Table: [%v], Time: [%v], Date: [%v]", err2, v.ChannelInfo, mountpoint, table, time.Since(t).Seconds(), riqi)
			time.Sleep(time.Second * 3)
			for {
				info, err4, table, riqi := MongoDB.DeleteMongoTsAll(v.ChannelInfo, dates, srv)
				if err4 != nil {
					atomic.AddInt32(&MongoDeleteFailCount, 1)
					manager.logger.Errorf("Delete MongoDB Record Again Error: [%v], ChannelId: [%v], 协程: [%v], Table: [%v], Time: [%v], Date: [%v]", err4, v.ChannelInfo, mountpoint, table, time.Since(t).Seconds(), riqi)
					time.Sleep(time.Second * 3)
					continue
				} else {
					atomic.AddInt32(&MongoDeleteCount, 1)
					atomic.AddInt32(&MongoFileCount, int32(info.Removed))
					manager.logger.Infof("Delete MongoDB Record Again Success, ChannelId: [%v], 协程: [%v], Table: [%v], Time: [%v], Count: [%v], Date: [%v]", v.ChannelInfo, mountpoint, table, time.Since(t).Seconds(), info.Removed, riqi)
					break
				}
			}
		} else {
			atomic.AddInt32(&MongoDeleteCount, 1)
			atomic.AddInt32(&MongoFileCount, int32(info.Removed))
			manager.logger.Infof("Delete MongoDB Record Success, ChannelId: [%v], 协程: [%v], Table: [%v], Time: [%v], Count: [%v], Date: [%v]", v.ChannelInfo, mountpoint, table, time.Since(t).Seconds(), info.Removed, riqi)
		}
		for _, v := range temp {
			errs, t := MongoDB.WriteMongoFile(v, srv)
			if errs != nil {
				atomic.AddInt32(&MongoWriteFailCount, 1)
				manager.logger.Errorf("Write MongoDB Record Error: [%v], ChannelId: [%v], 协程: [%v], Table: [%v]", errs, v.ChannelInfoID, mountpoint, t)
				time.Sleep(time.Second * 5)
				for {
					er, tt := MongoDB.WriteMongoFile(v, srv)
					if er != nil {
						atomic.AddInt32(&MongoWriteFailCount, 1)
						time.Sleep(time.Second * 5)
						manager.logger.Errorf("Write MongoDB Record Again Error: [%v], ChannelId: [%v], 协程: [%v], Table: [%v]", er, v.ChannelInfoID, mountpoint, tt)
						continue
					} else {
						atomic.AddInt32(&MongoWriteCount, 1)
						manager.logger.Infof("Write MongoDB Record Again Success, ChannelId: [%v], 协程: [%v], Table: [%v]", v.ChannelInfoID, mountpoint, tt)
						break
					}
				}
			} else {
				atomic.AddInt32(&MongoWriteCount, 1)
				manager.logger.Infof("Write MongoDB Record Success, ChannelId: [%v], 协程: [%v], Table: [%v]", v.ChannelInfoID, mountpoint, t)
			}
		}
	}
	<-chxianliu
}

//mongo中查询需要删除的数据
func (manager *DeleteTask) getNeedDeleteTask(mountpoint string, task []DataManager.StorageDaysInfo, wg *sync.WaitGroup, srv MongoModular.MongoDBServ, srv1 MongoModular.MongoDBServ) {
	manager.logger.Infof("开启协程: [%v], len: [%v]", mountpoint, len(task))
	defer wg.Done()
	chResist := make(chan int, SearchNum)
	for _, v := range task {
		chResist <- 0
		if v.Type == 4 { //动存
			manager.goSearchTaskOnMongo2(v, mountpoint, srv1, chResist)
		} else if v.Type == 0 || v.Type == 9 { //全存
			manager.goSearchTaskOnMongo2(v, mountpoint, srv1, chResist)
		}
		time.Sleep(time.Nanosecond)
	}
}

//提取需要删除的TS
func (manager *DeleteTask) goConnectDeleteServer() error {
	manager.logger.Infof("tsTask len is: [%v]", len(manager.TaskList))
	if len(manager.DeleteServerList) == 0 {
		manager.logger.Error("DeleteServerList is Empty")
		return errors.New("DeleteServerList is Empty")
	}
	for _, task := range manager.TaskList {
		strAddr := ""
		for key, v := range manager.DeleteServerList {
			if strings.Contains(v.Mountponit, task.MountPoint) {
				strAddr = key
				manager.logger.Infof("Handle MountPoint [%s] DeleteServer [%v] Has Found:, ChannelID:[%s]", task.MountPoint, strAddr, task.ChannelInfoID)
				manager.DeleteServerList[key].task = append(manager.DeleteServerList[key].task, task)
			}
		}
		if strAddr == "" {
			manager.logger.Errorf("No DeleteServer to Handle MountPoint:[%s], ChannelID:[%s]", task.MountPoint, task.ChannelInfoID)
		}
		time.Sleep(time.Nanosecond)
	}
	for key, tasks := range manager.DeleteServerList {
		manager.logger.Infof("服务器收到任务数: [%v], [%v]", len(tasks.task), key)
	}
	manager.logger.Info("Connect Over")
	return nil
}

func (manager *DeleteTask) SendTask() {
	count := 0
	for key, server := range manager.DeleteServerList {
		for _, v := range server.task {
			v.MountPoint += "/"
			manager.logger.Infof("开始通知删除服务器[%v], ChannelInfoID[%v], MountPoint[%v]", key, v.ChannelInfoID, v.MountPoint)
			pRespon, err := server.Con.Notify(v.ChannelInfoID, v.RecordRelativePath, v.MountPoint, Date2, v.ID.Hex(), v.StartTime, int32(v.LockStatus))
			if nil != err {
				manager.logger.Errorf("收到删除结果[%v]失败[%v], Error: [%v]", key, v, err)
				count = 1
				manager.NotifyFailedList[key] = append(manager.NotifyFailedList[key], v)
				continue
			}
			if pRespon.NRespond == 1 {
				manager.logger.Infof("删除服务器[%v]已收到任务[%v]", key, v)
			}
			time.Sleep(time.Nanosecond)
		}
	}
	if count != 0 {
		time.Sleep(time.Second * 60 * 5)
		manager.logger.Info("Start ReNotify")
		var wg sync.WaitGroup
		for key, v := range manager.NotifyFailedList {
			wg.Add(1)
			go manager.ReNotify(key, v, &wg)
		}
		wg.Wait()
		manager.logger.Info("ReNotify Over")
	}
	manager.logger.Info("Notify All Over")
}

func (manager *DeleteTask) ReNotify(key string, date []DataDefine.RecordFileInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	temp := strings.Split(key, ":")
	key = temp[0]
	key1 := manager.ServerList[key]
	for {
		if err, add := Redis.GetRedisRecordManager().GetSeverList(key1); err != nil {
			manager.logger.Infof("删除服务器未上线: [%v]", key1)
			time.Sleep(time.Second * 60 * 5)
		} else {
			srv := &DeleteServerInfo{
				Con: &StorageMaintainerGRpcClient.GRpcClient{},
			}
			err := srv.Con.GRpcDial(add)
			if err != nil {
				manager.logger.Errorf("Dial Grpc Error: [%v]", err)
				continue
			}
			for _, v := range date {
				manager.logger.Infof("重新开始通知删除服务器[%v], ChannelInfoID[%v], MountPoint[%v]", key1, v.ChannelInfoID, v.MountPoint)
				pRespon, err := srv.Con.Notify(v.ChannelInfoID, v.RecordRelativePath, v.MountPoint, Date2, v.ID.Hex(), v.StartTime, int32(v.LockStatus))
				if nil != err {
					manager.logger.Errorf("重新收到删除结果[%v]失败[%v], Error: [%v]", key1, v, err)
					continue
				}
				if pRespon.NRespond == 1 {
					manager.logger.Infof("重新删除服务器[%v]已收到任务[%v]", key1, v)
				}
				time.Sleep(time.Nanosecond)
			}
			break
		}
	}
}

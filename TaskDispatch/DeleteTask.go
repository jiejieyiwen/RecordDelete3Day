package TaskDispatch

import (
	"StorageMaintainer1/DataDefine"
	SDataDefine "StorageMaintainer1/DataDefine"
	"StorageMaintainer1/DataManager"
	"StorageMaintainer1/MongoDB"
	"StorageMaintainer1/Redis"
	"StorageMaintainer1/StorageMaintainerGRpc/StorageMaintainerGRpcClient"
	"StorageMaintainer1/StorageMaintainerGRpc/StorageMaintainerMessage"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2/bson"
	"iPublic/LoggerModular"
	"strings"
	"sync"
	"time"
)

func (manager *DeleteTask) Init() {
	manager.bRunning = true
	manager.logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	manager.m_mapDeleteServer = make(map[string]*DeleteServer)
	manager.m_mapAllMountPoint = make(map[string][]string)
	manager.m_mapDeleteOnMongoList = make(map[string]chan StorageMaintainerMessage.StreamResData, DataManager.Size)
	manager.m_chResults = make(chan StorageMaintainerMessage.StreamResData, 1024)

	manager.DeleteServerList = make(map[string]*DeleteServerInfo)
	manager.getDeleteServer()

	for !DataManager.CheckNetworkTimeWithNTSC() { //防止服务器时间出问题，取时间时和互联网对时一次，误差超过1小时当次不处理
		manager.logger.Errorf("Check Time Sync Error, will continue. ")
		time.Sleep(time.Second * 15)
	}
	go manager.gogetDeleteServer()

	go manager.goStartQueryMongoByMountPoint()

	go manager.goSendNotifyToDeleteServer()
	//
	//go manager.goGetResults()
}

func (manager *DeleteTask) getDeleteServer() {
	tempDeleteServerList := Redis.GetRedisRecordManager().GetDeleteServerConfig()
	manager.DeleteServerListLock.Lock()
	for key, v := range tempDeleteServerList {
		if manager.DeleteServerList[key] == nil {
			srv := &DeleteServerInfo{
				Con:        &StorageMaintainerGRpcClient.GRpcClient{},
				Mountponit: "",
			}
			err := srv.Con.GRpcDial(key)
			if err != nil {
				manager.logger.Errorf("Dial Grpc Error: [%v]", err)
				continue
			}
			manager.logger.Infof("DeleteServer Has Connected: [%v]", key)
			srv.Mountponit = v
			manager.DeleteServerList[key] = srv
			continue
		}
		manager.logger.Infof("DeleteServer Connection Is OK: [%v]", key)
	}
	manager.DeleteServerListLock.Unlock()
}

func (manager *DeleteTask) gogetDeleteServer() {
	for manager.bRunning {
		tempDeleteServerList := Redis.GetRedisRecordManager().GetDeleteServerConfig()
		manager.DeleteServerListLock.Lock()
		for key, v := range tempDeleteServerList {
			if manager.DeleteServerList[key] == nil {
				srv := &DeleteServerInfo{
					Con:        &StorageMaintainerGRpcClient.GRpcClient{},
					Mountponit: "",
				}
				err := srv.Con.GRpcDial(key)
				if err != nil {
					manager.logger.Errorf("Dial Grpc Error: [%v]", err)
					continue
				}
				manager.logger.Infof("DeleteServer Has Connected: [%v]", key)
				srv.Mountponit = v
				manager.DeleteServerList[key] = srv
				continue
			}
			manager.logger.Infof("DeleteServer Connection Is OK: [%v]", key)
		}
		manager.DeleteServerListLock.Unlock()
		manager.logger.Info("Update DeleteServer Success")
		time.Sleep(time.Second * 1800)
	}
}

func (manager *DeleteTask) goStartQueryMongoByMountPoint() {
	for manager.bRunning {
		taskmap := DataManager.GetDataManager().GetMountPointMap()
		var wg sync.WaitGroup
		wg.Add(len(taskmap))
		for key, v := range taskmap {
			go manager.getNeedDeleteTask(key, v, &wg)
		}
		wg.Wait()

		temp := manager.GetRevertId()
		for _, id := range temp {
			MongoDB.GetMongoRecordManager().RevertFailedDelete(id)
		}
		time.Sleep(3 * time.Second)

		manager.logger.Info("Start To Get New ChannelStorage")
		DataManager.GetDataManager().TaskMap = []DataManager.StorageDaysInfo{}
		DataManager.GetDataManager().GetNewChannelStorage()
		time.Sleep(3 * time.Second)
	}
}

//mongo中查询需要删除的数据
func (manager *DeleteTask) getNeedDeleteTask(mountpoint string, task []DataManager.StorageDaysInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	for _, v := range task {
		startTs, err := DataManager.GetSubDayMorningTimeStamp(v.StorageDays)
		//天数转换出错就继续下一个
		if err != nil {
			manager.logger.Errorf("Get SubDay MorningTimeStamp err: [%v] ", err)
			continue
		}
		var dbResults []DataDefine.RecordFileInfo
		err = MongoDB.GetMongoRecordManager().QueryRecord(v.ChannelInfo, startTs, &dbResults, 0)
		if err != nil {
			manager.logger.Errorf("Get MongoDB Record Error: [%v], ChannelId: [%v]", err, v.ChannelInfo)
			continue
		}
		if len(dbResults) == 0 {
			manager.logger.Infof("No DBResult For ChannelID:[%v]", v.ChannelInfo)
			continue
		}
		manager.logger.Infof("Get Qualify Data Form MongoDB, len:[%v], ChannelID:[%v]", len(dbResults), v.ChannelInfo)
		mapdate := make(map[string]string)
		for i, k := range dbResults {
			if i == 0 {
				if err := MongoDB.GetMongoRecordManager().SetTsInfoMongoToLock(k.ID); err != nil {
					manager.logger.Errorf("Set TsInfo Mongo To Lock 1 [%s], Error:[%v]", k.RecordID, err)
					continue
				}
				t := time.Unix(k.StartTime, 0)
				date := t.Format("2006-01-02")
				mapdate[date] = date
				DataManager.GetDataManager().PushNeedDeleteTs(k)
				continue
			}
			t := time.Unix(k.StartTime, 0)
			date1 := t.Format("2006-01-02")
			//不属于同一天的才装进预删除表,并添加到临时表
			if _, ok := mapdate[date1]; ok == false {
				manager.logger.Infof("Append Date Is: [%v], ChannelId: [%v]", date1, k.ChannelInfoID)
				if err := MongoDB.GetMongoRecordManager().SetTsInfoMongoToLock(k.ID); err != nil {
					manager.logger.Errorf("Set TsInfo Mongo To Lock 1 [%v], Error:[%v]", k.ChannelInfoID, err)
					continue
				}
				mapdate[date1] = date1
				DataManager.GetDataManager().PushNeedDeleteTs(k)
			} else {
				//如果挂载点飘了也要装进删除表
				if mountpoint != k.MountPoint {
					if err := MongoDB.GetMongoRecordManager().SetTsInfoMongoToLock(k.ID); err != nil {
						manager.logger.Errorf("Set TsInfo Mongo To Lock 1 [%v], Error:[%v]", k.ChannelInfoID, err)
						continue
					}
					DataManager.GetDataManager().PushNeedDeleteTs(k)
					continue
				}
				if err := MongoDB.GetMongoRecordManager().SetInfoMongoToDelete(k.ID); err != nil {
					manager.logger.Errorf("Set TsInfo Mongo To Lock 2 [%s], Error:[%v]", k.RecordID, err)
					continue
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
}

//提取需要删除的TS
func (manager *DeleteTask) goSendNotifyToDeleteServer() {
	for manager.bRunning {
		tsTask := DataManager.GetDataManager().GetNeedDeleteTs(SDataDefine.SingleDealLimit)

		strAddr := ""
		var con *StorageMaintainerGRpcClient.GRpcClient

		manager.DeleteServerListLock.Lock()
		tempDeleteServer := manager.DeleteServerList
		manager.DeleteServerListLock.Unlock()

		for _, task := range tsTask {
			for key, v := range tempDeleteServer {
				if strings.Contains(task.MountPoint, v.Mountponit) {
					strAddr = key
					con = v.Con
					break
				}
			}

			if strAddr == "" && con == nil {
				manager.logger.Errorf("NO DeleteServer to HandleThis MountPoint:[%s], ChannelID:[%s]", task.MountPoint, task.ChannelInfoID)
				manager.AddRevertId(task.ID)
				continue
			}

			t := time.Unix(task.StartTime, 0)
			date := t.Format("2006-01-02")

			pRespon, err := con.Notify(task.ChannelInfoID, task.RecordRelativePath, task.MountPoint, date, task.ID.Hex(), task.StartTime)

			if nil != err {
				manager.logger.Errorf("通知删除服务器[%s]失败,err:%v", strAddr, err.Error())
				manager.AddRevertId(task.ID)
				continue
			}
			manager.logger.Infof("收到删除结果:[%v]", *pRespon)
			//manager.m_chResults <- *pRespon
		}
		time.Sleep(time.Millisecond)
	}
}

func (manager *DeleteTask) goGetResults() {
	/*
		若有删除结果，则取出来，没有则阻塞
	*/
	for result := range manager.m_chResults {
		switch result.GetNRespond() {
		case 1:
			{
				//删除成功，装入删除列表
				if !bson.IsObjectIdHex(result.GetStrRecordID()) {
					manager.logger.Error("收到的信息错误")
					manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
					continue
				}
				manager.logger.Infof("ChannelID[%v] 删除文件成功, MountPoint[%v]", result.GetStrChannelID(), result.StrMountPoint)
				if err := MongoDB.GetMongoRecordManager().SetInfoMongoToDelete(bson.ObjectIdHex(result.GetStrRecordID())); err != nil {
					manager.logger.Errorf("Set TS Info On Mongo To Delete Error, ChannelID[%s], RecordID: [%v], Error: [%v]", result.StrChannelID, result.GetStrRecordID(), err)
					manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
				} else {
					manager.logger.Infof("删除mongo记录成功: ChannelID: [%v], RecordID: [%v]", result.StrChannelID, result.GetStrRecordID())
				}
			}
		case -1:
			{
				if !bson.IsObjectIdHex(result.GetStrRecordID()) {
					manager.logger.Error("收到的信息错误")
					manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
					continue
				}
				manager.logger.Infof("Channel[%s] 删除文件失败, 错误代码：[%v]", result.GetStrChannelID(), result.NRespond)
				manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
			}
		case -2:
			{
				if !bson.IsObjectIdHex(result.GetStrRecordID()) {
					manager.logger.Error("收到的信息错误")
					manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
					continue
				}
				manager.logger.Infof("Channel[%s] 删除文件失败, 错误代码：[%v]", result.GetStrChannelID(), result.NRespond)
				manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
			}
		}
	}
}

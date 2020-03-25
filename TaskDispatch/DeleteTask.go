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

/*
	从DataManager查询需要处理的通道信息，查询相应TS
*/

func (manager *DeleteTask) Init() {
	manager.bRunning = true
	manager.logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	manager.m_mapDeleteServer = make(map[string]*DeleteServer)
	manager.m_mapAllMountPoint = make(map[string][]string)
	manager.m_mapDeleteByMountPoint = make(map[string][]StorageMaintainerMessage.StreamResData, DataManager.Size)

	for !DataManager.CheckNetworkTimeWithNTSC() { //防止服务器时间出问题，取时间时和互联网对时一次，误差超过1小时当次不处理
		manager.logger.Errorf("Check Time Sync Error, will continue. ")
		time.Sleep(time.Second * 30)
	}
	go manager.goStartSearchDeleteServer()

	//go manager.GoStartQueryMongoDB()

	go manager.GoStartQueryMongoByMountPoint()

	go manager.GoStartDeleteFromQueue()
}

func (manager *DeleteTask) goStartSearchDeleteServer() {
	/*
		定时扫描Redis，然后去连接删除服务器
	*/
	for manager.bRunning {
		pServerInfo := Redis.GetRedisRecordManager().GetDeleteServerConfig()
		manager.m_mapDeleteServerLock.Lock()
		for ServerAddr, strAllMountPoint := range pServerInfo {
			if pClient := manager.m_mapDeleteServer[ServerAddr]; nil != pClient {
				manager.logger.Infof("server[%s] has connected", ServerAddr)
				//已连接
				continue
			}
			pClient, err := NewDeleteServer(ServerAddr, 1024)
			if nil != err {
				//连接失败
				manager.logger.Error("Can't connect server[%s]", ServerAddr)
				CloseDeleteServer(pClient)
				continue
			}
			//添加到表中
			manager.logger.Infof("server[%s] is connecting", ServerAddr)
			manager.m_mapDeleteServer[ServerAddr] = pClient

			// 挂载点
			manager.m_mapAllMountPointLock.Lock()
			arrMountPoint := strings.Split(strAllMountPoint, ":")
			for _, mountPoint := range arrMountPoint {
				if _, ok := manager.m_mapAllMountPoint[mountPoint]; !ok {
					manager.m_mapAllMountPoint[mountPoint] = []string{ServerAddr}
				} else {
					manager.m_mapAllMountPoint[mountPoint] = append(manager.m_mapAllMountPoint[mountPoint], ServerAddr)
				}
			}
			manager.m_mapAllMountPointLock.Unlock()

			//开始工作
			go manager.goServerWorkerNofity(ServerAddr, pClient.m_pClient, pClient.m_chTask, pClient.m_chResult)
			go manager.goGetResults(ServerAddr, pClient.m_chResult)
		}
		manager.m_mapDeleteServerLock.Unlock()
		time.Sleep(1800 * time.Second)
	}
}

func (manager *DeleteTask) GoStartQueryMongoDB() {
	/*
		从MongoDB中，查询需要处理的TS记录, 推入DataManager
	*/
	count := 0
	for manager.bRunning {
		chInfos := DataManager.GetDataManager().GetAllChannelInfo()
		manager.logger.Debugf("len of ChannelInfo[%v]", len(chInfos))
		for _, chInfo := range chInfos {
			tt1 := time.Now()
			if startTs, err := DataManager.GetSubDayMorningTimeStamp(chInfo.StorageDays); err != nil {
				manager.logger.Errorf("GetSubDayMorningTimeStamp err: [%v] ", err)
			} else {
				for {
					var dbResults []DataDefine.RecordFileInfo
					//prometheus
					//t1 := time.Now()
					if err := MongoDB.GetMongoRecordManager().QueryRecord(chInfo.ChannelInfo, startTs, &dbResults, DataDefine.SingleDealLimit); err != nil {
						manager.logger.Errorf("Get Mongo Record Manager.QueryRecord Error: [%v]", err)
					} else {
						//prometheus
						//t2 := time.Now()
						//subTime := t2.Sub(t1).Milliseconds()
						//manager.m_pDiskPercent.WithLabelValues("QueryRecordTime").Set(float64(t2.Sub(t1).Milliseconds()))
						if len(dbResults) == 0 {
							manager.logger.Infof("No DBResult from Channel, Channel:[%v], QueryTime:[%v]", chInfo.ChannelInfo)
							break
						} else {
							manager.logger.Debugf("MongDB len:[%v], Channel:[%v], QueryTime:[%v]", len(dbResults), chInfo.ChannelInfo)
						}
						var mapdate = make(map[string]string, 0)
						for i, k := range dbResults {
							if i == 0 {
								if err := MongoDB.GetMongoRecordManager().SetTsInfoMongoToLock(k.ID); err != nil {
									manager.logger.Errorf("Set TsInfo Mongo To Lock 1 [%s], Error:[%v]", k.RecordID, err)
									continue
								}
								t := time.Unix(int64(k.StartTime), 0)
								date := t.Format("2006-01-02")
								mapdate[date] = date
								DataManager.GetDataManager().PullNeedDeleteTs(k)
								count++
								continue
							}
							t := time.Unix(int64(k.StartTime), 0)
							date1 := t.Format("2006-01-02")
							//不属于同一天的才装进预删除表,并添加到临时表
							if _, ok := mapdate[date1]; ok == false {
								manager.logger.Infof("Append Date Is: [%v], channelid: [%v]", date1, k.ChannelInfoID)
								if err := MongoDB.GetMongoRecordManager().SetTsInfoMongoToLock(k.ID); err != nil {
									manager.logger.Errorf("Set TsInfo Mongo To Lock 1 [%v], Error:[%v]", k.RecordID, err)
									continue
								}
								mapdate[date1] = date1
								DataManager.GetDataManager().PullNeedDeleteTs(k)
								count++
							} else {
								if err := MongoDB.GetMongoRecordManager().SetInfoMongoToDelete(k.ID); err != nil {
									manager.logger.Errorf("Set TsInfo Mongo To Lock 2 [%s], Error:[%v]", k.RecordID, err)
									continue
								}
								count++
							}
							//time.Sleep(2 * time.Millisecond)
						}
					}
				}
			}
			tt2 := time.Now()
			subTime := tt2.Sub(tt1).Milliseconds()
			time.Sleep(time.Millisecond)

			manager.logger.Infof("查询时间为：[%v] 毫秒,channelid:[%v]", subTime, chInfo.ChannelInfo)
		}

		temp := manager.GetRevertId()
		for _, id := range temp {
			MongoDB.GetMongoRecordManager().RevertFailedDelete(id)
		}
		manager.logger.Infof("total delete on Mongo：[%v]", count-len(temp))
		time.Sleep(5 * time.Second)

		manager.logger.Info("GetNewChannelStorage")
		DataManager.GetDataManager().GetNewChannelStorage()
		time.Sleep(5 * time.Second)
	}
}

func (manager *DeleteTask) GoStartQueryMongoByMountPoint() {
	for manager.bRunning {
		taskmap := DataManager.GetDataManager().GetMountPointMap()
		var wg sync.WaitGroup
		for _, v := range taskmap {
			wg.Add(1)
			go manager.getNeedDeleteTask(v, &wg)
		}
		wg.Wait()
		temp := manager.GetRevertId()
		for _, id := range temp {
			MongoDB.GetMongoRecordManager().RevertFailedDelete(id)
		}
		time.Sleep(5 * time.Second)

		manager.logger.Info("Start To Get New ChannelStorage")
		DataManager.GetDataManager().GetNewChannelStorage()
		time.Sleep(5 * time.Second)
	}
}

func (manager *DeleteTask) getNeedDeleteTask(task []DataManager.StorageDaysInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	for _, v := range task {
		startTs, err := DataManager.GetSubDayMorningTimeStamp(v.StorageDays)
		//时间校验出错就跳出
		if err != nil {
			manager.logger.Errorf("GetSubDayMorningTimeStamp err: [%v] ", err)
			break
		}
		var dbResults []DataDefine.RecordFileInfo
		err = MongoDB.GetMongoRecordManager().QueryRecord(v.ChannelInfo, startTs, &dbResults, 0)
		if err != nil {
			manager.logger.Errorf("Get MongoDB Record Error: [%v], ChannelId: [%v]", err, v.ChannelInfo)
			continue
		}
		if len(dbResults) == 0 {
			manager.logger.Infof("No DBResult For ChannelID:[%v]", v.ChannelInfo)
			break
		}
		DataDefine.SingleDealLimit = len(dbResults)
		manager.logger.Infof("Get Qualify Data Form MongoDB, len:[%v], ChannelID:[%v]", len(dbResults), v.ChannelInfo)
		for _, k := range dbResults {
			if err := MongoDB.GetMongoRecordManager().SetTsInfoMongoToLock(k.ID); err != nil {
				manager.logger.Errorf("Set TsInfo Mongo To Lock 1 [%s], Error:[%v]", k.RecordID, err)
				continue
			}
			DataManager.GetDataManager().PullNeedDeleteTs(k)

		}
	}
}

func (manager *DeleteTask) GoStartDeleteFromQueue() {
	/*
		提取需要删除的TS
	*/
	for manager.bRunning {
		tsTask := DataManager.GetDataManager().PushNeedDeleteTs(DataDefine.SingleDealLimit)
		if len(tsTask) == 0 {
			time.Sleep(time.Millisecond * 2)
			continue
		}
		for _, task := range tsTask {
			path := GeneralTaskPath(task)
			manager.logger.Infof("Get Ts RealPath Over:[%s], Try Start Delete. ChannelID[%s]", path, task.ChannelInfoID)
			manager.m_mapAllMountPointLock.RLock()
			if serverList, ok := manager.m_mapAllMountPoint[task.MountPoint]; ok {
				//向挂载点下面的server发送信息
				if len(serverList) > 0 {
				} else {
					manager.logger.Warnf("NO DeleteServer to HandleThis MountPoint[%s],ChannelID[%s].", task.MountPoint, task.ChannelInfoID)
					manager.AddRevertId(task.ID)
				}
				for _, serverAddr := range serverList {
					manager.m_mapDeleteServerLock.RLock()
					if pClient, ok := manager.m_mapDeleteServer[serverAddr]; ok {
						pClient.m_chTask <- task
					} else {
						manager.logger.Warnf("NO DeleteServer to Handle This MountPoint[%v]", task.MountPoint)
						manager.AddRevertId(task.ID)
					}
					manager.m_mapDeleteServerLock.RUnlock()
				}
			} else {
				manager.logger.Errorf("NO DeleteServer to HandleThis MountPoint[%v]", task.MountPoint)
				manager.AddRevertId(task.ID)
			}
			manager.m_mapAllMountPointLock.RUnlock()
		}
		time.Sleep(time.Millisecond)
	}
}

func GeneralTaskPath(task DataDefine.RecordFileInfo) string {
	return task.RecordRelativePath
}

func (manager *DeleteTask) goGetResults(strAddr string, results <-chan StorageMaintainerMessage.StreamResData) {
	/*
		若有删除结果，则取出来，没有则阻塞
	*/
	manager.logger.Infof("Server:[%v] Start get Results", strAddr)
	for result := range results {
		switch result.GetNRespond() {
		case 1:
			{
				/*
					删除成功，设置MongDB为删除状态
				*/
				if !bson.IsObjectIdHex(result.GetStrRecordID()) {
					manager.logger.Error("收到的信息错误")
					continue
				}
				manager.logger.Infof("删除服务器[%v],Channel[%s] 删除文件成功,id[%v]", strAddr, result.GetStrChannelID(), result.StrRecordID)
				if err := MongoDB.GetMongoRecordManager().SetInfoMongoToDelete(bson.ObjectIdHex(result.GetStrRecordID())); err != nil {
					manager.logger.Errorf("删除服务器[%v],Set Info Mongo To Delete,id[%s],  Err:[%v]", strAddr, result.GetStrRecordID(), err)
					manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
				} else {
					manager.logger.Infof("删除服务器[%v],RecordID[%s],删除mongoDB成功", strAddr, result.GetStrRecordID())
				}
			}
		case -1:
			{
				if !bson.IsObjectIdHex(result.GetStrRecordID()) {
					manager.logger.Error("收到的信息错误")
					continue
				}
				manager.logger.Infof("删除服务器[%v],Channel[%s] 删除文件失败, 错误代码：[%v]", strAddr, result.GetStrChannelID(), result.NRespond)
				manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
			}
		case -2:
			{
				if !bson.IsObjectIdHex(result.GetStrRecordID()) {
					manager.logger.Error("收到的信息错误")
					continue
				}
				manager.logger.Infof("删除服务器[%v],Channel[%s] 删除文件失败, 错误代码：[%v]", strAddr, result.GetStrChannelID(), result.NRespond)
				manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
			}
		default:
		}
	}
	manager.logger.Infof("Server:[%v] End get Results", strAddr)
}

func (manager *DeleteTask) goServerWorkerNofity(strAddr string,
	pClient *StorageMaintainerGRpcClient.GRpcClient,
	pTask <-chan SDataDefine.RecordFileInfo,
	result chan<- StorageMaintainerMessage.StreamResData) {
	/*
		若有删除任务，则通知服务器删除，没有则阻塞
	*/
	manager.logger.Infof("Server[%s] start work", strAddr)
	for task := range pTask {
		if task.Date == "" {
			t := time.Unix(int64(task.StartTime), 0)
			task.Date = t.Format("2006-01-02")
		}
		manager.logger.Infof("删除服务器[%s]处理删除任务,ChannelID[%s],MountPoint[%s],RelativePath[%s],id[%s] ", strAddr, task.ChannelInfoID, task.MountPoint, task.RecordRelativePath, task.ID.Hex())
		pRespon, err := pClient.Notify(task.ChannelInfoID, task.RecordRelativePath, task.MountPoint, task.Date, task.ID.Hex(), task.StartTime)
		if nil != err {
			manager.logger.Warnf("通知删除服务器[%s]失败,err:%v", strAddr, err.Error())
			manager.AddRevertId(task.ID)
			break
		}
		manager.logger.Infof("收到删除结果:[%v]", *pRespon)
		result <- *pRespon
		if pRespon.NRespond == SDataDefine.STATUS_DELECT_SUCCESS_AND_SLEEP {
			time.Sleep(time.Second * 5)
		}
	}

	//表中删除
	pClient.Close()
	CloseDeleteServer(manager.m_mapDeleteServer[strAddr])
	delete(manager.m_mapDeleteServer, strAddr)
	manager.logger.Infof("Server[%s] end work", strAddr)
}

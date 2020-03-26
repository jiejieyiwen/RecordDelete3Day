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
	manager.m_mapDeleteOnMongoList = make(map[string]chan StorageMaintainerMessage.StreamResData, DataManager.Size)

	for !DataManager.CheckNetworkTimeWithNTSC() { //防止服务器时间出问题，取时间时和互联网对时一次，误差超过1小时当次不处理
		manager.logger.Errorf("Check Time Sync Error, will continue. ")
		time.Sleep(time.Second * 30)
	}
	go manager.goStartSearchDeleteServer()

	//go manager.GoStartQueryMongoDB()

	go manager.GoStartQueryMongoByMountPoint()

	go manager.GoStartDeleteFromQueue()

	go manager.gogetdeleteOnMongoList()
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
					manager.m_mapDeleteOnMongoListLock.Lock()
					manager.m_mapDeleteOnMongoList[mountPoint] = make(chan StorageMaintainerMessage.StreamResData, 5)
					manager.m_mapDeleteOnMongoListLock.Unlock()
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

func (manager *DeleteTask) GoStartQueryMongoByMountPoint() {
	for manager.bRunning {
		taskmap := DataManager.GetDataManager().GetMountPointMap()
		var wg sync.WaitGroup
		for key, v := range taskmap {
			wg.Add(1)
			go manager.getNeedDeleteTask(key, v, &wg)
		}
		wg.Wait()

		temp := manager.GetRevertId()
		for _, id := range temp {
			MongoDB.GetMongoRecordManager().RevertFailedDelete(id)
		}
		time.Sleep(5 * time.Second)

		manager.logger.Info("Start To Get New ChannelStorage")
		DataManager.GetDataManager().TaskMap = []DataManager.StorageDaysInfo{}
		DataManager.GetDataManager().GetNewChannelStorage()
		time.Sleep(5 * time.Second)
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
			break
		}
		manager.logger.Infof("Get Qualify Data Form MongoDB, len:[%v], ChannelID:[%v]", len(dbResults), v.ChannelInfo)
		for _, k := range dbResults {
			if err := MongoDB.GetMongoRecordManager().SetTsInfoMongoToLock(k.ID); err != nil {
				manager.logger.Errorf("Set TsInfo Mongo To Lock 1, ChannelID: [%v], Error: [%v]", k.ChannelInfoID, err)
				continue
			}
			DataManager.GetDataManager().PushNeedDeleteTsByMountPoints(mountpoint, k)
		}
	}
}

//提取需要删除的TS
func (manager *DeleteTask) GoStartDeleteFromQueue() {
	for manager.bRunning {
		tsTask := DataManager.GetDataManager().GetAllNeedDeleteTs()
		if len(tsTask) == 0 {
			time.Sleep(time.Millisecond)
			continue
		}
		var wg sync.WaitGroup
		for _, v := range tsTask {
			wg.Add(1)
			go manager.goconnectDeleteServer(v, &wg)
		}
		wg.Wait()
		time.Sleep(time.Second)
	}
}

func (manager *DeleteTask) goconnectDeleteServer(tsTask []SDataDefine.RecordFileInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	for _, task := range tsTask {
		path := GeneralTaskPath(task)
		manager.logger.Infof("Get Ts RealPath Over:[%s], Try Start Delete. ChannelID[%s]", path, task.ChannelInfoID)
		manager.m_mapAllMountPointLock.RLock()
		if serverList, ok := manager.m_mapAllMountPoint[task.MountPoint]; ok {
			//向挂载点下面的server发送信息
			if len(serverList) > 0 {
			} else {
				manager.logger.Errorf("NO DeleteServer to HandleThis MountPoint[%s], ChannelID[%s]1", task.MountPoint, task.ChannelInfoID)
				manager.AddRevertId(task.ID)
			}
			for _, serverAddr := range serverList {
				manager.m_mapDeleteServerLock.RLock()
				if pClient, ok := manager.m_mapDeleteServer[serverAddr]; ok {
					pClient.m_chTask <- task
				} else {
					manager.logger.Errorf("NO DeleteServer to Handle This MountPoint[%v], ChannelID[%s]2", task.MountPoint, task.ChannelInfoID)
					manager.AddRevertId(task.ID)
				}
				manager.m_mapDeleteServerLock.RUnlock()
			}
		} else {
			manager.logger.Errorf("NO DeleteServer to HandleThis MountPoint[%v], ChannelID[%s]3", task.MountPoint, task.ChannelInfoID)
			manager.AddRevertId(task.ID)
		}
		manager.m_mapAllMountPointLock.RUnlock()
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
				//删除成功，装入删除列表
				if !bson.IsObjectIdHex(result.GetStrRecordID()) {
					manager.logger.Error("收到的信息错误")
					continue
				}
				manager.logger.Infof("删除服务器[%v],ChannelID[%v] 删除文件成功, MountPoint[%v]",
					strAddr, result.GetStrChannelID(), result.StrMountPoint)
				manager.m_mapDeleteOnMongoListLock.Lock()
				manager.m_mapDeleteOnMongoList[result.StrMountPoint] <- result
				manager.m_mapDeleteOnMongoListLock.Unlock()
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

func (manager *DeleteTask) gogetdeleteOnMongoList() {
	for manager.bRunning {
		manager.m_mapDeleteOnMongoListLock.Lock()
		task := manager.m_mapDeleteOnMongoList
		manager.m_mapDeleteOnMongoListLock.Unlock()

		for _, v := range task {
			go manager.godeleteOnMongo(v)
		}
	}
}

func (manager *DeleteTask) godeleteOnMongo(task <-chan StorageMaintainerMessage.StreamResData) {
	if len(task) == 0 {
		return
	}
	for ts := range task {
		if err := MongoDB.GetMongoRecordManager().SetInfoMongoToDelete(bson.ObjectIdHex(ts.GetStrRecordID())); err != nil {
			manager.logger.Errorf("Set TS Info On Mongo To Delete Error, ChannelID[%s], RecordID: [%v], Error: [%v]", ts.StrChannelID, ts.GetStrRecordID(), err)
			manager.AddRevertId(bson.ObjectIdHex(ts.GetStrRecordID()))
		} else {
			manager.logger.Infof("删除mongo记录成功: ChannelID: [%v], RecordID: [%v]", ts.StrChannelID, ts.GetStrRecordID())
		}
	}
	return
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

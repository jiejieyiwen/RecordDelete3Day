package TaskDispatch

import (
	AMQPModular "AMQPModular2"
	"Config"
	"StorageMaintainer1/DataDefine"
	SDataDefine "StorageMaintainer1/DataDefine"
	"StorageMaintainer1/DataManager"
	"StorageMaintainer1/MongoDB"
	"StorageMaintainer1/Redis"
	"StorageMaintainer1/StorageMaintainerGRpc/StorageMaintainerGRpcClient"
	"StorageMaintainer1/StorageMaintainerGRpc/StorageMaintainerMessage"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2/bson"
	"iPublic/LoggerModular"
	"strings"
	"sync"
	"time"
)

var ConcurrentNumber int

func (manager *DeleteTask) Init() {
	manager.bRunning = true
	manager.logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})

	manager.m_pMQConn = new(AMQPModular.RabbServer)
	manager.m_strMQURL = Config.GetConfig().PublicConfig.AMQPURL
	//manager.m_strMQURL = "amqp://guest:guest@192.168.0.56:30001/"
	//manager.m_strMQURL = "amqp://user:2Jv4v3Qjrx@register1.mojing.ts:31100/?PoolSize=10"

	err := AMQPModular.GetRabbitMQServ(manager.m_strMQURL, manager.m_pMQConn)
	if err != nil {
		manager.logger.Errorf("Init MQ Failed, Errors: %v", err.Error())
		return
	}
	manager.logger.Infof("Init MQ Success: [%v]", manager.m_strMQURL)

	go manager.goGetMQMsg()

	manager.m_chResults = make(chan StorageMaintainerMessage.StreamResData, 1024)

	manager.DeleteServerList = make(map[string]*DeleteServerInfo)
	manager.getDeleteServer()

	//防止服务器时间出问题，取时间时和互联网对时一次，误差超过1小时当次不处理
	for !DataManager.CheckNetworkTimeWithNTSC() {
		manager.logger.Errorf("Check Time Sync Error, will continue. ")
		time.Sleep(time.Second * 15)
	}

	go manager.goGetResults()

	go manager.goStartQueryMongoByMountPoint()

	go manager.goConnectDeleteServer()

	time.Sleep(time.Second * 60)
	go manager.goupdateDeleteServer()
}

func (manager *DeleteTask) getDeleteServer() {
	tempDeleteServerList := Redis.GetRedisRecordManager().GetDeleteServerConfig()
	for key, v := range tempDeleteServerList {
		srv := &DeleteServerInfo{
			Con:        &StorageMaintainerGRpcClient.GRpcClient{},
			Mountponit: "",
			task:       make(chan SDataDefine.RecordFileInfo, Config.GetConfig().StorageConfig.ConcurrentNumber),
		}
		err := srv.Con.GRpcDial(key)
		if err != nil {
			manager.logger.Errorf("Dial Grpc Error: [%v]", err)
			continue
		}
		srv.Mountponit = v
		manager.DeleteServerList[key] = srv

		go manager.goSend(manager.DeleteServerList[key], key)

		manager.logger.Infof("DeleteServer Has Connected: [%v]", key)
	}
}

func (manager *DeleteTask) goupdateDeleteServer() {
	for manager.bRunning {
		tempDeleteServerList := Redis.GetRedisRecordManager().GetDeleteServerConfig()

		manager.DeleteServerListLock.Lock()
		tempDeleteServer := manager.DeleteServerList
		manager.DeleteServerListLock.Unlock()

		for key, v := range tempDeleteServerList {
			//更新挂载点
			if tempDeleteServer[key].Mountponit != v {
				manager.DeleteServerListLock.Lock()
				manager.DeleteServerList[key].Mountponit = v
				manager.DeleteServerListLock.Unlock()
			}
			if tempDeleteServer[key].Con == nil {
				srv := &DeleteServerInfo{
					Con:        &StorageMaintainerGRpcClient.GRpcClient{},
					Mountponit: "",
					task:       make(chan SDataDefine.RecordFileInfo, Config.GetConfig().StorageConfig.ConcurrentNumber),
				}
				err := srv.Con.GRpcDial(key)
				if err != nil {
					manager.logger.Errorf("Dial Grpc Error: [%v]", err)
					continue
				}
				srv.Mountponit = v

				manager.DeleteServerListLock.Lock()
				manager.DeleteServerList[key] = srv
				manager.DeleteServerListLock.Unlock()

				manager.logger.Infof("New DeleteServer Has Connected: [%v]", key)

				go manager.goSend(manager.DeleteServerList[key], key)
				continue
			}
			manager.logger.Infof("DeleteServer Connection Is OK: [%v]", key)
		}
		manager.logger.Info("Update DeleteServer Success")
		time.Sleep(time.Second * 900)
	}
}

func (manager *DeleteTask) goStartQueryMongoByMountPoint() {
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
		time.Sleep(3 * time.Second)

		manager.logger.Info("Start To Get New ChannelStorage")
		DataManager.GetDataManager().TaskMap = []DataManager.StorageDaysInfo{}
		//DataManager.GetDataManager().NeedDeleteTsList = []SDataDefine.RecordFileInfo{}
		DataManager.GetDataManager().GetNewChannelStorage()
		time.Sleep(3 * time.Second)
	}
}

//mongo中查询需要删除的数据
func (manager *DeleteTask) getNeedDeleteTask(mountpoint string, task []DataManager.StorageDaysInfo, wg *sync.WaitGroup) {
	manager.logger.Infof("开启协程: [%v] ", mountpoint)
	defer wg.Done()
	for _, v := range task {
		startTs, err := DataManager.GetSubDayMorningTimeStamp(v.StorageDays)
		//count := 0
		//天数转换出错就继续下一个
		if err != nil {
			manager.logger.Errorf("Get SubDay MorningTimeStamp err: [%v] ", err)
			continue
		}
		var dbResults []DataDefine.RecordFileInfo
		t1 := time.Now()
		err = MongoDB.GetMongoRecordManager().QueryRecord(v.ChannelInfo, startTs, &dbResults, 1)
		t2 := time.Now()
		manager.logger.Infof("查询耗时: [%v], ChannelId: [%v]个", t2.Sub(t1).Milliseconds())
		if err != nil {
			manager.logger.Errorf("Get MongoDB Record Error: [%v], ChannelId: [%v], mountpoint: [%v]", err, v.ChannelInfo, mountpoint)
			continue
		}
		if len(dbResults) == 0 {
			manager.logger.Infof("No DBResult For ChannelID:[%v], mountpoint: [%v]", v.ChannelInfo, mountpoint)
			continue
		}
		DataManager.GetDataManager().PushNeedDeleteTs(dbResults[0])
		////manager.logger.Infof("Get Qualify Data Form MongoDB, len:[%v], ChannelID:[%v], mountpoint: [%v]", len(dbResults), v.ChannelInfo, mountpoint)
		//mapdate := make(map[string]string)
		//mapmp := make(map[string]string)
		//t := time.Unix(dbResults[0].StartTime, 0)
		//date := t.Format("2006-01-02")
		//mapdate[date] = date
		//mapmp[]
		//for i, k := range dbResults {
		//	if i == 0 {
		//		if err := MongoDB.GetMongoRecordManager().SetTsInfoMongoToLock(k.ID); err != nil {
		//			manager.logger.Errorf("Set TsInfo Mongo To Lock 1 [%s], Error:[%v]", k.RecordID, err)
		//			continue
		//		}
		//		t := time.Unix(k.StartTime, 0)
		//		date := t.Format("2006-01-02")
		//		mapdate[date] = date
		//		DataManager.GetDataManager().PushNeedDeleteTs(k)
		//		continue
		//	}
		//
		//	t := time.Unix(k.StartTime, 0)
		//	date1 := t.Format("2006-01-02")
		//	//不属于同一天的才装进预删除表,并添加到临时表
		//	if _, ok := mapdate[date1]; ok == false {
		//		manager.logger.Infof("Append Date Is: [%v], ChannelId: [%v]", date1, k.ChannelInfoID)
		//		if err := MongoDB.GetMongoRecordManager().SetTsInfoMongoToLock(k.ID); err != nil {
		//			manager.logger.Errorf("Set TsInfo Mongo To Lock 1 [%v], Error:[%v]", k.ChannelInfoID, err)
		//			continue
		//		}
		//		mapdate[date1] = date1
		//		DataManager.GetDataManager().PushNeedDeleteTs(k)
		//		continue
		//	} else {
		//		//属于同一天的先判断下挂载点是否一样，如果挂载点飘了也要装进删除表
		//		if !strings.Contains(k.MountPoint, mountpoint) {
		//			if _, ok := mapmp[k.MountPoint]; !ok {
		//				if err := MongoDB.GetMongoRecordManager().SetTsInfoMongoToLock(k.ID); err != nil {
		//					manager.logger.Errorf("Set TsInfo Mongo To Lock 1 [%v], Error:[%v]", k.ChannelInfoID, err)
		//					continue
		//				}
		//				DataManager.GetDataManager().PushNeedDeleteTs(k)
		//				mapmp[k.MountPoint] = k.MountPoint
		//				continue
		//			}
		//			//if err := MongoDB.GetMongoRecordManager().SetInfoMongoToDelete(k.ID); err != nil {
		//			//	manager.logger.Errorf("Set TsInfo Mongo To Lock 2 [%s], Error:[%v]", k.RecordID, err)
		//			//	continue
		//			//}
		//			if err := MongoDB.GetMongoRecordManager().DeleteMongoTsInfoByID(k.ID); err != nil {
		//				manager.logger.Errorf("Delete Data On Mongo, Error:[%v]", k.RecordID, err)
		//				continue
		//			}
		//			count++
		//		}
		//		//if err := MongoDB.GetMongoRecordManager().SetInfoMongoToDelete(k.ID); err != nil {
		//		//	manager.logger.Errorf("Set TsInfo Mongo To Lock 2 [%s], Error:[%v]", k.RecordID, err)
		//		//	continue
		//		//}
		//
		//		if err := MongoDB.GetMongoRecordManager().DeleteMongoTsInfoByID(k.ID); err != nil {
		//			manager.logger.Errorf("Delete Data On Mongo, Error:[%v]", k.RecordID, err)
		//			continue
		//		}
		//		count++
		//	}
		//}
		//manager.logger.Infof("已删除mongo记录: [%v], [%v]", v.ChannelInfo, count)
		//time.Sleep(time.Millisecond)
	}
}

//提取需要删除的TS
func (manager *DeleteTask) goConnectDeleteServer() {
	for manager.bRunning {
		tsTask := DataManager.GetDataManager().GetNeedDeleteTsAll()

		manager.DeleteServerListLock.Lock()
		tempDeleteServer := manager.DeleteServerList
		manager.DeleteServerListLock.Unlock()

		strAddr := ""

		for _, task := range tsTask {
			for key, v := range tempDeleteServer {
				if strings.Contains(v.Mountponit, task.MountPoint) {
					strAddr = key
					manager.logger.Infof("Handle MountPoint [%s] DeleteServer [%v] Has Found:, ChannelID:[%s]", task.MountPoint, strAddr, task.ChannelInfoID)
					manager.DeleteServerList[key].task <- task
				}
			}

			if strAddr == "" {
				manager.logger.Errorf("NO DeleteServer to Handle MountPoint:[%s], ChannelID:[%s]", task.MountPoint, task.ChannelInfoID)
				manager.AddRevertId(task.ID)
				//MongoDB.GetMongoRecordManager().RevertFailedDelete(task.ID)
			}

		}
		time.Sleep(time.Millisecond * 2)
	}
}

func (manager *DeleteTask) goSend(client *DeleteServerInfo, strAddr string) {
	for task := range client.task {
		t := time.Unix(task.StartTime, 0)
		date := t.Format("2006-01-02")
		manager.logger.Infof("开始通知删除服务器[%v], ChannelInfoID[%v], RelativePath[%v]", strAddr, task.ChannelInfoID, task.RecordRelativePath)
		pRespon, err := client.Con.Notify(task.ChannelInfoID, task.RecordRelativePath, task.MountPoint, date, task.ID.Hex(), task.StartTime)
		if nil != err {
			manager.logger.Errorf("收到删除结果[%v]失败[%v], Error: [%v]", strAddr, task, err)
			//MongoDB.GetMongoRecordManager().RevertFailedDelete(task.ID)
			manager.AddRevertId(task.ID)
			continue
		}
		if pRespon.NRespond == 1 {
			manager.logger.Infof("删除服务器[%v]已收到任务[%v]", strAddr, task)
		}
	}
}

func (manager *DeleteTask) goGetResults() {
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
				manager.logger.Infof("文件删除成功：[%v]", result)

				//if data, err := MongoDB.GetMongoRecordManager().SetInfoMongoToDelete1(result.StrChannelID, result.StrMountPoint, result.NStartTime); err != nil {
				//	manager.logger.Errorf("Set TS Info On Mongo To Delete Error, ChannelID[%s], ID: [%v], result.StrMountPoint: [%v], NStartTime: [%v], Error: [%v]", result.StrChannelID, result.GetStrRecordID(), result.StrMountPoint, result.NStartTime, err)
				//	//manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
				//} else {
				//	manager.logger.Infof("删除mongo记录成功: [%v], Count: [%v]", result, data)
				//}

				if _, err := MongoDB.GetMongoRecordManager().DeleteMongoTsAll(result.StrChannelID, result.StrMountPoint, result.NStartTime); err != nil {
					manager.logger.Errorf("Delete On Mongo Error, ChannelID[%s], ID: [%v], result.StrMountPoint: [%v], NStartTime: [%v], Error: [%v]", result.StrChannelID, result.GetStrRecordID(), result.StrMountPoint, result.NStartTime, err)
				} else {
					manager.logger.Infof("删除mongo记录成功: [%v]", result)
				}
			}
		case -1:
			{
				if !bson.IsObjectIdHex(result.GetStrRecordID()) {
					manager.logger.Error("收到的信息错误")
					manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
					//MongoDB.GetMongoRecordManager().RevertFailedDelete(bson.ObjectIdHex(result.GetStrRecordID()))
					continue
				}
				manager.logger.Infof("删除文件失败: [%v]", result)
				//MongoDB.GetMongoRecordManager().RevertFailedDelete(bson.ObjectIdHex(result.GetStrRecordID()))
				manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
			}
		case -2:
			{
				if !bson.IsObjectIdHex(result.GetStrRecordID()) {
					manager.logger.Error("收到的信息错误")
					manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
					//MongoDB.GetMongoRecordManager().RevertFailedDelete(bson.ObjectIdHex(result.GetStrRecordID()))
					continue
				}
				manager.logger.Infof("删除文件失败: [%v]", result)
				manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
				//MongoDB.GetMongoRecordManager().RevertFailedDelete(bson.ObjectIdHex(result.GetStrRecordID()))
			}
		}
	}
}

func (manager *DeleteTask) goGetMQMsg() {
	for {
		queue, err := manager.m_pMQConn.QueueDeclare("RecordDelete", false, false)
		if err != nil {
			manager.logger.Errorf("QueueDeclare Error: %s", err) // 声明队列, 设置为排他队列，链接断开后自动关闭删除
			time.Sleep(time.Second)
			continue
		}
		err = manager.m_pMQConn.AddConsumer("test", queue) //添加消费者
		if err != nil {
			manager.logger.Errorf("AddConsumer Error: %s", err) // 声明队列, 设置为排他队列，链接断开后自动关闭删除
			time.Sleep(time.Second)
			continue
		}
		//只能有一个消费者
		for _, delivery := range queue.Consumes {
			manager.logger.Infof("MQ Consumer: %s", "test")
			manager.m_pMQConn.HandleMessage(delivery, manager.HandleMessage)
		}
		time.Sleep(time.Millisecond)
	}
}

func (manager *DeleteTask) HandleMessage(data []byte) error {
	var msgBody StorageMaintainerMessage.StreamResData
	err := json.Unmarshal(data, &msgBody)
	if nil == err {
		manager.logger.Infof("Received a message: [%v]", msgBody)
		manager.m_chResults <- msgBody
		return nil
	}
	manager.logger.Errorf("Received Error: [%v]", err)
	return err
}

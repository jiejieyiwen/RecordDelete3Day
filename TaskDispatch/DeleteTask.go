package TaskDispatch

import (
	AMQPModular "AMQPModular2"
	"Config"
	"RecordDelete3Day/DataDefine"
	"RecordDelete3Day/DataManager"
	"RecordDelete3Day/MongoDB"
	"RecordDelete3Day/Redis"
	"RecordDelete3Day/StorageMaintainerGRpc/StorageMaintainerGRpcClient"
	"RecordDelete3Day/StorageMaintainerGRpc/StorageMaintainerMessage"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2/bson"
	"iPublic/LoggerModular"
	"iPublic/MongoModular"
	"math/rand"
	"strings"
	"sync"
	"time"
)

var ConcurrentNumber int

func (manager *DeleteTask) Init() {
	rand.Seed(time.Now().UnixNano())
	manager.logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})

	manager.m_pMQConn = new(AMQPModular.RabbServer)
	manager.m_strMQURL = Config.GetConfig().PublicConfig.AMQPURL
	//manager.m_strMQURL = "amqp://guest:guest@192.168.0.56:30001/"
	//manager.m_strMQURL = "amqp://dengyw:dengyw@49.234.88.77:5672/dengyw"

	err := AMQPModular.GetRabbitMQServ(manager.m_strMQURL, manager.m_pMQConn)
	if err != nil {
		manager.logger.Errorf("Init MQ Failed, Errors: %v", err.Error())
		return
	}
	manager.logger.Infof("Init MQ Success: [%v]", manager.m_strMQURL)

	go manager.goGetMQMsg()

	manager.DeleteServerList = make(map[string]*DeleteServerInfo)
	manager.getDeleteServer()

	//防止服务器时间出问题，取时间时和互联网对时一次，误差超过1小时当次不处理
	for !DataManager.CheckNetworkTimeWithNTSC() {
		manager.logger.Errorf("Check Time Sync Error, will continue. ")
		time.Sleep(time.Second * 15)
	}

	for key, mq := range DataManager.GetDataManager().GetMountPointMQMap() {
		go manager.goGetResultsByMountPoint(key, mq)
	}
	manager.logger.Infof("MQ map len: [%v]", len(DataManager.GetDataManager().GetMountPointMQMap()))

	go manager.goStartQueryMongoByMountPoint()

	go manager.goConnectDeleteServer()

	time.Sleep(time.Second * 300)
	go manager.goupdateDeleteServer()
}

func (manager *DeleteTask) getDeleteServer() {
	tempDeleteServerList := Redis.GetRedisRecordManager().GetDeleteServerConfig()
	for key, v := range tempDeleteServerList {
		srv := &DeleteServerInfo{
			Con:        &StorageMaintainerGRpcClient.GRpcClient{},
			Mountponit: "",
			task:       make(chan DataDefine.RecordFileInfo, Config.GetConfig().StorageConfig.ConcurrentNumber),
			//task:       make(chan SDataDefine.RecordFileInfo, 50),
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
	for {
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
					task:       make(chan DataDefine.RecordFileInfo, Config.GetConfig().StorageConfig.ConcurrentNumber),
					//task:       make(chan SDataDefine.RecordFileInfo, 50),
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
			}
			manager.logger.Infof("DeleteServer Connection Is OK: [%v]", key)
		}
		manager.logger.Info("Update DeleteServer Success")
		time.Sleep(time.Second * 300)
	}
}

func (manager *DeleteTask) goStartQueryMongoByMountPoint() {
	for {
		taskmap := DataManager.GetDataManager().GetMountPointMap()
		var wg sync.WaitGroup
		index := 0
		srv := MongoDB.GetMongoRecordManager1()
		for key, v := range taskmap {
			wg.Add(1)
			go manager.getNeedDeleteTask(key, v, &wg, srv.Srv[index], index)
			index++
			if index >= DataManager.Size {
				index = 0
			}
		}
		wg.Wait()

		manager.logger.Info("Start To Get New ChannelStorage")
		DataManager.GetDataManager().TaskMap = []DataManager.StorageDaysInfo{}
		DataManager.GetDataManager().GetNewChannelStorage()
		time.Sleep(time.Second)
	}
}

func (manager *DeleteTask) goSearchTaskOnMongo(v DataManager.StorageDaysInfo, mountpoint string, chres chan int, srv MongoModular.MongoDBServ, index int) {
	manager.logger.Infof("开启挂载点[%v]查询子协程: [%v], mongosrv: [%v]", mountpoint, v.ChannelInfo, index)
	startTs, err := DataManager.GetSubDayMorningTimeStamp(v.StorageDays)
	if err != nil {
		manager.logger.Errorf("Get SubDay MorningTimeStamp err: [%v] ", err)
		<-chres
		return
	}
	var dbResults []DataDefine.RecordFileInfo
	t1 := time.Now()
	//err = MongoDB.GetMongoRecordManager().QueryRecord(v.ChannelInfo, startTs, &dbResults, 1)
	err = MongoDB.QueryRecord1(v.ChannelInfo, startTs, &dbResults, 1, srv)
	t2 := time.Now()
	if err != nil {
		manager.logger.Errorf("Get MongoDB Record Error: [%v], ChannelId: [%v], 协程: [%v]", err, v.ChannelInfo, mountpoint)
		<-chres
		return
	}
	if len(dbResults) == 0 {
		manager.logger.Infof("No DBResult For ChannelID:[%v], 协程: [%v], 查询耗时: [%v]", v.ChannelInfo, mountpoint, t2.Sub(t1).Seconds())
		<-chres
		return
	} else {
		manager.logger.Infof("Get DBResult For ChannelID:[%v], len: [%v], 协程: [%v], 查询耗时: [%v]", v.ChannelInfo, len(dbResults), mountpoint, t2.Sub(t1).Seconds())
		temkey := make(map[string]string)
		temkeymp := make(map[string]string)
		t := time.Now()
		for index, task := range dbResults {
			t := time.Unix(task.StartTime, 0)
			date := t.Format("2006-01-02")
			if index == 0 {
				DataManager.GetDataManager().PushNeedDeleteTs(task)
				temkey[date] = date
				temkeymp[task.MountPoint] = task.MountPoint
				continue
			}
			if _, ok := temkey[task.MountPoint]; !ok {
				DataManager.GetDataManager().PushNeedDeleteTs(task)
				temkeymp[task.MountPoint] = task.MountPoint
			} else {
				if _, ok1 := temkeymp[date]; !ok1 {
					DataManager.GetDataManager().PushNeedDeleteTs(task)
					temkey[date] = date
				} else {

				}
			}
		}
		manager.logger.Infof("提取任务耗时:[%v], 协程: [%v], ChannelID: [%v]", time.Since(t).Seconds(), mountpoint, v.ChannelInfo)
		//DataManager.GetDataManager().PushNeedDeleteTs(dbResults[0])
		<-chres
	}
}

//mongo中查询需要删除的数据
func (manager *DeleteTask) getNeedDeleteTask(mountpoint string, task []DataManager.StorageDaysInfo, wg *sync.WaitGroup, srv MongoModular.MongoDBServ, index int) {
	manager.logger.Infof("开启协程: [%v], len: [%v], mongosrv: [%v]", mountpoint, len(task), index)
	defer wg.Done()
	chResist := make(chan int, SearchNum)
	for key, v := range task {
		chResist <- key
		go manager.goSearchTaskOnMongo(v, mountpoint, chResist, srv, index)
	}
}

//提取需要删除的TS
func (manager *DeleteTask) goConnectDeleteServer() {
	for {
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
					if ch := manager.DeleteServerList[key]; ch != nil {
						manager.DeleteServerList[key].task <- task
					} else {
						manager.logger.Errorf("通道未开启：[%v]", key)
						manager.DeleteServerList[key].task = make(chan DataDefine.RecordFileInfo, Config.GetConfig().StorageConfig.ConcurrentNumber)
						manager.DeleteServerList[key].task <- task
					}
				}
			}
			if strAddr == "" {
				manager.logger.Errorf("NO DeleteServer to Handle MountPoint:[%s], ChannelID:[%s]", task.MountPoint, task.ChannelInfoID)
			}
		}
		time.Sleep(time.Millisecond)
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
			//manager.AddRevertId(task.ID)
			continue
		}
		if pRespon.NRespond == 1 {
			manager.logger.Infof("删除服务器[%v]已收到任务[%v]", strAddr, task)
		}
	}
}

func (manager *DeleteTask) goGetResultsByMountPoint(mp string, chmq chan StorageMaintainerMessage.StreamResData) {
	manager.logger.Infof("MQ消息处理协程开始工作: [%v]", mp)
	for result := range chmq {
		switch result.GetNRespond() {
		case 1:
			{
				//删除成功，装入删除列表
				if !bson.IsObjectIdHex(result.GetStrRecordID()) {
					manager.logger.Error("收到的信息错误")
					//manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
					continue
				}
				manager.logger.Infof("文件删除成功：[%v], 协程: [%v]", result, mp)

				//t1 := time.Now()
				//if data, err := MongoDB.GetMongoRecordManager().SetInfoMongoToDelete1(result.StrChannelID, result.StrMountPoint, result.NStartTime); err != nil {
				//	manager.logger.Errorf("Set TS Info On Mongo To Delete Error, ChannelID[%s], ID: [%v], result.StrMountPoint: [%v], NStartTime: [%v], Error: [%v]", result.StrChannelID, result.GetStrRecordID(), result.StrMountPoint, result.NStartTime, err)
				//	//manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
				//} else {
				//	t2 := time.Now()
				//	manager.logger.Infof("删除mongo记录成功: [%v], Count: [%v]", result, data, t2.Sub(t1).Milliseconds())
				//}

				t1 := time.Now()
				if data, err := MongoDB.GetMongoRecordManager().DeleteMongoTsAll(result.StrChannelID, result.StrMountPoint, result.NStartTime); err != nil {
					t2 := time.Now()
					manager.logger.Errorf("删除mongo记录失败, ChannelID[%s], result.StrMountPoint: [%v], NStartTime: [%v], Error: [%v], 耗时: [%v]", result.StrChannelID, result.StrMountPoint, result.NStartTime, err, t2.Sub(t1).Seconds())
				} else {
					t2 := time.Now()
					manager.logger.Infof("删除mongo记录成功: [%v], 文件数：[%v], 耗时: [%v], 协程: [%v]", result, data.Removed, t2.Sub(t1).Seconds(), mp)
				}

				//t1 := time.Now()
				//if data, err := MongoDB.DeleteMongoTsAll1(result.StrChannelID, result.StrMountPoint, result.NStartTime, srv); err != nil {
				//	t2 := time.Now()
				//	manager.logger.Errorf("删除mongo记录失败, ChannelID[%s], result.StrMountPoint: [%v], NStartTime: [%v], Error: [%v], 耗时: [%v]", result.StrChannelID, result.StrMountPoint, result.NStartTime, err, t2.Sub(t1).Milliseconds())
				//} else {
				//	t2 := time.Now()
				//	manager.logger.Infof("删除mongo记录成功: [%v], 文件数：[%v], 耗时: [%v], 协程: [%v]", result, data.Removed, t2.Sub(t1).Milliseconds(), mp)
				//}
			}
		case -1:
			{
				if !bson.IsObjectIdHex(result.GetStrRecordID()) {
					manager.logger.Error("收到的信息错误")
					//manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
					continue
				}
				manager.logger.Infof("删除文件失败: [%v], 协程: [%v]", result, mp)
				//manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
			}
		case -2:
			{
				if !bson.IsObjectIdHex(result.GetStrRecordID()) {
					manager.logger.Error("收到的信息错误")
					//manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
					continue
				}
				manager.logger.Infof("删除文件失败: [%v], 协程: [%v]", result, mp)
				//manager.AddRevertId(bson.ObjectIdHex(result.GetStrRecordID()))
			}
		}
	}
}

func (manager *DeleteTask) goGetMQMsg() {
	queue, err := manager.m_pMQConn.QueueDeclare("RecordDelete", false, false)
	if err != nil {
		manager.logger.Errorf("QueueDeclare Error: %s", err) // 声明队列, 设置为排他队列，链接断开后自动关闭删除
		time.Sleep(time.Second)
		return
	}
	err = manager.m_pMQConn.AddConsumer("test", queue) //添加消费者
	if err != nil {
		manager.logger.Errorf("AddConsumer Error: %s", err) // 声明队列, 设置为排他队列，链接断开后自动关闭删除
		time.Sleep(time.Second)
		return
	}
	//只能有一个消费者
	for _, delivery := range queue.Consumes {
		manager.logger.Infof("MQ Consumer: %s", "test")
		manager.m_pMQConn.HandleMessage(delivery, manager.HandleMessage1)
	}
}

func (manager *DeleteTask) HandleMessage1(data []byte) error {
	var msgBody StorageMaintainerMessage.StreamResData
	err := json.Unmarshal(data, &msgBody)
	if nil == err {
		manager.logger.Infof("Received a message: [%v]", msgBody)
		taskmap := DataManager.GetDataManager().GetMountPointMQMap()
		if _, ok := taskmap[msgBody.StrMountPoint]; ok {
			taskmap[msgBody.StrMountPoint] <- msgBody
		} else {
			DataManager.GetDataManager().MountPointMQListLock.Lock()
			DataManager.GetDataManager().MountPointMQList[msgBody.StrMountPoint] = make(chan StorageMaintainerMessage.StreamResData, 10240)
			DataManager.GetDataManager().MountPointMQListLock.Unlock()
			//srv := MongoDB.GetMongoRecordManager1()
			//index := rand.Intn(DataManager.Size)
			go manager.goGetResultsByMountPoint(msgBody.StrMountPoint, taskmap[msgBody.StrMountPoint])
			taskmap[msgBody.StrMountPoint] <- msgBody
		}
		return nil
	}
	manager.logger.Errorf("Received Error: [%v]", err)
	return err
}

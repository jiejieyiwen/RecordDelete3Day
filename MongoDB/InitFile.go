package MongoDB

import (
	"StorageMaintainer1/DataDefine"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
	"iPublic/MongoModular"
	"time"
)

type RecordFileMongo struct {
	Table  string                   //table name "RecordFileInfo"
	Srv    MongoModular.MongoDBServ // MongoConnect
	Logger *logrus.Entry
}

var recordManager RecordFileMongo
var MongoSrv MongoModular.MongoDBServ
var RecoveryMongoDBTime int64

func GetMongoRecordManager() *RecordFileMongo {
	return &recordManager
}

func init() {
	recordManager.Table = DefaultMongoTable
	recordManager.Logger = LoggerModular.GetLogger().WithFields(logrus.Fields{"Table": DefaultMongoTable})
	RecoveryMongoDBTime = DefaultMongoRecoveryTime
}

func Init() error {
	logger := LoggerModular.GetLogger()
	conf := EnvLoad.GetConf()
	//conf.ServerConfig.MongoDBURL = "mongodb://mj_ya_admin:EkJcQeOP$bGh8IYC@192.168.2.64:27017/mj_log?authSource=admin&maxPoolSize=100"
	//conf.ServerConfig.MongoDBURL = "mongodb://mj_ya_admin:EkJcQeOP$bGh8IYC@127.0.0.1:15677/mj_log?authSource=admin&maxPoolSize=100"
	if err := MongoModular.GetMongoDBHandlerWithURL(conf.ServerConfig.MongoDBURL, &MongoSrv); err != nil {
		logger.Errorf("Init Mongo Connect Err:[%v]. ", err)
		return err
	} else {
		logger.Info("Init Mongo Connect over. url : %v ", conf.ServerConfig.MongoDBURL)
		recordManager.Srv = MongoSrv
		return nil
	}
}

func (record *RecordFileMongo) QueryRecord(Channel string, sTime int64, tpl interface{}, maxTs int) error {
	/*
		查询所有结束时间小于指定时间的Channel TS信息, 过滤条件，未被其他删除程序锁定
	*/
	logger := LoggerModular.GetLogger().WithFields(logrus.Fields{
		"StartTime": sTime,
		"MaxCount":  maxTs,
		"Channel":   Channel,
	})
	defer func() {
		if err := recover(); err != nil {
			logger.Errorf("Panic in QueryRecord: [%s]", err)
		}
	}()

	baseFilter := []interface{}{bson.M{"StartTime": bson.M{"$lte": sTime}}} //
	baseFilter = append(baseFilter, bson.M{"ChannelInfoID": Channel})
	baseFilter = append(baseFilter, bson.M{"LockStatus": DataDefine.StatusNotLock})
	filter := bson.M{"$and": baseFilter}
	return record.Srv.FindAll(record.Table, filter, RecordDefaultSort, maxTs, 0, tpl)
}
func (record *RecordFileMongo) QueryRecordByTime(Channel string, sStartTime int64, sEndTime int64, tpl interface{}, maxTs int) error {
	/*
		查询所有结束时间小于指定时间的Channel TS信息, 过滤条件，未被其他删除程序锁定
	*/
	logger := LoggerModular.GetLogger().WithFields(logrus.Fields{
		"StartTime": sStartTime,
		"EndTime":   sEndTime,
		"MaxCount":  maxTs,
		"Channel":   Channel,
	})
	defer func() {
		if err := recover(); err != nil {
			logger.Errorf("Panic in QueryRecord: [%s]", err)
		}
	}()

	baseFilter := []interface{}{bson.M{"StartTime": bson.M{"$lte": sStartTime}}} //
	baseFilter = append(baseFilter, bson.M{"StartTime": bson.M{"$gt": sEndTime}})
	baseFilter = append(baseFilter, bson.M{"ChannelInfoID": Channel})
	baseFilter = append(baseFilter, bson.M{"LockStatus": DataDefine.StatusNotLock})
	filter := bson.M{"$and": baseFilter}
	return record.Srv.FindAll(record.Table, filter, RecordDefaultSort, maxTs, 0, tpl)
}

func (record *RecordFileMongo) SetTsInfoMongoToLock(id bson.ObjectId) error {
	/*
		锁定单条TS记录，防止其他删除程序干涉
	*/
	baseFilter := []interface{}{bson.M{"_id": id}}
	baseFilter = append(baseFilter, bson.M{"LockStatus": DataDefine.StatusNotLock})
	filter := bson.M{"$and": baseFilter}
	apply := mgo.Change{
		Update: bson.M{
			"LockStatus": DataDefine.StatusLockToLock, //设置为预备删除
			"LockTime":   time.Now().Unix()},
		ReturnNew: true,
		Upsert:    false,
	}
	var Result interface{}
	return record.Srv.Update(record.Table, filter, apply, Result)
}

func (record *RecordFileMongo) SetInfoMongoToDelete(id bson.ObjectId) error {
	/*
		设置为状态为文件删除,
	*/
	baseFilter := []interface{}{bson.M{"_id": id}}
	//baseFilter = append(baseFilter, bson.M{"LockStatus": DataDefine.StatusLockToLock})
	filter := bson.M{"$and": baseFilter}
	apply := mgo.Change{
		Update: bson.M{
			"LockStatus": DataDefine.StatusLockToDelete, //设置为删除
			"DeleteTime": time.Now().Unix()},            //设置个预备删除时间，经过一段时间后，再删除MongoDB数据, 方便前期排错
		ReturnNew: true,
		Upsert:    false,
	}
	var Result interface{}
	return record.Srv.Update(record.Table, filter, apply, Result)
}

func (record *RecordFileMongo) RevertFailedDelete(id bson.ObjectId) error {
	/*
		设置为状态为文件未删除,
	*/
	//tNow := time.Now().Unix()
	baseFilter := []interface{}{bson.M{"LockStatus": DataDefine.StatusLockToLock}}
	//baseFilter = append(baseFilter, bson.M{"LockStatus": DataDefine.StatusLockToLock})
	baseFilter = append(baseFilter, bson.M{"_id": id})
	filter := bson.M{"$and": baseFilter}
	apply := mgo.Change{
		Update: bson.M{
			"LockStatus": DataDefine.StatusNotLock, //设置为未删除
		},
		ReturnNew: true,
		Upsert:    false,
	}
	var Result interface{}
	return record.Srv.Update(record.Table, filter, apply, Result)
}

func (record *RecordFileMongo) DeleteMongoTsInfo(beforeDays int) error {
	/*
		彻底删除MongoDB记录
	*/
	tNow := time.Now()
	DaysAgo := tNow.AddDate(0, 0, -beforeDays)
	baseFilter := []interface{}{bson.M{"LockStatus": DataDefine.StatusLockToDelete}}
	baseFilter = append(baseFilter, bson.M{"DeleteTime": bson.M{"$lte": DaysAgo.Unix()}})
	filter := bson.M{"$and": baseFilter}
	return record.Srv.Delete(record.Table, filter)
}

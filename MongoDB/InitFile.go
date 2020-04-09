package MongoDB

import (
	"Config"
	"StorageMaintainer1/DataDefine"
	"StorageMaintainer1/DataManager"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"iPublic/LoggerModular"
	"iPublic/MongoModular"
	"strconv"
	"strings"
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
	MongoDBURL := Config.GetConfig().MongoDBConfig.MongoDBURLMongo
	//conf.ServerConfig.MongoDBURL = "mongodb://mj_ya_admin:EkJcQeOP$bGh8IYC@192.168.2.64:27017/mj_log?authSource=admin&maxPoolSize=100"
	//MongoDBURL = "mongodb://mj_ya_admin:EkJcQeOP$bGh8IYC@127.0.0.1:15677/mj_log?authSource=admin&maxPoolSize=100"
	//MongoDBURL := "mongodb://mj_log:SwhRdslmS61A9c3P@10.0.1.220:27017,10.0.1.221:27017,10.0.1.222:27017,10.0.1.223:27017,10.0.1.224:27017/mj_log?authSource=mj_log&maxPoolSize=100"
	poolsize := "maxPoolSize="
	s := strconv.Itoa(DataManager.Size)
	poolsize += s
	MongoDBURL = strings.Replace(MongoDBURL, "maxPoolSize=10", poolsize, -1)
	if err := MongoModular.GetMongoDBHandlerWithURL(MongoDBURL, &MongoSrv); err != nil {
		logger.Errorf("Init Mongo Connect Err: [%v]. ", err)
		return err
	} else {
		logger.Info("Init Mongo Connect over url: [%v] ", MongoDBURL)
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

func (record *RecordFileMongo) SetInfoMongoToDelete1(id, mp string, stime int64) (info *mgo.ChangeInfo, err error) {
	/*
		设置为状态为文件删除,
	*/
	baseFilter := []interface{}{bson.M{"ChannelInfoID": id}}
	baseFilter = append(baseFilter, bson.M{"MountPoint": mp})
	tNow := time.Unix(stime, 0)
	eTime := time.Date(tNow.Year(), tNow.Month(), tNow.Day(), 0, 1440, 0, 0, time.Local).Unix()
	baseFilter = append(baseFilter, bson.M{"StartTime": bson.M{"$lt": eTime}})
	//baseFilter = append(baseFilter, bson.M{"StartTime": bson.M{"$gte": stime}})
	filter := bson.M{"$and": baseFilter}
	apply := mgo.Change{
		Update: bson.M{
			"LockStatus": DataDefine.StatusLockToDelete, //设置为删除
			"DeleteTime": time.Now().Unix()},            //设置个预备删除时间，经过一段时间后，再删除MongoDB数据, 方便前期排错
		ReturnNew: true,
		Upsert:    false,
	}
	var Result interface{}
	return record.Srv.Update1(record.Table, filter, apply, Result)
}

func (record *RecordFileMongo) DeleteMongoTsInfo1(id, mp string, stime int64) error {
	/*
		彻底删除MongoDB记录
	*/
	baseFilter := []interface{}{bson.M{"ChannelInfoID": id}}
	baseFilter = append(baseFilter, bson.M{"MountPoint": mp})
	tNow := time.Unix(stime, 0)
	eTime := time.Date(tNow.Year(), tNow.Month(), tNow.Day(), 0, 1440, 0, 0, time.Local).Unix()
	baseFilter = append(baseFilter, bson.M{"StartTime": bson.M{"$lt": eTime}})
	//baseFilter = append(baseFilter, bson.M{"StartTime": bson.M{"$gte": stime}})
	filter := bson.M{"$and": baseFilter}
	return record.Srv.Delete(record.Table, filter)
}

func (record *RecordFileMongo) DeleteMongoTsInfoByID(id, mp string, stime int64) error {
	///彻底删除MongoDB记录
	baseFilter := []interface{}{bson.M{"ChannelInfoID": id}}
	baseFilter = []interface{}{bson.M{"MountPoint": mp}}
	tNow := time.Unix(stime, 0)
	eTime := time.Date(tNow.Year(), tNow.Month(), tNow.Day(), 24, 0, 0, 0, time.UTC)
	baseFilter = append(baseFilter, bson.M{"StartTime": bson.M{"$lte": eTime}})
	baseFilter = append(baseFilter, bson.M{"StartTime": bson.M{"$gte": stime}})
	filter := bson.M{"$and": baseFilter}
	return record.Srv.Delete(record.Table, filter)
}

func (record *RecordFileMongo) DeleteMongoTsAll(id, mp string, stime int64) (info *mgo.ChangeInfo, err error) {
	///彻底删除MongoDB记录
	baseFilter := []interface{}{bson.M{"ChannelInfoID": id}}
	baseFilter = append(baseFilter, bson.M{"MountPoint": mp})
	tNow := time.Unix(stime, 0)
	eTime := time.Date(tNow.Year(), tNow.Month(), tNow.Day(), 0, 1440, 0, 0, time.Local).Unix()
	baseFilter = append(baseFilter, bson.M{"StartTime": bson.M{"$lt": eTime}})
	//baseFilter = append(baseFilter, bson.M{"StartTime": bson.M{"$gte": stime}})
	filter := bson.M{"$and": baseFilter}
	return record.Srv.DeleteAll(record.Table, filter)
}

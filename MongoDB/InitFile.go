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
	recordManager.Table = DefaultMongoTable + Date
	recordManager.Logger = LoggerModular.GetLogger().WithFields(logrus.Fields{"Table": DefaultMongoTable})
	RecoveryMongoDBTime = DefaultMongoRecoveryTime
}

func Init() error {
	logger := LoggerModular.GetLogger()
	MongoDBURL := Config.GetConfig().MongoDBConfig.MongoDBURLMongo
	poolsize := "maxPoolSize="
	s := strconv.Itoa(DataManager.Size)
	poolsize += s
	MongoDBURL = strings.Replace(MongoDBURL, "maxPoolSize=10", poolsize, -1)
	//MongoDBURL = "mongodb://mj_ya_admin:EkJcQeOP$bGh8IYC@127.0.0.1:15677/mj_log?authSource=admin&maxPoolSize=200"
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
	//baseFilter = append(baseFilter, bson.M{"LockStatus": DataDefine.StatusNotLock})
	filter := bson.M{"$and": baseFilter}
	return record.Srv.FindAll(record.Table, filter, RecordDefaultSort, maxTs, 0, tpl)
}

func (record *RecordFileMongo) SetInfoMongoToDelete(id, mp string, stime int64) (info *mgo.ChangeInfo, err error) {
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

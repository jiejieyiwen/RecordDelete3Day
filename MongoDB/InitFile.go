package MongoDB

import (
	"Config"
	"RecordDelete3Day/DataDefine"
	"RecordDelete3Day/DataManager"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"iPublic/LoggerModular"
	"iPublic/MongoModular"
	"time"
)

type RecordFileMongo struct {
	Logger *logrus.Entry
	Srv    MongoModular.MongoDBServ
}

type RecordFileMongo2 struct {
	Srv []MongoModular.MongoDBServ // MongoConnect
}

type RecordFileMongo1 struct {
	Srv []MongoModular.MongoDBServ // MongoConnect
}

var recordManager RecordFileMongo
var recordManager1 RecordFileMongo1
var recordManager2 RecordFileMongo2

var MongoSrv MongoModular.MongoDBServ

func GetMongoRecordManager() *RecordFileMongo {
	return &recordManager
}

func GetMongoRecordManager2() *RecordFileMongo2 {
	return &recordManager2
}

func GetMongoRecordManager1() *RecordFileMongo1 {
	return &recordManager1
}

func init() {
	recordManager.Logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
}

func (record *RecordFileMongo) Init() error {
	logger := LoggerModular.GetLogger()
	MongoDBURL := Config.GetConfig().MongoDBConfig.MongoDBURLMongo
	//if err := MongoModular.GetMongoDBHandlerWithURL(MongoDBURL, &record.Srv); err != nil {
	//	logger.Errorf("Init Mongo Connect Err: [%v]. ", err)
	//	return err
	//} else {
	//	logger.Infof("Init Mongo Connect over url: [%v] ", MongoDBURL)
	//}

	for i := 0; i < DataManager.Size; i++ {
		var srv MongoModular.MongoDBServ
		if err := MongoModular.GetMongoDBHandlerWithURL(MongoDBURL, &srv); err != nil {
			logger.Errorf("Init Mongo Connect Err: [%v]. ", err)
			return err
		} else {
			recordManager1.Srv = append(recordManager1.Srv, srv)
			logger.Infof("Init Mongo Connect over url: [%v] ", MongoDBURL)
		}
	}

	//for i := 0; i < DataManager.Size; i++ {
	//	var srv MongoModular.MongoDBServ
	//	if err := MongoModular.GetMongoDBHandlerWithURL(MongoDBURL, &srv); err != nil {
	//		logger.Errorf("Init Mongo Connect Err: [%v]. ", err)
	//		return err
	//	} else {
	//		recordManager2.Srv = append(recordManager2.Srv, srv)
	//		logger.Infof("Init Mongo Connect over url: [%v] ", MongoDBURL)
	//	}
	//}
	return nil
}

func QueryRecord(Channel string, sTime int64, tpl interface{}, maxTs int, srv MongoModular.MongoDBServ) error {
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
	return srv.FindAll(DefaultMongoTable, filter, RecordDefaultSort, maxTs, 0, tpl)
}

func SetInfoMongoToDelete(id, mp string, stime int64, srv MongoModular.MongoDBServ) (info *mgo.ChangeInfo, err error) {
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
	return srv.Update1(DefaultMongoTable, filter, apply, Result)
}

func DeleteMongoTsAll(id, mp string, stime int64, srv MongoModular.MongoDBServ) (info *mgo.ChangeInfo, err error) {
	baseFilter := []interface{}{bson.M{"ChannelInfoID": id}}
	baseFilter = append(baseFilter, bson.M{"MountPoint": mp})
	tNow := time.Unix(stime, 0)
	eTime := time.Date(tNow.Year(), tNow.Month(), tNow.Day(), 0, 1440, 0, 0, time.Local).Unix()
	baseFilter = append(baseFilter, bson.M{"StartTime": bson.M{"$lt": eTime}})
	//baseFilter = append(baseFilter, bson.M{"StartTime": bson.M{"$gte": stime}})
	filter := bson.M{"$and": baseFilter}
	return srv.DeleteAll(DefaultMongoTable, filter)
}

func DeleteMongoTsInfoByID(id bson.ObjectId, srv MongoModular.MongoDBServ) error {
	///彻底删除MongoDB记录
	baseFilter := []interface{}{bson.M{"_id": id}}
	filter := bson.M{"$and": baseFilter}
	return srv.Delete(DefaultMongoTable, filter)
}

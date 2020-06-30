package MongoDB

import (
	"Config"
	"RecordDelete3Day/DataDefine"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"iPublic/LoggerModular"
	"iPublic/MongoModular"
	"time"
)

type RecordFileMongo struct {
	Logger *logrus.Entry
	Srv    []MongoModular.MongoDBServ
}

var recordManager RecordFileMongo
var recordManager1 RecordFileMongo

func GetMongoRecordManager() *RecordFileMongo {
	return &recordManager
}

func GetMongoRecordManager1() *RecordFileMongo {
	return &recordManager1
}

func init() {
	recordManager.Logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
}

func (record *RecordFileMongo) Init() error {
	logger := LoggerModular.GetLogger()
	MongoDBURL := Config.GetConfig().MongoDBConfig.MongoDBURLMongo
	//MongoDBURL := "mongodb://mj_log:SwhRdslmS61A9c3P@10.0.3.22:27017,10.0.3.23:27017,10.0.3.24:27017/mj_log?authSource=mj_log&maxPoolSize=100"
	//MongoDBURL = "mongodb://mj_ya_admin:EkJcQeOP$bGh8IYC@127.0.0.1:15677/mj_log?authSource=admin&maxPoolSize=100"

	//动存
	for i := 0; i < 100; i++ {
		var srv MongoModular.MongoDBServ
		if err := MongoModular.GetMongoDBHandlerWithURL(MongoDBURL, &srv); err != nil {
			logger.Errorf("Init Mongo Connect Err: [%v]. ", err)
			for {
				if err1 := MongoModular.GetMongoDBHandlerWithURL(MongoDBURL, &srv); err1 != nil {
					logger.Errorf("ReInit Mongo Connect Err: [%v]. ", err1)
					time.Sleep(time.Second)
					continue
				} else {
					recordManager.Srv = append(recordManager.Srv, srv)
					logger.Infof("ReInit Mongo Connect over url: [%v] ", MongoDBURL)
					break
				}
			}
		} else {
			recordManager.Srv = append(recordManager.Srv, srv)
			logger.Infof("Init Mongo Connect over url: [%v] ", MongoDBURL)
		}
	}

	//全存
	MongoDBURL = Config.GetConfig().PullStorageConfig.MongoDBURLMongo
	//MongoDBURL = "mongodb://mj_log:SwhRdslmS61A9c3P@10.0.3.27:27017,10.0.3.28:27017,10.0.3.29:27017/mj_log?authSource=mj_log&maxPoolSize=100"
	for i := 0; i < 100; i++ {
		var srv MongoModular.MongoDBServ
		if err := MongoModular.GetMongoDBHandlerWithURL(MongoDBURL, &srv); err != nil {
			logger.Errorf("Init Pull Mongo Connect Err: [%v]. ", err)
			for {
				if err1 := MongoModular.GetMongoDBHandlerWithURL(MongoDBURL, &srv); err1 != nil {
					logger.Errorf("ReInit Mongo Connect Err: [%v]. ", err1)
					time.Sleep(time.Second)
					continue
				} else {
					recordManager1.Srv = append(recordManager1.Srv, srv)
					logger.Infof("ReInit Pull Mongo Connect over url: [%v] ", MongoDBURL)
					break
				}
			}
		} else {
			recordManager1.Srv = append(recordManager1.Srv, srv)
			logger.Infof("Init Pull Mongo Connect over url: [%v] ", MongoDBURL)
		}
	}
	return nil
}

func QueryRecordbydate(Channel, date string, tpl interface{}, maxTs int, srv MongoModular.MongoDBServ) (err error, table, data string) {
	logger := LoggerModular.GetLogger().WithFields(logrus.Fields{
		"MaxCount": maxTs,
		"Channel":  Channel,
	})
	defer func() {
		if err := recover(); err != nil {
			logger.Errorf("Panic in QueryRecord: [%s]", err)
		}
	}()
	baseFilter := []interface{}{bson.M{"ChannelInfoID": Channel}}
	baseFilter = append(baseFilter, bson.M{"Date": date})
	filter := bson.M{"$and": baseFilter}
	Table := "RecordFileInfo_"
	Table = Table + Date
	return srv.FindAll(Table, filter, RecordDefaultSort, maxTs, 0, tpl), Table, date
}

func DeleteMongoTsAll(id, date string, srv MongoModular.MongoDBServ) (info *mgo.ChangeInfo, err error, table, da string) {
	baseFilter := []interface{}{bson.M{"ChannelInfoID": id}}
	baseFilter = append(baseFilter, bson.M{"Date": date})
	filter := bson.M{"$and": baseFilter}
	Table := "RecordFileInfo_"
	Table = Table + Date
	info, err = srv.DeleteAll(Table, filter)
	return info, err, Table, date
}

func WriteMongoFile(rec DataDefine.RecordFileInfo, srv MongoModular.MongoDBServ) (err error, table string) {
	baseFilter := []interface{}{bson.M{"ChannelInfoID": rec.ChannelInfoID,
		"RecordID":           rec.RecordID,
		"StorageMediumInfo":  rec.StorageMediumInfo,
		"RecordName":         rec.RecordName,
		"RecordRelativePath": rec.RecordRelativePath,
		"RecordFileType":     rec.RecordFileType,
		"StartTime":          rec.StartTime,
		"EndTime":            rec.EndTime,
		"CreateTime":         rec.CreateTime,
		"Status":             rec.Status,
		"LockStatus":         rec.LockStatus,
		"FileSize":           rec.FileSize,
		"TaskID":             rec.TaskID,
		"MountPoint":         rec.MountPoint,
		"TsTime":             rec.TsTime,
		"Date":               rec.Date}}
	Table := "RecordFileInfo_"
	Table = Table + Date
	return srv.Insert(Table, baseFilter), Table
}

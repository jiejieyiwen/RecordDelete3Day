package DataDefine

import (
	"gopkg.in/mgo.v2/bson"
	"time"
)

type MongoLockStatus int
type MongoRecordStatus int

const (
	StorageType_Local_NFS = 1
)

const (
	StatusNotLock      MongoLockStatus = 0 // 初始状态
	StatusLockToLock   MongoLockStatus = 1 // 文件预备删除, 删除程序启动后，对记录进行加锁，加锁后的状态为预备删除锁定
	StatusLockToDelete MongoLockStatus = 2 // 文件删除

)

const (
	StatusNormal        MongoRecordStatus = 0 // 初始状态
	StatusReadyToDelete MongoRecordStatus = 1 //

)

const (
	STATUS_DELECT_FAILED            = -1
	STATUS_DELECT_NONE              = 0
	STATUS_DELECT_SUCCESS           = 1
	STATUS_DELECT_SUCCESS_AND_SLEEP = 2
)

type RecordFileInfo struct {
	ID                 bson.ObjectId     `bson:"_id"`
	RecordID           string            `json:"RecordID" bson:"RecordID"`                     //录像ID, 生成算法待定
	ChannelInfoID      string            `json:"ChannelInfoID" bson:"ChannelInfoID"`           //通道ID
	StorageMediumInfo  string            `json:"StorageMediumInfo" bson:"StorageMediumInfo"`   //存储介质ID
	RecordName         string            `json:"RecordName" bson:"RecordName"`                 //录像名称
	RecordRelativePath string            `json:"RecordRelativePath" bson:"RecordRelativePath"` //录像文件相对路径
	RecordFileType     int               `json:"RecordFileType" bson:"RecordFileType"`         //录像文件类型
	StartTime          int64             `json:"StartTime" bson:"StartTime"`                   //文件起始时间
	EndTime            int               `json:"EndTime" bson:"EndTime"`                       //文件结束时间
	CreateTime         time.Time         `json:"CreateTime" bson:"CreateTime"`                 //创建时间, 手动生成
	Status             MongoRecordStatus `json:"Status" bson:"Status"`                         //状态,
	LockStatus         MongoLockStatus   `json:"LockStatus" bson:"LockStatus"`                 //锁状态,
	FileSize           int32             `json:"FileSize" bson:"FileSize"`                     // 文件大小 , 单位KB
	TaskID             string            `json:"TaskID" bson:"TaskID"`
	MountPoint         string            `json:"MountPoint" bson:"MountPoint"`
	TsTime             int               `json:"TsTime" bson:"TsTime-"`
	Date               string            `json:"DATE" bson:"Date"`
}

func (info *RecordFileInfo) GetFileName() string {
	return info.RecordName
}

func (info *RecordFileInfo) GetFilePath() string {
	return info.RecordRelativePath
}

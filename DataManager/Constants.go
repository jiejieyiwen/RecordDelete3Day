package DataManager

import (
	"github.com/sirupsen/logrus"
	DataDefine "iPublic/DataFactory/DataDefine/ProtoBuf"
	"sync"
)

type StorageDaysInfo struct {
	ChannelInfo string
	StorageDays int32
	Path        string
}

type DataManager struct {
	SliceChannelStorageInfo     []DataDefine.StorageData
	SliceChannelStorageInfoLock sync.RWMutex

	logger *logrus.Logger //日志

	MountPointList     map[string][]StorageDaysInfo //挂载点表
	MountPointListLock sync.Mutex
}

var dManager DataManager
var NTSC_URL = "http://www.baidu.com/"

//var NTSC_URL = "http://192.168.60.35:39002/imccp-mediacore"

func GetDataManager() *DataManager {
	return &dManager
}

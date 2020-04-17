package DataManager

import (
	SDataDefine "StorageMaintainer/DataDefine"
	"StorageMaintainer/StorageMaintainerGRpc/StorageMaintainerMessage"
	"github.com/sirupsen/logrus"
	"iPublic/DataFactory"
	DataDefine "iPublic/DataFactory/DataDefine/ProductPlatformDataDefine"
	"sync"
)

type StorageDaysInfo struct {
	ChannelInfo string
	StorageDays int
	Path        string
}

type DataManager struct {
	/*
		存放介质，通道Token等信息
	*/
	bRunning                bool
	bRecicvedGRPCNotify     bool
	bRecicvedGRPCNotifyLock sync.RWMutex

	mpDataInterface             DataFactory.Datainterface
	SliceChannelStorageInfo     []DataDefine.ChannelStorageInfo
	SliceChannelStorageInfoLock sync.RWMutex

	TaskMap               []StorageDaysInfo
	TaskMapLock           sync.RWMutex
	NeedDeleteTsList      []SDataDefine.RecordFileInfo // TS信息
	NeedDeleteTsList1     chan SDataDefine.RecordFileInfo
	NeedDeleteTsList1Lock sync.Mutex
	PlatformToken         string
	logger                *logrus.Logger //日志

	MountPointList     map[string][]StorageDaysInfo //挂载点表
	MountPointListLock sync.Mutex

	MountPointMQList     map[string]chan StorageMaintainerMessage.StreamResData //挂载点表
	MountPointMQListLock sync.Mutex

	MapNeedDeleteList     map[string][]SDataDefine.RecordFileInfo
	MapNeedDeleteListLock sync.Mutex
}

var dManager DataManager
var NTSC_URL = "http://www.baidu.com/"

//var NTSC_URL = "http://192.168.60.35:39002/imccp-mediacore"

func GetDataManager() *DataManager {
	return &dManager
}

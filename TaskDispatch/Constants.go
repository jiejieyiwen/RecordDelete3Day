package TaskDispatch

import (
	SDataDefine "RecordDelete3Day/DataDefine"
	"RecordDelete3Day/StorageMaintainerGRpc/StorageMaintainerGRpcClient"
	"github.com/sirupsen/logrus"
	"sync"
)

var SearchNum = 1

type DeleteServerInfo struct {
	Con        *StorageMaintainerGRpcClient.GRpcClient
	Mountponit string
	task       []SDataDefine.RecordFileInfo
}

type DeleteTask struct {
	logger *logrus.Entry

	DeleteServerList     map[string]*DeleteServerInfo
	DeleteServerListLock sync.Mutex

	NotifyFailedList map[string][]SDataDefine.RecordFileInfo

	ServerList map[string]string

	TaskList     []SDataDefine.RecordFileInfo
	TaskListLock sync.Mutex
}

var task DeleteTask

func GetTaskManager() *DeleteTask {
	return &task
}

package TaskDispatch

import (
	AMQPModular "AMQPModular2"
	SDataDefine "RecordDelete3Day/DataDefine"
	"RecordDelete3Day/StorageMaintainerGRpc/StorageMaintainerGRpcClient"
	"github.com/sirupsen/logrus"
	"sync"
)

const SearchNum int = 15

type DeleteServerInfo struct {
	Con        *StorageMaintainerGRpcClient.GRpcClient
	Mountponit string
	task       chan SDataDefine.RecordFileInfo
}

type DeleteTask struct {
	logger *logrus.Entry

	DeleteServerList     map[string]*DeleteServerInfo
	DeleteServerListLock sync.Mutex

	m_pMQConn  *AMQPModular.RabbServer //MQ连接
	m_strMQURL string                  //MQ连接地址
}

var task DeleteTask

func GetTaskManager() *DeleteTask {
	return &task
}
